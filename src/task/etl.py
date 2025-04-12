# src/task/etl.py

import sys
import os
import json
import pandas as pd
import logging
import requests
from pandas import json_normalize

# Ajustar sys.path para acceder a src/
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

# Importaciones del proyecto
from src.database.db_operation import creating_connection, closing_connection, load_artists_merged, load_raw_data
from src.extract.spotify_extract import extracting_spotify_data
from src.transform.spotify_transform import transforming_spotify_data
from src.extract.theaudiodb_etl import extract_theaudiodb_data
from src.transform.theaudiodb import transform_theaudiodb_data
from src.transform.merge import merging_datasets
from src.load_and_store.load import loading_merged_data
from src.load_and_store.store import upload_to_drive, storing_merged_data

# Configuración del logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(message)s',
    datefmt='%m/%d/%Y %I:%M:%S %p'
)

# Definiciones globales
ENTITIES = ["artist", "genre", "release-group"]
HEADERS = {"User-Agent": "Workshop2DAG/1.0 (tu.email@ejemplo.com)"}
DATA_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "data"))


# ========== EXTRACT ==========

def extract_spotify():
    """Extrae datos de Spotify desde archivo CSV local."""
    try:
        df = extracting_spotify_data("./data/spotify_dataset.csv")
        logging.info("Spotify data extracted successfully.")
        return df.to_json(orient="records")
    except Exception as e:
        logging.error(f"Error extracting Spotify data: {e}")
        raise

def extract_grammys():
    """Extrae datos de premios Grammy desde la base de datos."""
    conn = None
    try:
        conn = creating_connection()
        cur = conn.cursor()
        cur.execute("SELECT * FROM grammy_awards_raw")
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        df = pd.DataFrame(rows, columns=columns)
        logging.info("Grammys data extracted successfully.")
        return df.to_json(orient="records")
    except Exception as e:
        logging.error(f"Error extracting Grammys data: {e}")
        raise
    finally:
        if 'cur' in locals(): cur.close()
        if conn: closing_connection(conn)

def extract_theaudiodb():
    """Extrae datos de álbumes desde TheAudioDB API."""
    try:
        extract_theaudiodb_data()
        with open(os.path.join(DATA_DIR, "albums_transformed.json"), "r") as f:
            data = json.load(f)
        logging.info("TheAudioDB data extracted successfully.")
        return json.dumps(data)
    except Exception as e:
        logging.error(f"Error extracting TheAudioDB data: {e}")
        raise


# ========== TRANSFORM ==========

def transform_spotify(df):
    """Transforma datos crudos de Spotify."""
    try:
        raw_df = pd.DataFrame(json.loads(df))
        df = transforming_spotify_data(raw_df)
        if df is None or not isinstance(df, pd.DataFrame):
            raise ValueError("La transformación devolvió datos inválidos.")
        logging.info("Datos de Spotify transformados exitosamente.")
        return df.to_json(orient="records")
    except Exception as e:
        logging.error(f"Error transformando datos de Spotify: {e}")
        raise

def transform_grammys(df):
    """Transforma datos crudos de premios Grammy."""
    try:
        raw_df = pd.DataFrame(json.loads(df))
        raw_df.columns = [col.strip().lower().replace(" ", "_") for col in raw_df.columns]
        logging.info("Grammys data transformed successfully.")
        return raw_df.to_json(orient="records")
    except Exception as e:
        logging.error(f"Error transforming Grammys data: {e}")
        raise


# ========== MERGE ==========

def etl_merge():
    """Une los datos extraídos desde CSV, API y DB."""
    try:
        # Leer CSV
        csv_path = os.path.join(DATA_DIR, "spotify_dataset.csv")
        csv_df = pd.read_csv(csv_path)
        csv_artists = csv_df['artists'].dropna().unique()

        # Leer JSON
        json_path = os.path.join(DATA_DIR, "albums_raw.json")
        with open(json_path, 'r') as file:
            json_data = json.load(file)
        json_artists = [album['strArtist'] for album in json_data if 'strArtist' in album]

        # Leer DB
        engine = creating_connection()  # Se espera un SQLAlchemy engine aquí
        query = "SELECT artist FROM grammy_awards_raw"
        db_df = pd.read_sql_query(query, engine)
        db_artists = db_df['artist'].dropna().unique()

        # Unir artistas
        all_artists = set(csv_artists) | set(json_artists) | set(db_artists)
        final_df = pd.DataFrame({'artist': list(all_artists)})

        # Cargar en nueva tabla
        load_artists_merged(final_df)
        logging.info("Datos fusionados exitosamente.")
    except Exception as e:
        logging.error(f"Error en la fusión de datos: {e}")
        raise


# ========== LOAD ==========

def load_data(df: pd.DataFrame):
    """Carga los datos unificados en la tabla 'artists_merged'."""
    conn = None
    try:
        conn = creating_connection()
        table_name = "artists_merged"
        load_raw_data(conn, df, table_name)
        logging.info("Tabla 'artists_merged' cargada correctamente.")
    except Exception as e:
        logging.error(f"No se pudo cargar la tabla 'artists_merged': {e}")
        raise
    finally:
        if conn:
            closing_connection(conn)


# ========== STORE ==========

def data_storing(df_raw):
    # Convertir entrada si viene en string JSON
    if isinstance(df_raw, str):
        try:
            df = pd.read_json(df_raw)
        except Exception as e:
            raise ValueError(f"Error al convertir JSON a DataFrame: {e}")
    elif isinstance(df_raw, pd.DataFrame):
        df = df_raw
    else:
        raise TypeError("Formato de entrada no válido, debe ser str (JSON) o DataFrame")



