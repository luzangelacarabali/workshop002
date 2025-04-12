# src/transform/theaudiodb.py

import pandas as pd
import numpy as np
import json
import logging
from pathlib import Path

# Configuración del logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    datefmt="%d/%m/%Y %I:%M:%S %p"
)

# ----- TRANSFORM ----- #
def transform_theaudiodb_data(albums_raw_path):
    """
    Transforma los datos del archivo JSON de TheAudioDB.

    Parámetros:
        albums_raw_path (str o Path): Ruta al archivo albums_raw.json

    Retorna:
        pd.DataFrame: DataFrame transformado con datos de álbumes
    """
    try:
        # Cargar datos
        with open(albums_raw_path, "r", encoding="utf-8") as file:
            albums_data = json.load(file)

        # Convertir a DataFrame
        albums_df = pd.DataFrame(albums_data["album"])

        # Convertir columnas a tipo numérico
        albums_df["intYearReleased"] = pd.to_numeric(albums_df["intYearReleased"], errors="coerce")
        albums_df["intScore"] = pd.to_numeric(albums_df["intScore"], errors="coerce")
        albums_df["intScoreVotes"] = pd.to_numeric(albums_df["intScoreVotes"], errors="coerce")

        # Logs informativos
        print("🎵 Álbumes únicos:", albums_df["idAlbum"].nunique())
        print("🎤 Artistas únicos:", albums_df["idArtist"].nunique())
        print("🏷️ Sellos discográficos únicos:", albums_df["idLabel"].nunique())
        print("📅 Años únicos de lanzamiento:", albums_df["intYearReleased"].nunique())
        print("🎼 Estilos únicos:", albums_df["strStyle"].nunique())
        print("🎧 Géneros únicos:", albums_df["strGenre"].nunique())

        print("✔️ Con Discogs ID:", albums_df["strDiscogsID"].notna().sum())
        print("✔️ Con Wikidata ID:", albums_df["strWikidataID"].notna().sum())
        print("✔️ Con Wikipedia ID:", albums_df["strWikipediaID"].notna().sum())
        print("✔️ Con Genius ID:", albums_df["strGeniusID"].notna().sum())

        return albums_df

    except Exception as e:
        logging.error(f"❌ Error al transformar los datos: {e}")
        return None
