# ----------------------------
# Imports estándar
# ----------------------------
from datetime import datetime, timedelta
import sys
import os
import pandas as pd
import logging
import json

# ----------------------------
# Imports de Airflow
# ----------------------------
from airflow import DAG
from airflow.operators.python import PythonOperator

# ----------------------------
# Configuración de path del proyecto
# ----------------------------
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

# ----------------------------
# Imports de módulos propios
# ----------------------------
from task.etl import (
    extract_spotify, extract_grammys, extract_theaudiodb_data,
    transform_spotify, transform_grammys, transform_theaudiodb_data
)

from src.database.db_operation import creating_connection, load_raw_data, load_artists_merged
from src.load_and_store.store import upload_to_drive
from src.task.etl import storing_merged_data




# ----------------------------
# Configuración por defecto del DAG
# ----------------------------
default_args = {
    'owner': "airflow",
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 5),
    'email': ["example@example.com"],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

# ----------------------------
# Funciones Python Callables
# ----------------------------

def load_grammy_raw_data():
    engine = creating_connection()
    csv_path = "/home/ubuntu/Escritorio/workshop2/data/the_grammy_awards.csv"
    df = pd.read_csv(csv_path)
    load_raw_data(engine, df, "grammy_awards_raw")

def spotify_extraction():
    return extract_spotify()

def grammys_extraction():
    return extract_grammys()

def theaudiodb_extraction():
    return extract_theaudiodb_data()

def spotify_transformation():
    raw_df = spotify_extraction()
    return transform_spotify(raw_df)

def grammys_transformation():
    raw_df = grammys_extraction()
    return transform_grammys(raw_df)

def theaudiodb_transformation():
    raw_df = theaudiodb_extraction()
    return transform_theaudiodb_data(raw_df)

def data_merging():
    try:
        # Leer CSV
        csv_path = "/home/ubuntu/Escritorio/workshop2/data/spotify_dataset.csv"
        csv_df = pd.read_csv(csv_path)
        csv_artists = csv_df['artists'].dropna().unique()

        # Leer JSON
        json_path = "/home/ubuntu/Escritorio/workshop2/data/albums_raw.json"
        with open(json_path, 'r') as file:
            json_data = json.load(file)
        json_artists = [album['strArtist'] for album in json_data if 'strArtist' in album]

        # Leer DB
        engine = creating_connection()
        query = "SELECT artist FROM grammy_awards_raw"
        db_df = pd.read_sql_query(query, engine)
        db_artists = db_df['artist'].dropna().unique()

        # Unir todos los artistas
        all_artists = set(csv_artists) | set(json_artists) | set(db_artists)
        final_df = pd.DataFrame({'artist': list(all_artists)})

        # Guardar en tabla
        load_artists_merged(final_df)

        return final_df.to_json(orient='records')

    except Exception as e:
        logging.error(f"[ERROR] Error en data_merging: {e}")
        raise

def data_loading(ti):
    merged_json = ti.xcom_pull(task_ids='data_merging')
    if merged_json is None:
        raise ValueError("No se recibió JSON desde data_merging")
    
    merged_df = pd.read_json(merged_json)
    return load_artists_merged(merged_df)


def data_storing():
    """Función para Airflow: almacena datos usando la función que accede a la base y los sube a Drive."""
    try:
        storing_merged_data()
        logging.info("Datos almacenados y subidos correctamente a Google Drive.")
    except Exception as e:
        logging.error(f"Error al almacenar los datos: {e}")
        raise


# ----------------------------
# Definición del DAG
# ----------------------------
with DAG(
    dag_id='workshop2_dag',
    default_args=default_args,
    description='Workshop 2: ETL pipeline for Spotify, Grammys, and TheAudioDB data.',
    schedule_interval=timedelta(days=1),
    max_active_runs=1,
    catchup=False,
    concurrency=1,
    tags=["etl", "music", "workshop"]
) as dag:

    load_grammy_task = PythonOperator(
        task_id='load_grammy_raw_data',
        python_callable=load_grammy_raw_data
    )

    spotify_extract_task = PythonOperator(
        task_id='spotify_extraction',
        python_callable=spotify_extraction
    )

    grammys_extract_task = PythonOperator(
        task_id='grammys_extraction',
        python_callable=grammys_extraction
    )

    theaudiodb_extract_task = PythonOperator(
        task_id='theaudiodb_extraction',
        python_callable=theaudiodb_extraction
    )

    spotify_transform_task = PythonOperator(
        task_id='spotify_transformation',
        python_callable=spotify_transformation
    )

    grammys_transform_task = PythonOperator(
        task_id='grammys_transformation',
        python_callable=grammys_transformation
    )

    theaudiodb_transform_task = PythonOperator(
        task_id='theaudiodb_transformation',
        python_callable=theaudiodb_transformation
    )

    merge_task = PythonOperator(
        task_id='data_merging',
        python_callable=data_merging
    )

    load_task = PythonOperator(
        task_id='data_loading',
        python_callable=data_loading
    )

    store_task = PythonOperator(
        task_id='data_storing',
        python_callable=data_storing
    )

    # ----------------------------
    # Definición de dependencias
    # ----------------------------
    load_grammy_task >> grammys_extract_task

    for extract_task, transform_task in zip(
        [spotify_extract_task, grammys_extract_task, theaudiodb_extract_task],
        [spotify_transform_task, grammys_transform_task, theaudiodb_transform_task]
    ):
        extract_task >> transform_task

    [spotify_transform_task, grammys_transform_task, theaudiodb_transform_task] >> merge_task
    merge_task >> load_task >> store_task
