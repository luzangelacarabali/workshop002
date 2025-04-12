import os
import json
import requests
import pandas as pd
import logging

# Configuración de logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s:%(message)s")

# Headers (opcional)
HEADERS = {
    "User-Agent": "TheAudioDBETLWorkshop/1.0 (tucorreo@gmail.com)"
}

# Ruta donde se guardan los datos
DATA_DIR = "/home/ubuntu/Escritorio/workshop2/data"
os.makedirs(DATA_DIR, exist_ok=True)

# URL fija para obtener álbumes por ID de artista
ALBUMS_URL = "https://www.theaudiodb.com/api/v1/json/2/album.php?i=112024"

## ----- EXTRACT ----- ##
def extract_theaudiodb_data():
    try:
        response = requests.get(ALBUMS_URL, headers=HEADERS)
        response.raise_for_status()
        albums_data = response.json()
        with open(os.path.join(DATA_DIR, "albums_raw.json"), "w") as f:
            json.dump(albums_data, f)
        logging.info("Datos de álbumes extraídos exitosamente.")
    except Exception as e:
        logging.error(f"Error al extraer datos de álbumes: {e}")

## ----- TRANSFORM ----- ##
def transform_theaudiodb_data():
    try:
        with open(os.path.join(DATA_DIR, "albums_raw.json"), "r") as f:
            data = json.load(f)
        df_albums = pd.json_normalize(data.get("album", []))
        df_albums.to_json(os.path.join(DATA_DIR, "albums_transformed.json"), orient="records")
        logging.info("Datos de álbumes transformados exitosamente.")
    except Exception as e:
        logging.error(f"Error al transformar datos de álbumes: {e}")

## ----- LOAD ----- ##
def load_theaudiodb_data():
    try:
        with open(os.path.join(DATA_DIR, "albums_transformed.json"), "r") as f:
            data = json.load(f)
        df = pd.json_normalize(data)
        df.to_csv(os.path.join(DATA_DIR, "albums.csv"), index=False)
        logging.info("Datos de álbumes cargados a CSV exitosamente.")
    except Exception as e:
        logging.error(f"Error al cargar datos de álbumes: {e}")

## ----- EJECUTAR ----- ##
if __name__ == "__main__":
    extract_theaudiodb_data()
    transform_theaudiodb_data()
    load_theaudiodb_data()
