import os
import pandas as pd
import logging
import numpy as np

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s:%(message)s")

## ----- Spotify Extract ----- ##

def extracting_spotify_data(path):
    """
    Extracts data from a Spotify CSV file and returns it as a pandas DataFrame.
    :param path: Path to the CSV file.
    :return: pandas DataFrame with the extracted data or None if the file is empty.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"File not found: {path}. Make sure the path is correct.")
    
    if not path.endswith(".csv"):
        raise ValueError(f"El archivo {path} no tiene un formato CSV. Asegúrate de que el archivo sea un CSV.")
    
    try:
        df = pd.read_csv(path)
        if df.empty:
            logging.warning("El archivo CSV está vacío o no contiene datos válidos.")
            return None
        
        logging.info(f"Data extracted from {path}.")
        return df
    except FileNotFoundError as e:
        logging.error(f"Archivo no encontrado: {e}")
        raise
    except pd.errors.EmptyDataError as e:
        logging.error(f"El archivo CSV está vacío: {e}")
        raise
    except Exception as e:
        logging.error(f"Error extracting data: {e}.")
        raise
