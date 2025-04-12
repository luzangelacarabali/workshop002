# src/transform/spotify_transform.py
import pandas as pd
import numpy as np
import logging

def transforming_spotify_data(raw_df):
    try:
        if raw_df is None or raw_df.empty:
            raise ValueError("El DataFrame de entrada es None o está vacío")
        
        # Ejemplo de transformación (sustituye con tu lógica)
        df = raw_df.copy()
        # Uso hipotético de NumPy que podría causar el error
        df['alguna_columna'] = np.log1p(df['columna_numérica'])
        
        logging.info("Transformación de Spotify completada")
        return df
    except Exception as e:
        logging.error(f"Falló la transformación: {e}")
        raise