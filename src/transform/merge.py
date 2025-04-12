import pandas as pd
import logging

# Configuración del logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%d/%m/%Y %I:%M:%S %p")
log = logging.getLogger(__name__)

# ---------------- FUNCIONES AUXILIARES ---------------- #

def fill_null_values(df: pd.DataFrame, columns: list, value) -> None:
    """
    Llena los valores nulos de las columnas especificadas con un valor dado.
    """
    for column in columns:
        if column in df.columns:
            df[column] = df[column].fillna(value)

def drop_columns(df: pd.DataFrame, columns: list) -> None:
    """
    Elimina columnas especificadas del DataFrame, ignorando errores si no existen.
    """
    df.drop(columns=columns, inplace=True, errors="ignore")

# ---------------- FUNCIÓN DE MERGE ---------------- #

def merging_datasets(spotify_df: pd.DataFrame, grammys_df: pd.DataFrame) -> pd.DataFrame:
    """
    Une dos DataFrames (Spotify y Grammys) basándose en coincidencia entre 'track_name' y 'nominee'.

    Args:
        spotify_df (pd.DataFrame): Dataset de Spotify.
        grammys_df (pd.DataFrame): Dataset de premios Grammy.

    Returns:
        pd.DataFrame: DataFrame combinado y limpiado.
    """
    log.info("Iniciando proceso de merge entre datasets...")

    log.info(f"Spotify dataset: {spotify_df.shape[0]} filas, {spotify_df.shape[1]} columnas.")
    log.info(f"Grammys dataset: {grammys_df.shape[0]} filas, {grammys_df.shape[1]} columnas.")

    try:
        # Limpieza de columnas clave para mejorar el match
        spotify_df["track_name_clean"] = spotify_df["track_name"].str.lower().str.strip()
        grammys_df["nominee_clean"] = grammys_df["nominee"].str.lower().str.strip()

        # Realizar el merge
        df_merged = spotify_df.merge(
            grammys_df,
            how="left",
            left_on="track_name_clean",
            right_on="nominee_clean",
            suffixes=("", "_grammys")
        )

        # Rellenar valores nulos
        fill_null_values(df_merged, ["title", "category"], "Not applicable")
        fill_null_values(df_merged, ["is_nominated"], False)

        # Eliminar columnas innecesarias
        drop_columns(df_merged, ["year", "artist", "nominee", "nominee_clean", "track_name_clean"])

        # Asignar columna 'id'
        df_merged = df_merged.reset_index().rename(columns={"index": "id"})
        df_merged["id"] = df_merged["id"].astype(int)

        log.info(f"Merge completado: {df_merged.shape[0]} filas, {df_merged.shape[1]} columnas.")

        return df_merged

    except Exception as e:
        log.error(f"Error en el proceso de merge: {e}")
        return pd.DataFrame()  # Opcional: retornar un df vacío en caso de error
