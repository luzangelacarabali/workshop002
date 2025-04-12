# src/database/db_operation.py

import os
import logging
import pandas as pd
import psycopg2
from psycopg2 import Error
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from sqlalchemy import create_engine, text  # Necesario para load_artists_merged

# ---------------------- CONFIGURACIÓN DE LOGGING ----------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s - %(message)s",
    datefmt="%d/%m/%Y %I:%M:%S %p"
)

# ---------------------- CARGA DE VARIABLES DE ENTORNO ----------------------
env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", ".env")
load_dotenv(env_path)
logging.info(f"Archivo .env cargado desde: {env_path}")

user = os.getenv("PG_USER")
password = os.getenv("PG_PASSWORD")
host = os.getenv("PG_HOST")
port = os.getenv("PG_PORT")
database = os.getenv("PG_DATABASE")

# ---------------------- CONEXIÓN A BASE DE DATOS ----------------------
def creating_connection():
    variables = {
        "PG_USER": user,
        "PG_PASSWORD": password,
        "PG_HOST": host,
        "PG_PORT": port,
        "PG_DATABASE": database
    }

    missing = [k for k, v in variables.items() if not v]
    if missing:
        raise ValueError(f"Faltan variables de entorno: {missing}")

    try:
        port_int = int(port)
    except (ValueError, TypeError) as e:
        raise ValueError(f"PG_PORT debe ser un número entero, se recibió: {port}") from e

    try:
        conn = psycopg2.connect(
            user=user,
            password=password,
            host=host,
            port=port_int,
            database=database
        )
        logging.info("Conexión a la base de datos creada exitosamente.")
        return conn
    except Error as e:
        logging.error(f"Error al conectar a la base de datos: {e}")
        raise

def closing_connection(conn):
    if conn:
        conn.close()
        logging.info("Conexión a la base de datos cerrada.")
    else:
        logging.warning("No se cerró la conexión porque no estaba definida.")

# ---------------------- FUNCIONES PARA TRABAJAR CON DATAFRAMES ----------------------
def infer_psycopg2_type(dtype):
    if "int" in dtype.name:
        return "INTEGER"
    elif "float" in dtype.name:
        return "FLOAT"
    elif "object" in dtype.name:
        return "TEXT"
    elif "datetime" in dtype.name:
        return "TIMESTAMP"
    elif "bool" in dtype.name:
        return "BOOLEAN"
    else:
        return "TEXT"

def create_table_from_dataframe(conn, df, table_name):
    try:
        cur = conn.cursor()
        columns = ", ".join([f"{col} {infer_psycopg2_type(dtype)}" for col, dtype in df.dtypes.items()])
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns});"
        cur.execute(create_table_query)
        conn.commit()
        logging.info(f"Tabla '{table_name}' creada o ya existente.")
    except Error as e:
        logging.error(f"Error al crear la tabla '{table_name}': {e}")
        conn.rollback()
        raise
    finally:
        cur.close()

def load_raw_data(conn, df, table_name):
    try:
        logging.info(f"Cargando datos crudos en la tabla '{table_name}'...")
        create_table_from_dataframe(conn, df, table_name)
        data_tuples = [tuple(row) for row in df.to_numpy()]
        columns = ", ".join(df.columns)
        cur = conn.cursor()
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES %s ON CONFLICT DO NOTHING;"
        execute_values(cur, insert_query, data_tuples)
        conn.commit()
        logging.info(f"Datos cargados correctamente en la tabla '{table_name}'.")
    except Error as e:
        logging.error(f"Error al cargar datos en '{table_name}': {e}")
        conn.rollback()
        raise
    finally:
        cur.close()

def load_artists_merged(df: pd.DataFrame):
    """
    Crea la tabla artists_merged si no existe e inserta los datos desde el DataFrame proporcionado.
    """
    try:
        conn = creating_connection()

        # Crear la tabla si no existe
        create_table_query = """
        CREATE TABLE IF NOT EXISTS artists_merged (
            id SERIAL PRIMARY KEY,
            artist TEXT NOT NULL
        );
        """
        with conn.cursor() as cur:
            cur.execute(create_table_query)
            conn.commit()
            logging.info("Tabla 'artists_merged' verificada/creada.")

        # Insertar los datos del DataFrame
        if not df.empty:
            data_tuples = [tuple(row) for row in df.to_numpy()]
            insert_query = "INSERT INTO artists_merged (artist) VALUES %s ON CONFLICT DO NOTHING;"
            with conn.cursor() as cur:
                execute_values(cur, insert_query, data_tuples)
                conn.commit()
                logging.info("Datos insertados correctamente en 'artists_merged'.")
        else:
            logging.warning("El DataFrame está vacío. No se insertaron datos en 'artists_merged'.")

    except Exception as e:
        logging.error(f"No se pudo cargar la tabla 'artists_merged': {e}")
        if conn:
            conn.rollback()
        raise

    finally:
        closing_connection(conn)
