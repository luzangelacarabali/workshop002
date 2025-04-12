# dags/load_raw_data.py
import os
import sys
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from src.database.db_operation import creating_connection, load_raw_data
# Agregar el path del proyecto para importar desde src
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))


def main():
    # Crear conexión
    conn = creating_connection()

    # Ruta absoluta del archivo CSV
    csv_path = "/home/ubuntu/Escritorio/workshop2/data/the_grammy_awards.csv"

    # Leer el archivo CSV
    df = pd.read_csv(csv_path)

    # Cargar los datos sin transformación
    load_raw_data(conn, df, "grammy_awards_raw")

    # Cerrar la conexión
    conn.close()

if __name__ == "__main__":
    main()
