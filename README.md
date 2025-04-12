# 🎶 Integración Analítica de Datos Musicales: Spotify, Premios Grammy y TheAudioDB con Apache Airflow y Power BI

## 📌 Descripción del Proyecto

Este proyecto integra datos musicales de múltiples fuentes —Spotify, Premios Grammy y la API de TheAudioDB— para construir una base de datos enriquecida que permita analizar la relación entre popularidad, premiación y contexto cultural de los artistas. Se utiliza **Apache Airflow** para orquestar el pipeline de datos y **Power BI** para la visualización ejecutiva e interactiva.

## 🎯 Objetivos

- Realizar análisis exploratorio de datos musicales.
- Integrar datasets estructuralmente diversos.
- Automatizar procesos ETL con Apache Airflow.
- Almacenar datos en PostgreSQL.
- Enriquecer la información con TheAudioDB API.
- Visualizar los resultados mediante dashboards de Power BI.

## 🛠️ Tecnologías y Herramientas

| Herramienta         | Uso Principal                                      |
|---------------------|---------------------------------------------------|
| Python 3.10         | Programación y transformación de datos            |
| Apache Airflow      | Automatización del pipeline de datos              |
| PostgreSQL          | Almacenamiento estructurado de datos              |
| Power BI            | Visualización de resultados                        |
| TheAudioDB API      | Enriquecimiento contextual y multimedia           |
| Pandas / Seaborn    | Limpieza, transformación y EDA                    |
| Ubuntu Linux        | Sistema operativo de desarrollo                   |

## 📂 Estructura del Proyecto
music_awards_pipeline/
├── dags/
│   └── airflow_pipeline.py
├── notebooks/
│   ├── 00-grammy_raw_load.ipynb
│   ├── 01-EDA_Spotify.ipynb
│   ├── 02-EDA_Grammys.ipynb
│   └── 03-EDA_AudioDB_API.ipynb
├── src/
│   ├── extract/
│   │   ├── spotify.py
│   │   ├── grammys.py
│   │   └── audiodb_api.py
│   ├── transform/
│   │   ├── spotify_transform.py
│   │   ├── grammys_transform.py
│   │   ├── api_enrichment.py
│   │   └── merge.py
│   └── db/
│       └── db_operations.py
├── powerbi/
│   └── dashboards.pbix
└── 


## 🧬 Pipeline de Datos (ETL)

### 🔍 Extracción:
- `spotify_dataset.csv` (local)
- Tabla `the_grammys_awards` (PostgreSQL)
- API de TheAudioDB (consultas GET por artista)

### 🧪 Transformación:
- Limpieza de datos nulos y duplicados
- Segmentación de popularidad, energía y duración
- Enriquecimiento con datos biográficos y multimedia
- Unión por campos `artist` y `year`

### 🧱 Carga:
- Persistencia en PostgreSQL usando SQLAlchemy y Pandas `.to_sql()`

### 🧩 Orquestación:
- Definición de DAGs en `airflow_pipeline.py`

## 📊 Visualización en Power BI

Incluye cuatro paneles:

1. **Visión Histórica**: Premios por década y categorías más populares.
2. **Popularidad vs Reconocimiento**: Relación entre métricas sonoras y premios.
3. **Perfil de Artistas**: Imagen, biografía y premios adicionales.
4. **Evolución Musical**: Cambios en géneros y categorías a lo largo del tiempo.

## 📈 Resultados Clave

- La popularidad no siempre predice el reconocimiento en premios.
- El análisis enriquecido con la API de TheAudioDB aporta valor visual y narrativo.
- El pipeline automatizado permite escalabilidad y replicabilidad en proyectos similares.

## 🔮 Recomendaciones Futuras

- Incluir más premiaciones (Latin Grammy, Billboard, etc.)
- Analizar letras con procesamiento de lenguaje natural (NLP).
- Construir una API propia con los datos procesados.
- Aplicar machine learning para predecir nominaciones o tendencias musicales.

## 📥 Instalación y Uso

1. Clona este repositorio:
```bash

