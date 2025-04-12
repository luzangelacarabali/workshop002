¡Claro! Aquí tienes el README organizado, con una estructura clara y profesional:

---

# 🎶 Integración Analítica de Datos Musicales: Spotify, Premios Grammy y TheAudioDB con Apache Airflow y Power BI

## 📌 Descripción del Proyecto

Este proyecto integra datos musicales de múltiples fuentes —**Spotify**, **Premios Grammy** y la **API de TheAudioDB**— para construir una base de datos enriquecida que permita analizar la relación entre popularidad, premiación y contexto cultural de los artistas. Se utiliza **Apache Airflow** para orquestar el pipeline de datos y **Power BI** para la visualización ejecutiva e interactiva.

---

## 🎯 Objetivos

- Realizar análisis exploratorio de datos musicales.  
- Integrar datasets estructuralmente diversos.  
- Automatizar procesos ETL con Apache Airflow.  
- Almacenar datos en PostgreSQL.  
- Enriquecer la información con TheAudioDB API.  
- Visualizar los resultados mediante dashboards de Power BI.

---

## 🛠️ Tecnologías y Herramientas

| Herramienta        | Uso Principal                                     |
|--------------------|--------------------------------------------------|
| Python 3.10        | Programación y transformación de datos          |
| Apache Airflow     | Automatización del pipeline de datos            |
| PostgreSQL         | Almacenamiento estructurado de datos            |
| Power BI           | Visualización de resultados                      |
| TheAudioDB API     | Enriquecimiento contextual y multimedia         |
| Pandas / Seaborn   | Limpieza, transformación y análisis exploratorio |
| Ubuntu Linux       | Sistema operativo de desarrollo                 |

---

## 📂 Estructura del Proyecto

```
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
```

---

## 🧬 Pipeline de Datos (ETL)

### 🔍 Extracción

- Dataset `spotify_dataset.csv` (local)  
- Tabla `the_grammys_awards` (PostgreSQL)  
- API de TheAudioDB (consultas GET por artista)

### 🧪 Transformación

- Limpieza de datos nulos y duplicados  
- Segmentación de popularidad, energía y duración  
- Enriquecimiento con datos biográficos y multimedia  
- Unión por campos `artist` y `year`

### 🧱 Carga

- Persistencia en PostgreSQL usando SQLAlchemy y `pandas.to_sql()`

### 🧩 Orquestación

- Definición de DAGs en `airflow_pipeline.py`  
- Programación de ejecución periódica y monitoreo de tareas

---

## 📊 Visualización en Power BI

Dashboards incluidos:

1. **Visión Histórica**  
   Premios por década y categorías más populares.

2. **Popularidad vs Reconocimiento**  
   Relación entre métricas sonoras y premios obtenidos.

3. **Perfil de Artistas**  
   Imagen, biografía y premios adicionales.

4. **Evolución Musical**  
   Cambios en géneros y categorías a lo largo del tiempo.

---

## 📈 Resultados Clave

- La popularidad no siempre predice el reconocimiento en premios.  
- El análisis enriquecido con TheAudioDB aporta valor visual y narrativo.  
- El pipeline automatizado permite escalabilidad y replicabilidad.

---

## 🔮 Recomendaciones Futuras

- Integrar otras premiaciones (Latin Grammy, Billboard, etc.)  
- Analizar letras con técnicas de procesamiento de lenguaje natural (NLP)  
- Construir una API propia con los datos procesados  
- Aplicar modelos de machine learning para predicción de nominaciones

---
📥 Instalación y Uso
Clona este repositorio:
```bash
https://github.com/luzangelacarabali/workshop002.git
```


2. Crea y activa un entorno virtual:

```bash
python -m venv venv
source venv/bin/activate  # en Linux/macOS
venv\Scripts\activate     # en Windows
```



Gracias por tomarte el tiempo de explorar este proyecto. Ha sido una experiencia enriquecedora combinar la música, los datos y la automatización en una misma arquitectura analítica.
Si este trabajo te inspiró, te ayudó o simplemente te pareció interesante, no dudes en compartirlo o contribuir.
¡La música y los datos tienen mucho más que contarnos!

🎶 "Donde terminan las palabras, comienza la música... y con ella, los datos cobran vida."



