from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import geopandas as gpd
import os

# Primera transformación sobre los datos obtenidos del repositorio del ayuntamiento de València.

BRONZE_DIR = "/opt/airflow/data/bronze"
SILVER_DIR = "/opt/airflow/data/silver"

def process_geojson():
    files = [f for f in os.listdir(BRONZE_DIR) if f.endswith(".geojson")]

    os.makedirs(SILVER_DIR, exist_ok=True)

    for file in files:
        path = os.path.join(BRONZE_DIR, file)
        gdf = gpd.read_file(path)

        # Estandarizado de las columnas
        gdf.columns = [c.lower().replace(" ", "_") for c in gdf.columns]
        # Normalizado de la geografía al estándar
        gdf = gdf.to_crs(epsg=4326) 
        # Guardar como parquet por optimización
        out_path = os.path.join(SILVER_DIR, file.replace(".geojson", ".parquet"))
        gdf.to_parquet(out_path)

# Declarar el DAG para Airflow.
with DAG(
    dag_id="geojson_bronze_to_silver",
    start_date=datetime(2024,1,1),
    schedule_interval=None,
    catchup=False
):
    PythonOperator(
        task_id="process_geojson",
        python_callable=process_geojson
    )