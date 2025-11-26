from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import geopandas as gpd
import pandas as pd
from pathlib import Path

BRONZE = Path("/opt/airflow/data/bronze")
SILVER = Path("/opt/airflow/data/silver")

def transform_to_silver():
    SILVER.mkdir(parents=True, exist_ok=True)

    for file in BRONZE.glob("*.geojson"):
        print("Procesando:", file.name)
        gdf = gpd.read_file(file)

        # Asegurar CRS 4326
        if gdf.crs is None or gdf.crs.to_epsg() != 4326:
            gdf = gdf.to_crs(epsg=4326)

        # Extraer lat/lon
        gdf["lat"] = gdf.geometry.y
        gdf["lon"] = gdf.geometry.x

        df = pd.DataFrame(gdf.drop(columns="geometry"))

        out = SILVER / file.name.replace(".geojson", ".parquet")
        df.to_parquet(out, index=False)

with DAG(
    dag_id="silver_transform",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    task = PythonOperator(
        task_id="convert_bronze_to_silver",
        python_callable=transform_to_silver,
    )
