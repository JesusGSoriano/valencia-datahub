from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import geopandas as gpd

PARQUET_PATH = "/opt/airflow/data/silver/hospitales.parquet"

def inspect_parquet():
    try:
        print("📌 Leyendo parquet:", PARQUET_PATH)

        # intentar con pandas
        try:
            df = pd.read_parquet(PARQUET_PATH)
            print("\n=== COLUMNAS (pandas) ===")
            print(df.columns)
            print("\n=== PRIMERAS 10 FILAS ===")
            print(df.head(10).to_markdown())
            return
        except:
            pass

        # intentar con geopandas si fallara pandas
        df = gpd.read_parquet(PARQUET_PATH)
        print("\n=== COLUMNAS (geopandas) ===")
        print(df.columns)
        print("\n=== PRIMERAS 10 FILAS ===")
        print(df.head(10).to_markdown())

    except Exception as e:
        print("❌ ERROR LEYENDO EL PARQUET")
        print(str(e))


with DAG(
    dag_id="inspect_parquet_hospitales",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["debug", "parquet"],
):
    inspect_task = PythonOperator(
        task_id="inspect_file",
        python_callable=inspect_parquet
    )
