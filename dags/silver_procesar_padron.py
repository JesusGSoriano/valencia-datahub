from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os

# -----------------------------
# Función para procesar el Excel
# -----------------------------
def procesar_padron():
    input_file = "/opt/airflow/data/bronze/020101_PadronMunicipal.xlsx"
    output_dir = "/opt/airflow/data/silver"
    os.makedirs(output_dir, exist_ok=True)

    # Leer la hoja "6"
    df = pd.read_excel(input_file, sheet_name="6", skiprows=3)

    # Seleccionar columnas relevantes y renombrarlas
    df = df.iloc[:, :6]  # Superficie, Población 2024, Densidad, Población 2023, Variación
    df.columns = ["Distrito", "Superficie_ha", "Poblacion_2024", "Densidad_hab_km2", "Poblacion_2023", "Variacion_interanual"]

    # Limpiar filas que no sean distritos (por ejemplo sumas o notas)
    df = df[df["Distrito"].notna()]

    # Convertir tipos
    df["Superficie_ha"] = pd.to_numeric(df["Superficie_ha"], errors='coerce')
    df["Poblacion_2024"] = pd.to_numeric(df["Poblacion_2024"], errors='coerce')
    df["Densidad_hab_km2"] = pd.to_numeric(df["Densidad_hab_km2"], errors='coerce')
    df["Poblacion_2023"] = pd.to_numeric(df["Poblacion_2023"], errors='coerce')
    df["Variacion_interanual"] = pd.to_numeric(
        df["Variacion_interanual"].astype(str).str.replace("%","").str.replace("nan",""), 
        errors='coerce'
    )
    # Guardar parquet
    output_file = os.path.join(output_dir, "padron_distritos.parquet")
    df.to_parquet(output_file, index=False)
    print(f"✔ Datos procesados y guardados en {output_file}")

# -----------------------------
# DAG
# -----------------------------
with DAG(
    dag_id="padron_distritos",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["poblacion", "valencia"],
) as dag:

    procesar = PythonOperator(
        task_id="procesar_padron",
        python_callable=procesar_padron
    )