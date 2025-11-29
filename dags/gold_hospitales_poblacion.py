from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import geopandas as gpd
import pandas as pd
import folium

# -----------------------------
# Función que ejecuta todo el workflow
# -----------------------------
def procesar_hospitales_distritos():

    # -----------------------------
    # Rutas
    # -----------------------------
    BASE_DIR = "data"
    SILVER_DIR = os.path.join(BASE_DIR, "silver")
    GOLD_DIR = os.path.join(BASE_DIR, "gold")
    os.makedirs(GOLD_DIR, exist_ok=True)

    # -----------------------------
    # 1. Cargar SILVER
    # -----------------------------
    distritos = gpd.read_parquet(os.path.join(SILVER_DIR, "districtes-distritos.parquet"))
    distritos = distritos.dissolve(by='coddistrit', aggfunc='first').reset_index()

    hospitales = gpd.read_parquet(os.path.join(SILVER_DIR, "hospitales.parquet"))
    padron = pd.read_parquet(os.path.join(SILVER_DIR, "padron_distritos.parquet"))

    print(f"\n=== DATOS CARGADOS ===")
    print(f"Distritos: {len(distritos)}, Hospitales: {len(hospitales)}, Padrón: {len(padron)}")
    print(f"\nColumnas hospitales: {hospitales.columns.tolist()}")

    # Aseguramos que hospitales tiene el mismo CRS
    hospitales = hospitales.to_crs(distritos.crs)

    # -----------------------------
    # 2. Join espacial hospitales → distritos
    # -----------------------------
    join_hosp = gpd.sjoin(hospitales, distritos, how="left", predicate="within", rsuffix="__distrito")


    
    print(f"\n=== DESPUÉS DEL SPATIAL JOIN ===")
    print(f"Columnas disponibles: {join_hosp.columns.tolist()}")
    
    # Identificar la columna de código de distrito
    cod_col = None
    for col in join_hosp.columns:
        if 'coddistrit' in col.lower() and '__distrito' in col:
            cod_col = col
            break
    
    if cod_col is None:
        # Si no hay código con sufijo, buscar el original
        if 'coddistrit_left' in join_hosp.columns:
            cod_col = 'coddistrit_left'
        elif 'coddistrit' in join_hosp.columns:
            cod_col = 'coddistrit'
    
    print(f"Usando columna código: {cod_col}")
    print(f"Hospitales con distrito asignado: {join_hosp[cod_col].notna().sum()} de {len(join_hosp)}")

    print(f"\n=== HOSPITALES EN POBLATS DEL NORD ===")
    # Primero encuentra el código de Poblats del Nord
    poblats_code = distritos[distritos["nombre"].str.contains("POBLATS DEL NORD", case=False, na=False)]["coddistrit"].values
    print(f"Código de Poblats del Nord: {poblats_code}")

    # Luego filtra hospitales por ese código
    poblats_hosp = join_hosp[join_hosp[cod_col].isin(poblats_code)]
    print(f"Hospitales en Poblats del Nord: {len(poblats_hosp)}")
    print(poblats_hosp[["nombre_left", cod_col]])

    # Contar hospitales por código de distrito
    hosp_count = (
        join_hosp[join_hosp[cod_col].notna()]
        .groupby(cod_col)
        .size()
        .reset_index(name="num_hospitales")
    )
    
    print(f"\n=== HOSPITALES POR CÓDIGO DE DISTRITO ===")
    print(hosp_count)

    # -----------------------------
    # 3. Preparar nombres normalizados en distritos
    # -----------------------------
    distritos["nombre_norm"] = distritos["nombre"].str.upper().str.strip()
    
    # -----------------------------
    # 4. Limpiar padrón y normalizar nombres
    # -----------------------------
    padron_limpio = padron[
        ~padron["Distrito"].str.contains("Nota:|Fuente:", na=False)
    ].copy()
    
    # Quitar números y puntos del inicio, normalizar
    padron_limpio["nombre_norm"] = (
        padron_limpio["Distrito"]
        .str.replace(r'^\s*\d+\.\s*', '', regex=True)
        .str.strip()
        .str.upper()
    )

    print(f"\n=== NOMBRES EN DISTRITOS ===")
    print(distritos[["coddistrit", "nombre", "nombre_norm"]].drop_duplicates().sort_values("coddistrit"))
    
    print(f"\n=== NOMBRES EN PADRÓN ===")
    print(padron_limpio[["Distrito", "nombre_norm", "Poblacion_2024"]])

    # -----------------------------
    # 5. Mapeo manual para casos con diferencias
    # -----------------------------
    mapeo_nombres = {
        "JESÚS": "JESUS",
        "POBLES DEL NORD": "POBLATS DEL NORD",
        "POBLES DE L'OEST": "POBLATS DE L'OEST",
        "POBLES DEL SUD": "POBLATS DEL SUD",
        "POBLATS MARÍTIMS": "POBLATS MARITIMS",
        "ALGIRÓS": "ALGIROS",
        "LA SAÏDIA": "LA SAIDIA"
    }
    
    # Aplicar mapeo al padrón (convertir padrón a formato de distritos)
    padron_limpio["nombre_match"] = padron_limpio["nombre_norm"].replace(mapeo_nombres)
    distritos["nombre_match"] = distritos["nombre_norm"]

    print(f"\n=== NOMBRES PARA MATCHING ===")
    print("Distritos:")
    print(distritos[["coddistrit", "nombre", "nombre_match"]].drop_duplicates().sort_values("coddistrit"))
    print("\nPadrón:")
    print(padron_limpio[["nombre_norm", "nombre_match", "Poblacion_2024"]])

    # -----------------------------
    # 6. Merge distritos con población (por nombre)
    # -----------------------------
    distritos_con_pob = distritos.merge(
        padron_limpio[["nombre_match", "Poblacion_2024"]], 
        on="nombre_match", 
        how="left"
    )
    
    print(f"\n=== MERGE POBLACIÓN ===")
    print(f"Distritos con población asignada: {distritos_con_pob['Poblacion_2024'].notna().sum()} de {len(distritos_con_pob)}")
    print(distritos_con_pob[["coddistrit", "nombre", "Poblacion_2024"]].drop_duplicates().sort_values("coddistrit"))
    
    # -----------------------------
    # 7. Merge con hospitales (por código de distrito)
    # -----------------------------
    hosp_count = hosp_count.rename(columns={cod_col: "coddistrit"})
    
    gold = distritos_con_pob.merge(
        hosp_count[["coddistrit", "num_hospitales"]], 
        on="coddistrit",
        how="left"
    )

    gold["num_hospitales"] = gold["num_hospitales"].fillna(0).astype(int)
    gold["Poblacion_2024"] = gold["Poblacion_2024"].fillna(0).astype(int)

    gold = gold.dissolve(by='coddistrit', aggfunc='first').reset_index()

    print(f"\n=== MERGE HOSPITALES ===")
    print(f"Distritos con hospitales: {gold['num_hospitales'].gt(0).sum()}")

    # -----------------------------
    # 8. Métrica GOLD: hospitales por 10.000 habitantes
    # -----------------------------

    # Justo antes de calcular hospitales_por_10k, añade:
    print("\n=== DEBUG POBLATS DEL NORD ===")
    poblats = gold[gold["nombre"].str.contains("POBLATS DEL NORD", case=False, na=False)]
    print(f"Número de filas para Poblats del Nord: {len(poblats)}")
    print(poblats[["coddistrit", "nombre", "num_hospitales", "Poblacion_2024"]])
    print(f"\nValores únicos de num_hospitales: {poblats['num_hospitales'].unique()}")
    print(f"Valores únicos de Poblacion_2024: {poblats['Poblacion_2024'].unique()}")

   # AHORA calcula la métrica
    gold["hospitales_por_10k"] = (
        gold["num_hospitales"] / (gold["Poblacion_2024"] / 10000)
    ).replace([float("inf"), float("nan")], 0)

    print(f"\n=== RESULTADO FINAL ===")
    resultado = gold[["coddistrit", "nombre", "num_hospitales", "Poblacion_2024", "hospitales_por_10k"]].drop_duplicates().sort_values("coddistrit")
    print(resultado)
    print(f"\nTotal hospitales: {gold['num_hospitales'].sum()}")
    print(f"Total población: {gold['Poblacion_2024'].sum()}")

    # -----------------------------
    # 9. Exportar GOLD
    # -----------------------------
    out_path = os.path.join(GOLD_DIR, "gold_hospitales_poblacion_distritos.parquet")
    gold.to_parquet(out_path, compression="snappy")
    print(f"\n✔ GOLD guardado en {out_path}")

    # -----------------------------
    # 10. Crear mapa interactivo
    # -----------------------------
    m = folium.Map(location=[39.47, -0.38], zoom_start=11)

    # Convertir coddistrit a string para el mapa
    gold["coddistrit_str"] = gold["coddistrit"].astype(str)

    folium.Choropleth(
        geo_data=gold.to_json(),
        data=gold,
        columns=["coddistrit_str", "hospitales_por_10k"],
        key_on="feature.properties.coddistrit_str",
        fill_color="YlOrRd",
        fill_opacity=0.7,
        line_opacity=0.3,
        legend_name="Hospitales por 10.000 habitantes",
    ).add_to(m)

    folium.GeoJson(
        gold,
        tooltip=folium.GeoJsonTooltip(
            fields=["nombre", "num_hospitales", "Poblacion_2024", "hospitales_por_10k"],
            aliases=["Distrito:", "Hospitales:", "Población 2024:", "Hospitales por 10k hab.:"],
            localize=True
        )
    ).add_to(m)

    map_path = os.path.join(GOLD_DIR, "mapa_hospitales_poblacion_distritos.html")
    m.save(map_path)
    print(f"✔ Mapa interactivo creado en {map_path}")


# -----------------------------
# Definición del DAG
# -----------------------------
with DAG(
    dag_id="hospitales_por_distrito",
    start_date=datetime(2024,1,1),
    schedule_interval=None,
    catchup=False,
    tags=["ver3", "gold", "distritos", "ver3"]
) as dag:

    tarea_procesar = PythonOperator(
        task_id="procesar_datos_distritos",
        python_callable=procesar_hospitales_distritos
    )