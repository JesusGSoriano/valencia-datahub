from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import geopandas as gpd
import pandas as pd
import folium

def procesar_centros_sociales_distritos():

    #Directorios base
    BASE_DIR = "data"
    SILVER_DIR = os.path.join(BASE_DIR, "silver")
    GOLD_DIR = os.path.join(BASE_DIR, "gold")
    os.makedirs(GOLD_DIR, exist_ok=True)

    # Ficheros silver necesarios
    distritos = gpd.read_parquet(os.path.join(SILVER_DIR, "districtes-distritos.parquet"))
    distritos = distritos.dissolve(by='coddistrit', aggfunc='first').reset_index()

    padron = pd.read_parquet(os.path.join(SILVER_DIR, "padron_distritos.parquet"))
    juventud = gpd.read_parquet(os.path.join(SILVER_DIR, "joventut-juventud.parquet"))
    mayores = gpd.read_parquet(os.path.join(SILVER_DIR, "majors-mayores.parquet"))
    general = gpd.read_parquet(os.path.join(SILVER_DIR, "tota-la-poblacio-toda-la-poblacion.parquet"))

    # Aseguramos la codificación de los geopoints a utilizar
    juventud = juventud.to_crs(distritos.crs)
    mayores = mayores.to_crs(distritos.crs)
    general = general.to_crs(distritos.crs)

    # Realizamos spatial join para valernos de los geopoints para asociar cada centro a su distrito
    join_col_juv = gpd.sjoin(
        juventud,
        distritos,
        how="left",
        predicate="within",
        rsuffix="__distrito"
    )

    join_col_may = gpd.sjoin(
        mayores,
        distritos,
        how="left",
        predicate="within",
        rsuffix="__distrito"
    )

    join_col_general = gpd.sjoin(
        general,
        distritos,
        how="left",
        predicate="within",
        rsuffix="__distrito"
    )

    # Agrupamos por distrito
    juv_por_distrito = (
        join_col_juv
        .dropna(subset=["coddistrit"])
        .groupby(["coddistrit"])
        .size()
        .reset_index(name="num_juv")
    )

    may_por_distrito = (
        join_col_may
        .dropna(subset=["coddistrit"])
        .groupby(["coddistrit"])
        .size()
        .reset_index(name="num_may")
    )

    gen_por_distrito = (
        join_col_general
        .dropna(subset=["coddistrit"])
        .groupby(["coddistrit"])
        .size()
        .reset_index(name="num_gen")
    )

    print(f"\n=== CENTROS EN DISTRITO 17 ===")
    print(f"Juventud: {(join_col_juv['coddistrit'] == '17').sum()}")
    print(f"Mayores: {(join_col_may['coddistrit'] == '17').sum()}")
    print(f"General: {(join_col_general['coddistrit'] == '17').sum()}")

    # Creamos un pivot usando merge para almacenar los datos por distrito
    pivot = (
        juv_por_distrito
        .merge(may_por_distrito, on="coddistrit", how="outer")
        .merge(gen_por_distrito, on="coddistrit", how="outer")
    )

    pivot = pivot.fillna(0)

    cols_num = ["num_juv", "num_may", "num_gen"]
    pivot[cols_num] = pivot[cols_num].astype(int)


    #_____________________________________________________________________________________
    # Preparamos la salida
    distritos["nombre_norm"] = distritos["nombre"].str.upper().str.strip()

    padron_limpio = padron[
        ~padron["Distrito"].str.contains("Nota:|Fuente:", na=False)
    ].copy()

    padron_limpio["nombre_norm"] = (
        padron_limpio["Distrito"]
        .str.replace(r"^\s*\d+\.\s*", "", regex=True)
        .str.upper()
        .str.strip()
    )

    # Mapeamos de manera manual los nombres conflictivos.
    mapeo_nombres = {
        "JESÚS": "JESUS",
        "POBLES DEL NORD": "POBLATS DEL NORD",
        "POBLES DE L'OEST": "POBLATS DE L'OEST",
        "POBLES DEL SUD": "POBLATS DEL SUD",
        "POBLATS MARÍTIMS": "POBLATS MARITIMS",
        "ALGIRÓS": "ALGIROS",
        "LA SAÏDIA": "LA SAIDIA"
    }

    padron_limpio["nombre_match"] = padron_limpio["nombre_norm"].replace(mapeo_nombres)
    distritos["nombre_match"] = distritos["nombre_norm"]

    distritos_con_pob = distritos.merge(
        padron_limpio[["nombre_match", "Poblacion_2024"]],
        on="nombre_match",
        how="left"
    )

    gold = distritos_con_pob.merge(
        pivot,
        on="coddistrit",
        how="left"
    )

    gold["centros_totales"] = gold["num_gen"] + gold["num_may"] + gold["num_juv"]

    gold["Poblacion_2024"] = gold["Poblacion_2024"].fillna(0).astype(int)

    # Creamos una nueva métrica utilizando los datos de los que disponemos
    gold["centros_por_10k"] = (
        gold["centros_totales"] / (gold["Poblacion_2024"] / 10000)
    ).replace([float("inf"), float("nan")], 0)


    # Guardamos
    out_path = os.path.join(GOLD_DIR, "gold_centros_poblacion_distritos.parquet")
    gold.to_parquet(out_path, compression="snappy")
    print(f"✔ GOLD guardado en {out_path}")

    # Creamos el mapa interactivo.  
    m = folium.Map(location=[39.47, -0.38], zoom_start=11)

    gold["coddistrit_str"] = gold["coddistrit"].astype(str)

    folium.Choropleth(
        geo_data=gold.to_json(),
        data=gold,
        columns=["coddistrit_str", "centros_totales", "centros_por_10k","num_gen", "num_may", "num_juv"],
        key_on="feature.properties.coddistrit_str",
        fill_color="Reds",
        fill_opacity=0.7,
        line_opacity=0.3,
        legend_name="Centros sociales por distrito",
    ).add_to(m)

    folium.GeoJson(
        gold,
        tooltip=folium.GeoJsonTooltip(
            fields=["nombre", "centros_totales", "centros_por_10k","num_gen", "num_may", "num_juv"],
            aliases=["Distrito:","Centros total: ", "Centros por 10k hab.:","Generales:", "Mayores:", "Juventud:"],
            localize=True
        )
    ).add_to(m)

    map_path = os.path.join(GOLD_DIR, "mapa_centros_poblacion_distritos.html")
    m.save(map_path)
    print(f"✔ Mapa interactivo creado en {map_path}")


# Creamos el DAG
with DAG(
    dag_id="centros_por_distrito",
    start_date=datetime(2024,1,1),
    schedule_interval=None,
    catchup=False,
    tags=["gold", "centros", "distritos"]
) as dag:

    tarea = PythonOperator(
        task_id="procesar_centros",
        python_callable=procesar_centros_sociales_distritos
    )