from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import geopandas as gpd
import pandas as pd
import folium

def procesar_viviendas_distritos():
    # cargamos las rutas a utilizar
    BASE_DIR = "data"
    SILVER_DIR = os.path.join(BASE_DIR, "silver")
    GOLD_DIR = os.path.join(BASE_DIR, "gold")
    os.makedirs(GOLD_DIR, exist_ok=True)

    # cargamos los ficheros a utilizar para este análisis.
    # distritos para la información geográfica sobre la división de la ciudad´
    # viviendas porque es la variable a analizar
    # padrón para ver los datos de habitantes por distrito en 2024

    distritos = gpd.read_parquet(os.path.join(SILVER_DIR, "districtes-distritos.parquet"))
    distritos = distritos.dissolve(by='coddistrit', aggfunc='first').reset_index()
    viviendas = gpd.read_parquet(os.path.join(SILVER_DIR, "vivendes-proteccio-publica-vpp-viviendas-proteccion-publica-vpp.parquet"))
    padron = pd.read_parquet(os.path.join(SILVER_DIR, "padron_distritos.parquet"))

    # asegurar la codificación de los geopoints
    viviendas = viviendas.to_crs(distritos.crs)

    # spatial join para relacionar cada vivienda con su distrito
    join_col = gpd.sjoin(
        viviendas,
        distritos,
        how="left",
        predicate="within",
        rsuffix="__distrito"
    )

    print(join_col.columns)


    # normalizamos las columnas que contienen texto
    join_col["promocion_norm"] = (
        join_col["tipopromo"]
        .str.upper()
        .str.normalize('NFKD')
        .str.replace(r'[^A-Z]', '', regex=True)
        .str.strip()
    )

    join_col["uso_norm"] = (
        join_col["uso"]
        .str.upper()
        .str.normalize('NFKD')
        .str.replace(r'[^A-Z]', '', regex=True)
        .str.strip()
    )

    print(join_col.columns)

    # ahora, agrupamos ambas columnas y creamos sus respectivos pivots para ordenar
    viviendas_por_promocion = (
        join_col
        .dropna(subset=["coddistrit"])
        .groupby(["coddistrit", "promocion_norm"])
        .size()
        .reset_index(name="num")
    )

    viviendas_por_uso = (
        join_col
        .dropna(subset=["coddistrit"])
        .groupby(["coddistrit", "uso_norm"])
        .size()
        .reset_index(name="num")
    )

    viviendas_promocion_pivot = viviendas_por_promocion.pivot_table(
        index = "coddistrit",
        columns= "promocion_norm",
        values="num",
        fill_value=0
    ).reset_index()

    viviendas_uso_pivot = viviendas_por_uso.pivot_table(
        index = "coddistrit",
        columns= "uso_norm",
        values="num",
        fill_value=0
    ).reset_index()

    # renombramos para que sea más intuitivo
    viviendas_promocion_pivot = viviendas_promocion_pivot.rename(columns={
        "PUBLICA": "num_publicas",
        "PRIVADA": "num_privadas"
    })

    viviendas_uso_pivot = viviendas_uso_pivot.rename(columns={
        "ALQUILER": "num_alquiler",
        "VENTA": "num_venta"
    })

    # Si alguna no existe en algún distrito, la creamos con el valor 0
    for col in ["num_publicas", "num_privadas"]:
        if col not in viviendas_promocion_pivot.columns:
            viviendas_promocion_pivot[col] = 0

    for col in ["num_alquiler", "num_venta"]:
        if col not in viviendas_uso_pivot.columns:
            viviendas_uso_pivot[col] = 0

    # Aseguramos que los códigos de distrito han sido conservados en el spatial join.
    cod_col = None
    for c in join_col.columns:
        if "coddistrit" in c.lower():
            cod_col = c
            break

    # Realizamos el conteo de viviendas por distrito
    viviendas_count = (
        join_col[join_col[cod_col].notna()]
        .groupby(cod_col)
        .size()
        .reset_index(name="num_viviendas")
    )

    viviendas_count = viviendas_count.rename(columns={cod_col: "coddistrit"})

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

    # Añadimos la información relativa a la población utilizando el padrón
    distritos_con_pob = distritos.merge(
        padron_limpio[["nombre_match", "Poblacion_2024"]],
        on="nombre_match",
        how="left"
    )

    # Merge para preparar la salida
    gold = distritos_con_pob.merge(
        viviendas_count,
        on="coddistrit",
        how="left"
    )

    # añadimos el desglose al resultado
    gold = gold.merge(viviendas_promocion_pivot, on="coddistrit", how="left")
    gold = gold.merge(viviendas_uso_pivot, on="coddistrit", how="left")

    gold["num_viviendas"] = gold["num_viviendas"].fillna(0).astype(int)
    gold["Poblacion_2024"] = gold["Poblacion_2024"].fillna(0).astype(int)

    # Creamos una nueva métrica utilizando los datos de los que disponemos
    gold["viviendas_por_10k"] = (
        gold["num_viviendas"] / (gold["Poblacion_2024"] / 10000)
    ).replace([float("inf"), float("nan")], 0)

    # Guardamos
    out_path = os.path.join(GOLD_DIR, "gold_viviendas_vpp_poblacion_distritos.parquet")
    gold.to_parquet(out_path, compression="snappy")
    print(f"✔ GOLD guardado en {out_path}")

    # Creamos el mapa interactivo.  
    m = folium.Map(location=[39.47, -0.38], zoom_start=11)

    gold["coddistrit_str"] = gold["coddistrit"].astype(str)

    folium.Choropleth(
        geo_data=gold.to_json(),
        data=gold,
        columns=["coddistrit_str", "num_viviendas", "viviendas_por_10k", "Poblacion_2024", "num_publicas", "num_privadas", "num_alquiler", "num_venta"],
        key_on="feature.properties.coddistrit_str",
        fill_color="Greens",
        fill_opacity=0.7,
        line_opacity=0.3,
        legend_name="Número de viviendas de protección pública.",
    ).add_to(m)

    folium.GeoJson(
        gold,
        tooltip=folium.GeoJsonTooltip(
            fields=["nombre","num_viviendas", "viviendas_por_10k", "Poblacion_2024", "num_publicas", "num_privadas", "num_alquiler", "num_venta"],
            aliases=["Distrito:", "Viviendas:", "Viviendas por 10k hab.:", "Población 2024:", "Viviendas VPP promoción pública:", "Viviendas VPP promoción privada:", "Viviendas VPP para alquiler:" , "Viviendas VPP para venta:"],
            localize=True
        )
    ).add_to(m)

    map_path = os.path.join(GOLD_DIR, "mapa_viviendasvpp_poblacion_distritos.html")
    m.save(map_path)
    print(f"✔ Mapa interactivo creado en {map_path}")


# Creamos el DAG
with DAG(
    dag_id="viviendasvpp_por_distrito",
    start_date=datetime(2024,1,1),
    schedule_interval=None,
    catchup=False,
    tags=["gold", "viviendas", "distritos"]
) as dag:

    tarea = PythonOperator(
        task_id="procesar_viviendas",
        python_callable=procesar_viviendas_distritos
    )