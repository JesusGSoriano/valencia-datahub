from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import geopandas as gpd
import pandas as pd
import folium

def procesar_colegios_distritos():

    # Cargamos las rutas.
    BASE_DIR = "data"
    SILVER_DIR = os.path.join(BASE_DIR, "silver")
    GOLD_DIR = os.path.join(BASE_DIR, "gold")
    os.makedirs(GOLD_DIR, exist_ok=True)

    # Cargamos también los ficheros silver que vamos a necesitar.
    # En este caso: distritos para la información geográfica sobre la división de la ciudad
    # Colegios porque es la variable a analizar.
    # Padron por distritos que almacena el número de habitantes en 2024 por distrito
    distritos = gpd.read_parquet(os.path.join(SILVER_DIR, "districtes-distritos.parquet"))
    colegios = gpd.read_parquet(os.path.join(SILVER_DIR, "centros-educativos-en-valencia.parquet"))
    padron = pd.read_parquet(os.path.join(SILVER_DIR, "padron_distritos.parquet"))

    # Asegurar que los geopoints están codificados con el mismo EPSG
    colegios = colegios.to_crs(distritos.crs)

    print(f"Colegios cargados: {len(colegios)}")
    print(f"Distritos cargados: {len(distritos)}")

    # Hacemos un Spatial join para relacionar cada colegio con su distrito utilizando la ubicación.
    join_col = gpd.sjoin(
        colegios,
        distritos,
        how="left",
        predicate="within",
        rsuffix="__distrito"
    )

    # Contamos los colegios según su tipo de régimen

    # Normalizamos el texto
    join_col["regimen_norm"] = (
        join_col["regimen"]
        .str.upper()
        .str.normalize('NFKD')
        .str.replace(r'[^A-Z ]', '', regex=True)
        .str.strip()
    )

    # Agrupamos
    colegios_por_tipo = (
        join_col
        .dropna(subset=["coddistrit"])
        .groupby(["coddistrit", "regimen_norm"])
        .size()
        .reset_index(name="num")
    )

    # Creamos un pivot para ordenar los datos por distrito
    colegios_tipo_pivot = colegios_por_tipo.pivot_table(
        index="coddistrit",
        columns="regimen_norm",
        values="num",
        fill_value=0
    ).reset_index()

    # Renombramos para que sea más intuitivo
    colegios_tipo_pivot = colegios_tipo_pivot.rename(columns={
        "PUBLICO": "num_publicos",
        "PRIVADO": "num_privados",
        "CONCERTADO": "num_concertados"
    })

    # Si alguna no existe en algún distrito, la creamos con el valor 0
    for col in ["num_publicos", "num_privados", "num_concertados"]:
        if col not in colegios_tipo_pivot.columns:
            colegios_tipo_pivot[col] = 0


    # Aseguramos que los códigos de distrito han sido conservados en el spatial join.
    cod_col = None
    for c in join_col.columns:
        if "coddistrit" in c.lower():
            cod_col = c
            break

    # Realizamos el conteo de colegios por distrito
    colegios_count = (
        join_col[join_col[cod_col].notna()]
        .groupby(cod_col)
        .size()
        .reset_index(name="num_colegios")
    )

    colegios_count = colegios_count.rename(columns={cod_col: "coddistrit"})

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
        colegios_count,
        on="coddistrit",
        how="left"
    )

    # Añadimos el desglose de colegios por régimen
    gold = gold.merge(colegios_tipo_pivot, on="coddistrit", how="left")

    gold["num_colegios"] = gold["num_colegios"].fillna(0).astype(int)
    gold["Poblacion_2024"] = gold["Poblacion_2024"].fillna(0).astype(int)

    # Creamos una nueva métrica utilizando los datos de los que disponemos
    gold["colegios_por_10k"] = (
        gold["num_colegios"] / (gold["Poblacion_2024"] / 10000)
    ).replace([float("inf"), float("nan")], 0)

    # Guardamos
    out_path = os.path.join(GOLD_DIR, "gold_colegios_poblacion_distritos.parquet")
    gold.to_parquet(out_path, compression="snappy")
    print(f"✔ GOLD guardado en {out_path}")

    # Creamos el mapa interactivo.  
    m = folium.Map(location=[39.47, -0.38], zoom_start=11)

    gold["coddistrit_str"] = gold["coddistrit"].astype(str)

    folium.Choropleth(
        geo_data=gold.to_json(),
        data=gold,
        columns=["coddistrit_str", "colegios_por_10k", "num_publicos", "num_concertados", "num_privados"],
        key_on="feature.properties.coddistrit_str",
        fill_color="Purples",
        fill_opacity=0.7,
        line_opacity=0.3,
        legend_name="Colegios por 10.000 habitantes",
    ).add_to(m)

    folium.GeoJson(
        gold,
        tooltip=folium.GeoJsonTooltip(
            fields=["nombre", "num_colegios", "Poblacion_2024", "colegios_por_10k", "num_publicos", "num_concertados", "num_privados"],
            aliases=["Distrito:", "Colegios:", "Población 2024:", "Colegios por 10k hab.:", "Colegios públicos:", "Colegios concertados:", "Colegios privados:"],
            localize=True
        )
    ).add_to(m)

    map_path = os.path.join(GOLD_DIR, "mapa_colegios_poblacion_distritos.html")
    m.save(map_path)
    print(f"✔ Mapa interactivo creado en {map_path}")


# Creamos el DAG
with DAG(
    dag_id="colegios_por_distrito",
    start_date=datetime(2024,1,1),
    schedule_interval=None,
    catchup=False,
    tags=["gold", "colegios", "distritos"]
) as dag:

    tarea = PythonOperator(
        task_id="procesar_colegios",
        python_callable=procesar_colegios_distritos
    )