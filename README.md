# Proyecto de Análisis de Recursos Públicos en Valencia

## 📋 Descripción General

Este proyecto analiza la distribución de recursos públicos en la ciudad de València por distritos, utilizando datos abiertos del Ayuntamiento de Valencia. El análisis incluye hospitales, colegios, centros sociales y viviendas de protección pública (VPP).

## 🎯 Objetivos

- Analizar la distribución geográfica de recursos públicos por distrito
- Calcular métricas normalizadas por población (recursos por 10.000 habitantes)
- Visualizar la información mediante mapas interactivos
- Identificar desigualdades en la distribución de servicios públicos

## 🏗️ Arquitectura del Proyecto

El proyecto sigue una arquitectura de datos en capas (Medallion Architecture):

```
data/
├── bronze/     # Datos originales en formato GeoJSON (descargados del portal de datos abiertos)
├── silver/     # Datos procesados y estandarizados (formato Parquet)
└── gold/       # Datos agregados y métricas finales (análisis por distrito)
```

### Flujo de Datos

**BRONZE → SILVER → GOLD**

1. **Bronze**: Descarga de datos desde el portal de datos abiertos de Valencia
2. **Silver**: Normalización, estandarización de columnas y conversión a formato Parquet
3. **Gold**: Agregación por distrito, cálculo de métricas y generación de visualizaciones

## 📊 Datasets Utilizados

### Fuentes de Datos
- **Distritos y Barrios**: Geometrías administrativas de Valencia
- **Hospitales y Centros de Salud**: Ubicación y características
- **Centros Educativos**: Colegios públicos, privados y concertados
- **Centros Sociales**: Para jóvenes, mayores y población general
- **Viviendas VPP**: Viviendas de protección pública por distrito
- **Padrón Municipal**: Población por distrito (2024)

## 🛠️ Tecnologías Utilizadas

- **Apache Airflow**: Orquestación de pipelines de datos
- **Python 3.10+**
- **GeoPandas**: Análisis de datos geoespaciales
- **Pandas**: Manipulación de datos tabulares
- **Folium**: Generación de mapas interactivos
- **Matplotlib/Seaborn**: Visualizaciones estáticas

## 📁 Estructura de DAGs (Airflow)

### DAGs de Procesamiento Silver
- `geojson_bronze_to_silver`: Procesa archivos GeoJSON a Parquet con estandarización

### DAGs de Análisis Gold
- `hospitales_por_distrito`: Analiza distribución de hospitales
- `colegios_por_distrito`: Analiza centros educativos por tipo
- `centros_por_distrito`: Analiza centros sociales (juventud, mayores, general)
- `viviendasvpp_por_distrito`: Analiza viviendas de protección pública

## 🔍 Metodología de Análisis

### 1. Asignación de Recursos a Distritos
Se utiliza **spatial join** (unión espacial) para asignar cada recurso a su distrito correspondiente:

### 2. Normalización de Población
Se combina con datos del padrón municipal para obtener población actualizada por distrito.

**Nota importante sobre nombres**: Los datos de distritos usan nombres en valenciano normalizados, mientras que el padrón usa nombres con variaciones. Se aplica un mapeo manual para casos conflictivos:

```python
mapeo_nombres = {
    "JESÚS": "JESUS",
    "POBLES DEL NORD": "POBLATS DEL NORD",
    "ALGIRÓS": "ALGIROS",
    # ... etc
}
```

### 3. Cálculo de Métricas
Se calculan indicadores normalizados por población:

```python
recursos_por_10k = (num_recursos / (poblacion_2024 / 10000))
```

Esto permite comparar distritos de diferentes tamaños poblacionales.

## ⚠️ Problemas Conocidos y Soluciones

### Problema 1: Geometrías Múltiples en Poblats del Nord

**Síntoma**: Poblats del Nord mostraba valores duplicados/incorrectos en todas las métricas.

**Causa**: Poblats del Nord es un distrito especial formado por varios núcleos poblacionales dispersos (Borbotó, Carpesa, Benifaraig, Massarrojos). En el archivo GeoJSON, aparece con 4 geometrías separadas con el mismo `coddistrit=17`.

**Solución**: Aplicar `dissolve` al cargar los distritos para consolidar geometrías múltiples:

```python
distritos = gpd.read_parquet("districtes-distritos.parquet")
distritos = distritos.dissolve(by='coddistrit', aggfunc='first').reset_index()
```

### Problema 2: Datos Limitados en Origen

**Síntoma**: Algunos distritos tienen muy pocos recursos registrados (ej: solo 4 hospitales en Poblats del Nord).

**Causa**: Los datos oficiales del Ayuntamiento de Valencia pueden estar incompletos o corresponder solo a centros específicos (consultorios locales, no hospitales completos).

**Solución**: Los cálculos son correctos basándose en los datos disponibles. Se debe interpretar con precaución y validar con fuentes adicionales si es necesario.

## 📈 Outputs Generados

Para cada tipo de recurso se generan:

1. **Jupyter Notebook graficado** (`data/gold/`):
   - Visualización de gráficas por distrito
   - Visualización de mapas no interactivos por distrito
   - Comentarios sobre los resultados obtenidos

3. **Mapa Interactivo HTML** (`data/gold/`):
   - Visualización coroplética (colores por intensidad)
   - Tooltips con información detallada por distrito
   - Navegable y exportable
  
4. **Archivo Parquet Gold** (`data/gold/`):
   - Contiene todas las métricas calculadas por distrito
   - Incluye geometrías para visualización
   - Formato optimizado para análisis posteriores

## 🔄 Mejoras Futuras

- [ ] Incorporar más fuentes de datos (transporte público, zonas verdes)
- [ ] Análisis temporal (comparación entre años)
- [ ] Dashboard interactivo con Streamlit o Dash
- [ ] Análisis de accesibilidad (distancia a servicios)
- [ ] Machine Learning para predicción de necesidades

## 📝 Notas Técnicas

### CRS (Sistema de Referencia de Coordenadas)
- **Entrada**: Variable según fuente
- **Estandarizado**: EPSG:4326 (WGS 84) en capa Silver
- **Spatial Join**: Todos los datos se convierten al CRS de distritos antes de la unión

### Formato de Almacenamiento
- **GeoJSON** (Bronze): Formato original, legible pero pesado
- **Parquet** (Silver/Gold): Formato columnar, comprimido y optimizado para análisis

## 👥 Autor
Jesús García Soriano    
Proyecto desarrollado como análisis de datos abiertos de la ciudad de Valencia.

## 📄 Licencia

Los datos utilizados provienen del portal de datos abiertos del Ayuntamiento de Valencia y están sujetos a sus términos de uso.

---

**Última actualización**: Noviembre 2025
