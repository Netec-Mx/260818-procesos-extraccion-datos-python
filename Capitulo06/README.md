# Preparación de un dataset real

## Metadata

| Campo | Valor |
|-------|-------|
| **Duración** | 70 minutos |
| **Complejidad** | Media |
| **Nivel Bloom** | Aplicar |
| **Tecnologías** | Python 3.12.1, pandas 2.2.0, NumPy 1.26.4, scikit-learn 1.4.0, PostgreSQL 16.2, SQLAlchemy 2.0.25, Docker 25.0.3 |

---

## Descripción General

En este laboratorio aplicarás un flujo completo de preparación de datos sobre un subset del dataset público NYC Taxi Trips 2023 (100,000 registros). Partirás del diagnóstico de calidad —identificando nulos, duplicados y outliers— para luego limpiar, transformar, enriquecer mediante joins, normalizar variables numéricas y finalmente exportar el dataset limpio a CSV y a PostgreSQL. El resultado será el insumo directo del Laboratorio 07.

---

## Objetivos de Aprendizaje

- [ ] Identificar y cuantificar inconsistencias, valores nulos, duplicados y outliers en un dataset real usando pandas
- [ ] Aplicar técnicas de imputación de valores faltantes (mediana para numéricos, moda para categóricos) y eliminación de duplicados
- [ ] Ejecutar transformaciones (columnas derivadas), joins con una lookup table y agregaciones por zona/hora
- [ ] Normalizar variables numéricas con MinMaxScaler y exportar el dataset limpio a CSV y PostgreSQL

---

## Prerrequisitos

### Conocimientos previos
- Creación de DataFrames con pandas, indexación y filtrado básico
- Conceptos de calidad de datos: completitud, unicidad, exactitud, consistencia
- Conexión básica a PostgreSQL mediante SQLAlchemy (Labs anteriores)

### Acceso requerido
- Entorno virtual `venv_etl` activo en `~/etl_course/labs/lab01/`
- Contenedor Docker `postgres_etl` corriendo en puerto **5433**
- Conexión a Internet para descargar el dataset de ejemplo

---

## Entorno del Laboratorio

### Software requerido

| Componente | Versión |
|------------|---------|
| Python | 3.12.1 |
| pandas | 2.2.1 |
| numpy | 1.26.4 |
| scikit-learn | 1.4.0+ |
| SQLAlchemy | 2.0.29 |
| psycopg2-binary | 2.9.9 |
| Docker | 25.0+ |
| PostgreSQL (contenedor) | 16.2 |

### Configuración inicial

```bash
# Navegar al directorio raíz del proyecto
cd ~/etl_course/labs/lab01

# Activar el entorno virtual
# Linux/macOS:
source venv_etl/bin/activate
# Windows (WSL2):
# source venv_etl/bin/activate

# Instalar dependencias adicionales si no están presentes
pip install scikit-learn==1.4.0 pandas==2.2.1 numpy==1.26.4 sqlalchemy==2.0.29 psycopg2-binary==2.9.9

# Verificar que PostgreSQL está corriendo
docker ps --filter name=postgres_etl --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```

Si el contenedor no está corriendo:

```bash
cd ~/etl_course/labs/lab01/config
docker compose up -d
```

---

## Paso a Paso

### Paso 1: Generar el dataset de práctica y la lookup table

**Objetivo:** Crear un dataset sintético realista de 100,000 registros de viajes de taxi NYC y una tabla de zonas auxiliar, simulando la descarga desde una fuente pública.

**Instrucciones:**

1. Crea el script generador del dataset:

```bash
touch ~/etl_course/labs/lab01/src/generate_nyc_taxi_data.py
```

2. Escribe el siguiente contenido en `src/generate_nyc_taxi_data.py`:

```python
"""
Genera un dataset sintético de NYC Taxi Trips con problemas de calidad
intencionales para practicar técnicas de limpieza y preparación.
"""
import pandas as pd
import numpy as np
from pathlib import Path

np.random.seed(42)

N = 100_000
RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

# --- Generar dataset principal ---
pickup_dates = pd.date_range("2023-01-01", "2023-12-31", periods=N)
dropoff_dates = pickup_dates + pd.to_timedelta(
    np.random.exponential(15, N), unit="m"
)

df = pd.DataFrame({
    "trip_id": range(1, N + 1),
    "pickup_datetime": pickup_dates.strftime("%Y-%m-%d %H:%M:%S"),
    "dropoff_datetime": dropoff_dates.strftime("%Y-%m-%d %H:%M:%S"),
    "passenger_count": np.random.choice([0, 1, 2, 3, 4, 5, 6], N, 
                                         p=[0.02, 0.55, 0.18, 0.10, 0.08, 0.05, 0.02]),
    "trip_distance": np.abs(np.random.lognormal(1.0, 0.8, N)).round(2),
    "pickup_zone_id": np.random.randint(1, 51, N),
    "dropoff_zone_id": np.random.randint(1, 51, N),
    "fare_amount": np.abs(np.random.lognormal(2.5, 0.6, N)).round(2),
    "tip_amount": np.abs(np.random.exponential(3.0, N)).round(2),
    "payment_type": np.random.choice(
        ["Credit Card", "Cash", "No Charge", "Dispute", None],
        N, p=[0.55, 0.30, 0.05, 0.02, 0.08]
    ),
})

# --- Introducir problemas de calidad ---
# Nulos en passenger_count (~5%)
null_mask_pc = np.random.random(N) < 0.05
df.loc[null_mask_pc, "passenger_count"] = np.nan

# Nulos en trip_distance (~3%)
null_mask_td = np.random.random(N) < 0.03
df.loc[null_mask_td, "trip_distance"] = np.nan

# Nulos en fare_amount (~2%)
null_mask_fa = np.random.random(N) < 0.02
df.loc[null_mask_fa, "fare_amount"] = np.nan

# Duplicados (~1% - 1000 filas duplicadas)
duplicates = df.sample(1000, random_state=42)
df = pd.concat([df, duplicates], ignore_index=True)

# Outliers extremos en fare_amount (valores negativos y muy altos)
outlier_idx = np.random.choice(df.index, 200, replace=False)
df.loc[outlier_idx[:100], "fare_amount"] = np.random.uniform(-50, -1, 100).round(2)
df.loc[outlier_idx[100:], "fare_amount"] = np.random.uniform(500, 2000, 100).round(2)

# Mezclar tipo en trip_distance (algunos strings)
str_idx = np.random.choice(df.index, 50, replace=False)
df.loc[str_idx, "trip_distance"] = "N/A"

# Guardar dataset principal
df.to_csv(RAW_DIR / "nyc_taxi_trips_2023.csv", index=False)
print(f"Dataset principal guardado: {RAW_DIR / 'nyc_taxi_trips_2023.csv'}")
print(f"  Registros: {len(df)}")

# --- Generar lookup table de zonas ---
zones = pd.DataFrame({
    "zone_id": range(1, 51),
    "zone_name": [f"Zone_{i:02d}" for i in range(1, 51)],
    "borough": np.random.choice(
        ["Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island"],
        50, p=[0.35, 0.25, 0.20, 0.12, 0.08]
    ),
})

zones.to_csv(RAW_DIR / "taxi_zones_lookup.csv", index=False)
print(f"Lookup table guardada: {RAW_DIR / 'taxi_zones_lookup.csv'}")
print(f"  Zonas: {len(zones)}")
```

3. Ejecuta el script:

```bash
cd ~/etl_course/labs/lab01
python src/generate_nyc_taxi_data.py
```

**Salida esperada:**

```
Dataset principal guardado: /home/<user>/etl_course/labs/lab01/data/raw/nyc_taxi_trips_2023.csv
  Registros: 101000
Lookup table guardada: /home/<user>/etl_course/labs/lab01/data/raw/taxi_zones_lookup.csv
  Zonas: 50
```

**Verificación:**

```bash
ls -lh data/raw/nyc_taxi_trips_2023.csv data/raw/taxi_zones_lookup.csv
head -3 data/raw/nyc_taxi_trips_2023.csv
```

---

### Paso 2: Diagnóstico de calidad del dataset

**Objetivo:** Cargar el dataset, realizar un análisis exploratorio inicial y cuantificar los problemas de calidad (nulos, duplicados, outliers, inconsistencias de tipo).

**Instrucciones:**

1. Crea el script principal de preparación:

```bash
touch ~/etl_course/labs/lab01/src/prepare_nyc_taxi.py
```

2. Escribe la primera sección del script en `src/prepare_nyc_taxi.py`:

```python
"""
Pipeline de preparación del dataset NYC Taxi Trips 2023.
Lab 06-00-01: Diagnóstico, limpieza, transformación y normalización.
"""
import pandas as pd
import numpy as np
from pathlib import Path

# --- Configuración de rutas ---
BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "data" / "raw"
PROCESSED_DIR = BASE_DIR / "data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

# =============================================================
# PASO 2: DIAGNÓSTICO DE CALIDAD
# =============================================================
print("=" * 60)
print("PASO 2: DIAGNÓSTICO DE CALIDAD")
print("=" * 60)

# Cargar dataset
df = pd.read_csv(RAW_DIR / "nyc_taxi_trips_2023.csv")

# 2.1 Exploración estructural
print(f"\n--- Dimensiones ---")
print(f"Filas: {df.shape[0]}, Columnas: {df.shape[1]}")

print(f"\n--- Tipos de datos ---")
print(df.dtypes)

print(f"\n--- Info general ---")
df.info()

print(f"\n--- Estadísticas descriptivas ---")
print(df.describe(include="all").to_string())

# 2.2 Completitud: detección de nulos
print(f"\n--- Análisis de nulos ---")
nulos = df.isnull().sum()
porcentaje_nulos = (nulos / len(df) * 100).round(2)
resumen_nulos = pd.DataFrame({
    "nulos_absolutos": nulos,
    "porcentaje": porcentaje_nulos
})
print(resumen_nulos[resumen_nulos["nulos_absolutos"] > 0])

# Columnas con más del 30% de nulos
cols_criticas = porcentaje_nulos[porcentaje_nulos > 30].index.tolist()
print(f"\nColumnas con >30% nulos: {cols_criticas if cols_criticas else 'Ninguna'}")

# 2.3 Unicidad: detección de duplicados
total_duplicados = df.duplicated().sum()
print(f"\n--- Duplicados ---")
print(f"Filas completamente duplicadas: {total_duplicados}")

duplicados_por_id = df.duplicated(subset=["trip_id"], keep=False).sum()
print(f"Duplicados por trip_id: {duplicados_por_id}")

# 2.4 Exactitud: detección de outliers con IQR
print(f"\n--- Outliers (método IQR) ---")
columnas_numericas = ["trip_distance", "fare_amount", "tip_amount", "passenger_count"]

for col in columnas_numericas:
    # Convertir a numérico para manejar valores mixtos
    serie = pd.to_numeric(df[col], errors="coerce")
    q1 = serie.quantile(0.25)
    q3 = serie.quantile(0.75)
    iqr = q3 - q1
    lower = q1 - 1.5 * iqr
    upper = q3 + 1.5 * iqr
    n_outliers = ((serie < lower) | (serie > upper)).sum()
    print(f"  {col}: {n_outliers} outliers (rango válido: [{lower:.2f}, {upper:.2f}])")

# 2.5 Consistencia de tipos
print(f"\n--- Inconsistencias de tipo ---")
# trip_distance debería ser numérica
td_numeric = pd.to_numeric(df["trip_distance"], errors="coerce")
invalidos_td = df["trip_distance"].notnull() & td_numeric.isnull()
print(f"  trip_distance con valores no numéricos: {invalidos_td.sum()}")
```

3. Ejecuta el diagnóstico:

```bash
cd ~/etl_course/labs/lab01
python src/prepare_nyc_taxi.py
```

**Salida esperada (fragmento):**

```
============================================================
PASO 2: DIAGNÓSTICO DE CALIDAD
============================================================

--- Dimensiones ---
Filas: 101000, Columnas: 10

--- Análisis de nulos ---
                nulos_absolutos  porcentaje
passenger_count            ~5050       ~5.00
trip_distance              ~3030       ~3.00
fare_amount                ~2020       ~2.00
payment_type               ~8080       ~8.00

Columnas con >30% nulos: Ninguna

--- Duplicados ---
Filas completamente duplicadas: 1000
Duplicados por trip_id: 2000

--- Outliers (método IQR) ---
  trip_distance: ~XXXX outliers
  fare_amount: ~XXXX outliers
  ...

--- Inconsistencias de tipo ---
  trip_distance con valores no numéricos: 50
```

**Verificación:** El script debe completarse sin errores y mostrar las cuatro secciones del diagnóstico.

---

### Paso 3: Limpieza del dataset

**Objetivo:** Eliminar duplicados, corregir tipos de datos e imputar valores nulos usando mediana (numéricos) y moda (categóricos).

**Instrucciones:**

1. Agrega la siguiente sección al final de `src/prepare_nyc_taxi.py`:

```python
# =============================================================
# PASO 3: LIMPIEZA
# =============================================================
print("\n" + "=" * 60)
print("PASO 3: LIMPIEZA")
print("=" * 60)

# 3.1 Eliminar duplicados
filas_antes = len(df)
df = df.drop_duplicates(subset=["trip_id"], keep="first")
filas_despues = len(df)
print(f"\nDuplicados eliminados: {filas_antes - filas_despues}")
print(f"Filas restantes: {filas_despues}")

# 3.2 Corregir tipos de datos
# trip_distance: convertir a numérico (valores "N/A" → NaN)
df["trip_distance"] = pd.to_numeric(df["trip_distance"], errors="coerce")

# Parsear columnas de fecha
df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"], errors="coerce")
df["dropoff_datetime"] = pd.to_datetime(df["dropoff_datetime"], errors="coerce")

print(f"\nTipos corregidos:")
print(df[["trip_distance", "pickup_datetime", "dropoff_datetime"]].dtypes)

# 3.3 Imputación de valores nulos
# Numéricos: mediana
columnas_num_imputar = ["passenger_count", "trip_distance", "fare_amount", "tip_amount"]
for col in columnas_num_imputar:
    mediana = df[col].median()
    nulos_antes = df[col].isnull().sum()
    df[col] = df[col].fillna(mediana)
    print(f"  {col}: {nulos_antes} nulos imputados con mediana={mediana:.2f}")

# Categóricos: moda
moda_payment = df["payment_type"].mode()[0]
nulos_payment = df["payment_type"].isnull().sum()
df["payment_type"] = df["payment_type"].fillna(moda_payment)
print(f"  payment_type: {nulos_payment} nulos imputados con moda='{moda_payment}'")

# 3.4 Eliminar outliers en fare_amount (valores negativos y extremos)
fare_q1 = df["fare_amount"].quantile(0.25)
fare_q3 = df["fare_amount"].quantile(0.75)
fare_iqr = fare_q3 - fare_q1
fare_lower = fare_q1 - 1.5 * fare_iqr
fare_upper = fare_q3 + 1.5 * fare_iqr

outliers_fare = (df["fare_amount"] < fare_lower) | (df["fare_amount"] > fare_upper)
print(f"\nOutliers en fare_amount eliminados: {outliers_fare.sum()}")
df = df[~outliers_fare].reset_index(drop=True)

# 3.5 Filtrar passenger_count = 0 (viajes inválidos)
invalidos_pc = (df["passenger_count"] == 0).sum()
df = df[df["passenger_count"] > 0].reset_index(drop=True)
print(f"Viajes con 0 pasajeros eliminados: {invalidos_pc}")

# Verificación post-limpieza
print(f"\n--- Estado post-limpieza ---")
print(f"Filas finales: {len(df)}")
print(f"Nulos restantes: {df.isnull().sum().sum()}")
```

2. Ejecuta nuevamente:

```bash
python src/prepare_nyc_taxi.py
```

**Salida esperada (fragmento):**

```
============================================================
PASO 3: LIMPIEZA
============================================================

Duplicados eliminados: 1000
Filas restantes: 100000

Tipos corregidos:
trip_distance       float64
pickup_datetime     datetime64[ns]
dropoff_datetime    datetime64[ns]

  passenger_count: ~5000 nulos imputados con mediana=2.00
  trip_distance: ~3080 nulos imputados con mediana=X.XX
  fare_amount: ~2000 nulos imputados con mediana=X.XX
  ...
  payment_type: ~8000 nulos imputados con moda='Credit Card'

--- Estado post-limpieza ---
Filas finales: ~95XXX
Nulos restantes: 0
```

**Verificación:**

```python
# Agregar al final del script temporalmente para verificar:
assert df.isnull().sum().sum() == 0, "Aún quedan nulos"
assert df.duplicated(subset=["trip_id"]).sum() == 0, "Aún quedan duplicados"
print("✓ Verificaciones de limpieza pasadas")
```

---

### Paso 4: Transformaciones y enriquecimiento con joins

**Objetivo:** Crear columnas derivadas (duración del viaje, franja horaria), realizar un join con la tabla de zonas y calcular agregaciones.

**Instrucciones:**

1. Agrega la siguiente sección al final de `src/prepare_nyc_taxi.py`:

```python
# =============================================================
# PASO 4: TRANSFORMACIONES Y JOINS
# =============================================================
print("\n" + "=" * 60)
print("PASO 4: TRANSFORMACIONES Y JOINS")
print("=" * 60)

# 4.1 Columnas derivadas
# Duración del viaje en minutos
df["trip_duration_min"] = (
    (df["dropoff_datetime"] - df["pickup_datetime"]).dt.total_seconds() / 60
).round(2)

# Eliminar duraciones negativas o extremas (>180 min)
invalid_duration = (df["trip_duration_min"] <= 0) | (df["trip_duration_min"] > 180)
print(f"Viajes con duración inválida eliminados: {invalid_duration.sum()}")
df = df[~invalid_duration].reset_index(drop=True)

# Franja horaria (hora del pickup)
df["pickup_hour"] = df["pickup_datetime"].dt.hour
df["time_slot"] = pd.cut(
    df["pickup_hour"],
    bins=[0, 6, 12, 18, 24],
    labels=["Madrugada", "Mañana", "Tarde", "Noche"],
    right=False
)

print(f"\nDistribución por franja horaria:")
print(df["time_slot"].value_counts().sort_index())

# 4.2 Join con lookup table de zonas
zones = pd.read_csv(RAW_DIR / "taxi_zones_lookup.csv")

# Join para zona de pickup
df = df.merge(
    zones.rename(columns={
        "zone_id": "pickup_zone_id",
        "zone_name": "pickup_zone_name",
        "borough": "pickup_borough"
    }),
    on="pickup_zone_id",
    how="left"
)

# Join para zona de dropoff
df = df.merge(
    zones.rename(columns={
        "zone_id": "dropoff_zone_id",
        "zone_name": "dropoff_zone_name",
        "borough": "dropoff_borough"
    }),
    on="dropoff_zone_id",
    how="left"
)

print(f"\nColumnas después del join: {df.columns.tolist()}")
print(f"Filas después del join: {len(df)}")

# 4.3 Agregaciones por zona y hora
print(f"\n--- Agregación: promedio de tarifa por borough de pickup ---")
agg_borough = df.groupby("pickup_borough").agg(
    viajes=("trip_id", "count"),
    fare_promedio=("fare_amount", "mean"),
    distancia_promedio=("trip_distance", "mean"),
    duracion_promedio=("trip_duration_min", "mean")
).round(2)
print(agg_borough)

print(f"\n--- Agregación: viajes por franja horaria ---")
agg_hora = df.groupby("time_slot", observed=True).agg(
    viajes=("trip_id", "count"),
    fare_promedio=("fare_amount", "mean")
).round(2)
print(agg_hora)
```

2. Ejecuta:

```bash
python src/prepare_nyc_taxi.py
```

**Salida esperada (fragmento):**

```
============================================================
PASO 4: TRANSFORMACIONES Y JOINS
============================================================

Distribución por franja horaria:
Madrugada    ~XXXXX
Mañana       ~XXXXX
Tarde        ~XXXXX
Noche        ~XXXXX

Columnas después del join: ['trip_id', 'pickup_datetime', ..., 'pickup_zone_name', 'pickup_borough', 'dropoff_zone_name', 'dropoff_borough']

--- Agregación: promedio de tarifa por borough de pickup ---
                viajes  fare_promedio  distancia_promedio  duracion_promedio
pickup_borough
Bronx            XXXX          XX.XX               X.XX              XX.XX
Brooklyn         XXXX          XX.XX               X.XX              XX.XX
Manhattan        XXXX          XX.XX               X.XX              XX.XX
Queens           XXXX          XX.XX               X.XX              XX.XX
Staten Island    XXXX          XX.XX               X.XX              XX.XX
```

**Verificación:** Confirma que no se generaron nulos tras el join:

```python
assert df[["pickup_zone_name", "pickup_borough"]].isnull().sum().sum() == 0
print("✓ Join completado sin nulos")
```

---

### Paso 5: Normalización de variables numéricas

**Objetivo:** Aplicar MinMaxScaler y StandardScaler de scikit-learn a columnas numéricas seleccionadas.

**Instrucciones:**

1. Agrega la siguiente sección al final de `src/prepare_nyc_taxi.py`:

```python
# =============================================================
# PASO 5: NORMALIZACIÓN
# =============================================================
print("\n" + "=" * 60)
print("PASO 5: NORMALIZACIÓN")
print("=" * 60)

from sklearn.preprocessing import MinMaxScaler, StandardScaler

# Columnas a normalizar
cols_normalizar = ["trip_distance", "fare_amount", "tip_amount", "trip_duration_min"]

# 5.1 MinMaxScaler (rango 0-1)
scaler_minmax = MinMaxScaler()
cols_minmax = [f"{col}_minmax" for col in cols_normalizar]
df[cols_minmax] = scaler_minmax.fit_transform(df[cols_normalizar])

print("MinMaxScaler aplicado:")
print(df[cols_minmax].describe().loc[["min", "max", "mean"]].round(4))

# 5.2 StandardScaler (media=0, std=1)
scaler_std = StandardScaler()
cols_std = [f"{col}_zscore" for col in cols_normalizar]
df[cols_std] = scaler_std.fit_transform(df[cols_normalizar])

print(f"\nStandardScaler aplicado:")
print(df[cols_std].describe().loc[["mean", "std", "min", "max"]].round(4))

print(f"\nColumnas totales del dataset final: {len(df.columns)}")
print(f"Filas totales del dataset final: {len(df)}")
```

2. Ejecuta:

```bash
python src/prepare_nyc_taxi.py
```

**Salida esperada:**

```
============================================================
PASO 5: NORMALIZACIÓN
============================================================

MinMaxScaler aplicado:
     trip_distance_minmax  fare_amount_minmax  tip_amount_minmax  trip_duration_min_minmax
min                0.0000              0.0000             0.0000                    0.0000
max                1.0000              1.0000             1.0000                    1.0000
mean               X.XXXX              X.XXXX             X.XXXX                    X.XXXX

StandardScaler aplicado:
     trip_distance_zscore  fare_amount_zscore  tip_amount_zscore  trip_duration_min_zscore
mean              0.0000              0.0000             0.0000                    0.0000
std               1.0000              1.0000             1.0000                    1.0000
...
```

**Verificación:**

```python
# Las columnas MinMax deben estar entre 0 y 1
for col in cols_minmax:
    assert df[col].min() >= 0 and df[col].max() <= 1.0001, f"Error en {col}"

# Las columnas Z-score deben tener media ≈ 0
for col in cols_std:
    assert abs(df[col].mean()) < 0.001, f"Media no es ~0 en {col}"

print("✓ Normalización verificada correctamente")
```

---

### Paso 6: Exportación a CSV y PostgreSQL

**Objetivo:** Guardar el dataset limpio en formato CSV y cargarlo en la tabla `taxi_trips_clean` de PostgreSQL.

**Instrucciones:**

1. Agrega la sección final al script `src/prepare_nyc_taxi.py`:

```python
# =============================================================
# PASO 6: EXPORTACIÓN
# =============================================================
print("\n" + "=" * 60)
print("PASO 6: EXPORTACIÓN")
print("=" * 60)

from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# 6.1 Exportar a CSV
output_csv = PROCESSED_DIR / "nyc_taxi_clean.csv"
df.to_csv(output_csv, index=False)
print(f"\n✓ CSV exportado: {output_csv}")
print(f"  Tamaño: {output_csv.stat().st_size / 1_048_576:.2f} MB")
print(f"  Filas: {len(df)}, Columnas: {len(df.columns)}")

# 6.2 Exportar a PostgreSQL
load_dotenv(BASE_DIR / ".env")

DB_USER = os.getenv("DB_USER", "etl_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "Etl$ecure2024!")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5433")
DB_NAME = os.getenv("DB_NAME", "etl_db")

connection_string = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(connection_string)

# Seleccionar columnas para PostgreSQL (sin las normalizadas para ahorrar espacio)
cols_pg = [
    "trip_id", "pickup_datetime", "dropoff_datetime",
    "passenger_count", "trip_distance", "fare_amount", "tip_amount",
    "payment_type", "pickup_zone_id", "dropoff_zone_id",
    "trip_duration_min", "pickup_hour", "time_slot",
    "pickup_zone_name", "pickup_borough",
    "dropoff_zone_name", "dropoff_borough"
]

df_pg = df[cols_pg].copy()

# Cargar en lotes para mejor rendimiento
TABLE_NAME = "taxi_trips_clean"
df_pg.to_sql(
    TABLE_NAME,
    engine,
    if_exists="replace",
    index=False,
    chunksize=10_000,
    method="multi"
)

print(f"\n✓ Tabla '{TABLE_NAME}' creada en PostgreSQL (etl_db)")

# Verificar conteo
with engine.connect() as conn:
    result = conn.execute(
        __import__("sqlalchemy").text(f"SELECT COUNT(*) FROM {TABLE_NAME}")
    )
    count = result.scalar()
    print(f"  Registros en PostgreSQL: {count}")

engine.dispose()
print("\n✓ Pipeline de preparación completado exitosamente")
```

2. Instala `python-dotenv` si no está disponible:

```bash
pip install python-dotenv
```

3. Ejecuta el script completo:

```bash
cd ~/etl_course/labs/lab01
python src/prepare_nyc_taxi.py
```

**Salida esperada (sección final):**

```
============================================================
PASO 6: EXPORTACIÓN
============================================================

✓ CSV exportado: /home/<user>/etl_course/labs/lab01/data/processed/nyc_taxi_clean.csv
  Tamaño: ~XX.XX MB
  Filas: ~95XXX, Columnas: ~25

✓ Tabla 'taxi_trips_clean' creada en PostgreSQL (etl_db)
  Registros en PostgreSQL: ~95XXX

✓ Pipeline de preparación completado exitosamente
```

**Verificación:**

```bash
# Verificar el archivo CSV
wc -l data/processed/nyc_taxi_clean.csv
head -2 data/processed/nyc_taxi_clean.csv

# Verificar en PostgreSQL
docker exec postgres_etl psql -U etl_user -d etl_db -c "SELECT COUNT(*) FROM taxi_trips_clean;"
docker exec postgres_etl psql -U etl_user -d etl_db -c "SELECT * FROM taxi_trips_clean LIMIT 3;"
```

---

## Validación y Pruebas

Crea un script de validación integral:

```bash
touch ~/etl_course/labs/lab01/tests/test_prepare_nyc_taxi.py
```

```python
"""
Tests de validación para el pipeline de preparación NYC Taxi.
"""
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
PROCESSED_DIR = BASE_DIR / "data" / "processed"

def test_csv_output():
    """Verifica que el CSV limpio existe y tiene la estructura esperada."""
    csv_path = PROCESSED_DIR / "nyc_taxi_clean.csv"
    assert csv_path.exists(), "El archivo CSV no existe"
    
    df = pd.read_csv(csv_path)
    
    # Sin nulos en columnas críticas
    critical_cols = ["trip_id", "pickup_datetime", "fare_amount", "trip_distance"]
    for col in critical_cols:
        assert df[col].isnull().sum() == 0, f"Nulos encontrados en {col}"
    
    # Sin duplicados por trip_id
    assert df["trip_id"].duplicated().sum() == 0, "Duplicados en trip_id"
    
    # Columnas derivadas presentes
    assert "trip_duration_min" in df.columns, "Falta trip_duration_min"
    assert "pickup_hour" in df.columns, "Falta pickup_hour"
    assert "time_slot" in df.columns, "Falta time_slot"
    assert "pickup_borough" in df.columns, "Falta pickup_borough"
    
    # Columnas normalizadas presentes
    assert "fare_amount_minmax" in df.columns, "Falta fare_amount_minmax"
    assert "trip_distance_zscore" in df.columns, "Falta trip_distance_zscore"
    
    # Rangos de normalización
    minmax_cols = [c for c in df.columns if c.endswith("_minmax")]
    for col in minmax_cols:
        assert df[col].min() >= -0.001, f"{col} tiene valores < 0"
        assert df[col].max() <= 1.001, f"{col} tiene valores > 1"
    
    print(f"✓ test_csv_output PASSED ({len(df)} filas, {len(df.columns)} columnas)")


def test_postgresql_output():
    """Verifica que la tabla en PostgreSQL existe y tiene datos."""
    connection_string = "postgresql+psycopg2://etl_user:Etl$ecure2024!@localhost:5433/etl_db"
    engine = create_engine(connection_string)
    
    with engine.connect() as conn:
        # Tabla existe
        result = conn.execute(text(
            "SELECT EXISTS (SELECT FROM information_schema.tables "
            "WHERE table_name = 'taxi_trips_clean')"
        ))
        assert result.scalar(), "La tabla taxi_trips_clean no existe"
        
        # Tiene registros
        count = conn.execute(text("SELECT COUNT(*) FROM taxi_trips_clean")).scalar()
        assert count > 90000, f"Muy pocos registros: {count}"
        
        # Columnas esperadas
        cols = conn.execute(text(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'taxi_trips_clean' ORDER BY ordinal_position"
        ))
        col_names = [row[0] for row in cols]
        assert "trip_duration_min" in col_names
        assert "pickup_borough" in col_names
        
        # Sin nulos en fare_amount
        nulls = conn.execute(text(
            "SELECT COUNT(*) FROM taxi_trips_clean WHERE fare_amount IS NULL"
        )).scalar()
        assert nulls == 0, f"Nulos en fare_amount: {nulls}"
    
    engine.dispose()
    print(f"✓ test_postgresql_output PASSED ({count} registros)")


if __name__ == "__main__":
    test_csv_output()
    test_postgresql_output()
    print("\n✓ TODAS LAS VALIDACIONES PASARON")
```

Ejecuta los tests:

```bash
cd ~/etl_course/labs/lab01
python tests/test_prepare_nyc_taxi.py
```

**Salida esperada:**

```
✓ test_csv_output PASSED (~95XXX filas, ~25 columnas)
✓ test_postgresql_output PASSED (~95XXX registros)

✓ TODAS LAS VALIDACIONES PASARON
```

---

## Solución de Problemas

### Problema 1: Error de conexión a PostgreSQL

**Síntomas:**
```
sqlalchemy.exc.OperationalError: (psycopg2.OperationalError) could not connect to server: Connection refused
```

**Causa:** El contenedor Docker `postgres_etl` no está corriendo o el puerto 5433 no está mapeado correctamente.

**Solución:**

```bash
# Verificar estado del contenedor
docker ps -a --filter name=postgres_etl

# Si está detenido, reiniciar
docker start postgres_etl

# Si no existe, recrear desde docker-compose
cd ~/etl_course/labs/lab01/config
docker compose up -d

# Verificar conectividad
docker exec postgres_etl pg_isready -U etl_user -d etl_db
# Debe responder: "accepting connections"

# Verificar puerto desde el host
psql -h localhost -p 5433 -U etl_user -d etl_db -c "SELECT 1;"
```

---

### Problema 2: `to_sql` falla con error de tipo en columna `time_slot`

**Síntomas:**
```
ProgrammingError: (psycopg2.ProgrammingError) can't adapt type 'Interval'
```
o
```
TypeError: Object of type Categorical is not JSON serializable
```

**Causa:** La columna `time_slot` creada con `pd.cut()` tiene tipo `CategoricalDtype`, que SQLAlchemy no puede serializar directamente a PostgreSQL.

**Solución:**

Antes de la llamada a `to_sql`, convierte la columna categórica a string:

```python
# Convertir categorical a string antes de exportar
df_pg["time_slot"] = df_pg["time_slot"].astype(str)
```

Agrega esta línea justo después de crear `df_pg = df[cols_pg].copy()` en el Paso 6.

---

## Limpieza

Si deseas liberar recursos después de completar el laboratorio:

```bash
# Los archivos generados se conservan para el Lab 07
# Solo limpiar si necesitas espacio:

# Eliminar dataset raw (se puede regenerar)
# rm data/raw/nyc_taxi_trips_2023.csv

# El contenedor PostgreSQL se mantiene para laboratorios posteriores
# NO ejecutar: docker stop postgres_etl

# Desactivar entorno virtual al terminar la sesión
deactivate
```

> **Importante:** No elimines `data/processed/nyc_taxi_clean.csv` ni la tabla `taxi_trips_clean` en PostgreSQL. Ambos son insumos directos del Laboratorio 07.

---

## Resumen

En este laboratorio completaste un pipeline integral de preparación de datos:

| Etapa | Acción | Resultado |
|-------|--------|-----------|
| **Diagnóstico** | `isnull()`, `duplicated()`, IQR | Cuantificación de problemas |
| **Limpieza** | `drop_duplicates()`, `fillna()`, `to_numeric()` | 0 nulos, 0 duplicados |
| **Transformación** | Columnas derivadas, `merge()`, `groupby()` | Dataset enriquecido |
| **Normalización** | `MinMaxScaler`, `StandardScaler` | Variables escaladas |
| **Exportación** | `to_csv()`, `to_sql()` | CSV + PostgreSQL |

### Conceptos clave aplicados:
- Las cuatro dimensiones de calidad (completitud, unicidad, exactitud, consistencia)
- Imputación con mediana (robusta a outliers) para numéricos y moda para categóricos
- Método IQR para detección y eliminación de outliers
- Joins tipo left para enriquecer sin perder registros
- Normalización MinMax (rango 0-1) vs StandardScaler (z-score)

### Recursos adicionales:
- [pandas: Working with missing data](https://pandas.pydata.org/docs/user_guide/missing_data.html)
- [scikit-learn: Preprocessing data](https://scikit-learn.org/stable/modules/preprocessing.html)
- [SQLAlchemy: Engine Configuration](https://docs.sqlalchemy.org/en/20/core/engines.html)
