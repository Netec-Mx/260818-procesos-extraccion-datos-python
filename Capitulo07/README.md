# Automatización de un proceso ETL

## Metadata

| Campo | Valor |
|-------|-------|
| **Duración** | 15 minutos |
| **Complejidad** | Media |
| **Nivel Bloom** | Crear |

## Descripción General

En este laboratorio diseñarás e implementarás un pipeline ETL modular en Python que extrae datos desde PostgreSQL, los transforma con agregaciones, y los carga tanto en una nueva tabla de base de datos como en un bucket S3 emulado con LocalStack. Además, automatizarás su ejecución periódica con `schedule` y crearás un DAG de Apache Airflow para orquestación profesional.

## Objetivos de Aprendizaje

- [ ] Diseñar un pipeline ETL modular con funciones separadas `extract()`, `transform()` y `load()` en un archivo Python reutilizable
- [ ] Automatizar la ejecución periódica del pipeline usando `schedule` con logging integrado mediante `loguru`
- [ ] Crear y registrar un DAG en Apache Airflow con tres tareas secuenciales que orquesten el pipeline completo
- [ ] Integrar la carga de archivos procesados a un bucket S3 emulado con LocalStack usando `boto3`

## Prerrequisitos

### Conocimientos Previos

- Lab 06 completado: dataset `nyc_taxi_clean.csv` y tabla `taxi_trips_clean` poblada en PostgreSQL
- Familiaridad con el patrón ETL y diseño modular de pipelines (Lección 7.1)
- Conocimiento básico de Docker y Docker Compose

### Acceso y Servicios

| Servicio | Estado Requerido |
|----------|-----------------|
| PostgreSQL 16.2 (contenedor `postgres_etl`) | Corriendo en puerto 5433 |
| LocalStack 3.1.0 | Corriendo en puerto 4566 |
| Apache Airflow 2.8.1 | Base de datos inicializada |
| Docker / Docker Compose | Instalados y funcionales |

## Entorno del Laboratorio

### Software Adicional

| Paquete | Versión |
|---------|---------|
| schedule | 1.2.1 |
| loguru | 0.7.2 |
| boto3 | 1.34.34 |
| Apache Airflow | 2.8.1 |
| LocalStack | 3.1.0 |

### Preparación del Entorno

**Paso 0.1:** Activar el entorno virtual e instalar dependencias adicionales.

```bash
cd ~/etl_course/labs/lab01
source venv_etl/bin/activate
pip install schedule==1.2.1 loguru==0.7.2 boto3==1.34.34 apache-airflow==2.8.1
```

**Paso 0.2:** Iniciar LocalStack con Docker Compose. Agregar el servicio al archivo de configuración existente:

```bash
cat >> config/docker-compose.yml << 'EOF'

  localstack:
    image: localstack/localstack:3.1.0
    container_name: localstack_etl
    ports:
      - "4566:4566"
    environment:
      - SERVICES=s3
      - DEBUG=0
    volumes:
      - "/var/run/docker.sock:/var/run/docker.sock"
EOF
```

```bash
docker compose -f config/docker-compose.yml up -d localstack
```

**Paso 0.3:** Configurar la variable `AIRFLOW_HOME` e inicializar Airflow:

```bash
export AIRFLOW_HOME=~/etl_course/labs/lab01/airflow
mkdir -p $AIRFLOW_HOME/dags
airflow db init
airflow users create \
  --username admin \
  --password airflow_admin_pass \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com
```

**Paso 0.4:** Crear la estructura de directorios del laboratorio:

```bash
mkdir -p src/etl logs
```

## Paso a Paso

### Paso 1: Crear el Módulo ETL Modular

**Objetivo:** Implementar el pipeline con funciones `extract()`, `transform()` y `load()` separadas, siguiendo el patrón de diseño modular de la Lección 7.1.

**Instrucciones:**

1. Crear el archivo `src/etl_pipeline.py`:

```python
# src/etl_pipeline.py
# Pipeline ETL modular para datos de NYC Taxi

import os
import pandas as pd
import boto3
from sqlalchemy import create_engine, text
from loguru import logger
from pathlib import Path
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

# Configuración de logging
LOG_PATH = Path(__file__).resolve().parent.parent / "logs" / "etl_pipeline.log"
logger.add(str(LOG_PATH), rotation="10 MB", level="INFO")

# Configuración de base de datos
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5433")
DB_NAME = os.getenv("DB_NAME", "etl_db")
DB_USER = os.getenv("DB_USER", "etl_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "Etl$ecure2024!")

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Configuración S3 (LocalStack)
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:4566")
S3_BUCKET = "etl-processed-data"


def get_engine():
    """Crea y retorna un engine de SQLAlchemy."""
    return create_engine(DATABASE_URL)


def extract() -> pd.DataFrame:
    """Extrae datos desde la tabla taxi_trips_clean en PostgreSQL."""
    logger.info("Iniciando extracción desde PostgreSQL...")
    engine = get_engine()
    query = "SELECT * FROM taxi_trips_clean"
    df = pd.read_sql(query, engine)
    logger.info(f"Extracción completada. Registros: {len(df)}")
    engine.dispose()
    return df


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Aplica agregaciones: total de viajes y promedio de tarifa por día."""
    logger.info("Iniciando transformación...")

    # Asegurar que la columna de fecha existe
    if "pickup_datetime" in df.columns:
        df["pickup_date"] = pd.to_datetime(df["pickup_datetime"]).dt.date
    elif "tpep_pickup_datetime" in df.columns:
        df["pickup_date"] = pd.to_datetime(df["tpep_pickup_datetime"]).dt.date
    else:
        raise ValueError("No se encontró columna de fecha de recogida")

    # Agregación por día
    df_agg = df.groupby("pickup_date").agg(
        total_trips=("pickup_date", "count"),
        avg_fare=("fare_amount", "mean"),
        avg_distance=("trip_distance", "mean"),
        total_revenue=("total_amount", "sum")
    ).reset_index()

    df_agg["avg_fare"] = df_agg["avg_fare"].round(2)
    df_agg["avg_distance"] = df_agg["avg_distance"].round(2)
    df_agg["total_revenue"] = df_agg["total_revenue"].round(2)

    logger.info(f"Transformación completada. Filas agregadas: {len(df_agg)}")
    return df_agg


def load(df: pd.DataFrame) -> None:
    """Carga datos en PostgreSQL y sube CSV a S3 (LocalStack)."""
    logger.info("Iniciando carga...")

    # 1. Cargar en PostgreSQL
    engine = get_engine()
    df.to_sql(
        "taxi_trips_aggregated",
        engine,
        if_exists="replace",
        index=False
    )
    logger.info("Datos cargados en tabla 'taxi_trips_aggregated'.")
    engine.dispose()

    # 2. Guardar CSV localmente
    csv_path = Path(__file__).resolve().parent.parent / "data" / "processed" / "nyc_taxi_aggregated.csv"
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(csv_path, index=False)
    logger.info(f"CSV guardado en: {csv_path}")

    # 3. Subir a S3 (LocalStack)
    upload_to_s3(str(csv_path), "nyc_taxi_aggregated.csv")
    upload_to_s3(
        str(Path(__file__).resolve().parent.parent / "data" / "processed" / "nyc_taxi_clean.csv"),
        "nyc_taxi_clean.csv"
    )
    logger.info("Carga completada exitosamente.")


def upload_to_s3(file_path: str, s3_key: str) -> None:
    """Sube un archivo al bucket S3 en LocalStack."""
    s3_client = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1"
    )

    # Crear bucket si no existe
    try:
        s3_client.head_bucket(Bucket=S3_BUCKET)
    except Exception:
        s3_client.create_bucket(Bucket=S3_BUCKET)
        logger.info(f"Bucket '{S3_BUCKET}' creado.")

    s3_client.upload_file(file_path, S3_BUCKET, s3_key)
    logger.info(f"Archivo '{s3_key}' subido a s3://{S3_BUCKET}/{s3_key}")


def run_pipeline() -> None:
    """Orquesta la ejecución completa del pipeline ETL."""
    logger.info("=" * 60)
    logger.info("INICIO DEL PIPELINE ETL")
    logger.info("=" * 60)
    try:
        df_raw = extract()
        df_transformed = transform(df_raw)
        load(df_transformed)
        logger.info("Pipeline ETL finalizado sin errores.")
    except Exception as e:
        logger.error(f"Error en el pipeline: {e}")
        raise


if __name__ == "__main__":
    run_pipeline()
```

2. Verificar que el módulo se ejecuta correctamente:

```bash
cd ~/etl_course/labs/lab01
python src/etl_pipeline.py
```

**Salida esperada:**

```
2024-XX-XX HH:MM:SS | INFO | ============================================================
2024-XX-XX HH:MM:SS | INFO | INICIO DEL PIPELINE ETL
2024-XX-XX HH:MM:SS | INFO | Iniciando extracción desde PostgreSQL...
2024-XX-XX HH:MM:SS | INFO | Extracción completada. Registros: XXXX
2024-XX-XX HH:MM:SS | INFO | Iniciando transformación...
2024-XX-XX HH:MM:SS | INFO | Transformación completada. Filas agregadas: XX
2024-XX-XX HH:MM:SS | INFO | Iniciando carga...
2024-XX-XX HH:MM:SS | INFO | Datos cargados en tabla 'taxi_trips_aggregated'.
2024-XX-XX HH:MM:SS | INFO | Bucket 'etl-processed-data' creado.
2024-XX-XX HH:MM:SS | INFO | Archivo 'nyc_taxi_aggregated.csv' subido a s3://etl-processed-data/...
2024-XX-XX HH:MM:SS | INFO | Pipeline ETL finalizado sin errores.
```

**Verificación:**

```bash
# Verificar tabla en PostgreSQL
docker exec postgres_etl psql -U etl_user -d etl_db -c "SELECT * FROM taxi_trips_aggregated LIMIT 5;"

# Verificar archivo de log
cat logs/etl_pipeline.log | tail -10
```

---

### Paso 2: Automatizar con Schedule

**Objetivo:** Crear un script que ejecute el pipeline periódicamente usando `schedule`, simulando una automatización ligera.

**Instrucciones:**

1. Crear el archivo `src/run_etl.py`:

```python
# src/run_etl.py
# Script de automatización del pipeline ETL con schedule

import time
import schedule
from loguru import logger
from pathlib import Path

# Importar el pipeline
from etl_pipeline import run_pipeline

LOG_PATH = Path(__file__).resolve().parent.parent / "logs" / "etl_pipeline.log"
logger.add(str(LOG_PATH), rotation="10 MB", level="INFO")


def job():
    """Ejecuta el pipeline ETL con manejo de errores."""
    try:
        logger.info("Ejecución programada iniciada.")
        run_pipeline()
        logger.info("Ejecución programada completada exitosamente.")
    except Exception as e:
        logger.error(f"Error en ejecución programada: {e}")


# Programar ejecución cada hora
schedule.every(1).hours.do(job)

if __name__ == "__main__":
    logger.info("Scheduler ETL iniciado. Ejecutando primera iteración...")
    job()  # Ejecutar inmediatamente la primera vez
    logger.info("Próxima ejecución programada en 1 hora. Ctrl+C para detener.")

    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # Revisar cada 60 segundos
    except KeyboardInterrupt:
        logger.info("Scheduler detenido por el usuario.")
```

2. Ejecutar el scheduler (detener con `Ctrl+C` después de la primera ejecución):

```bash
cd ~/etl_course/labs/lab01/src
python run_etl.py
```

**Salida esperada:**

```
2024-XX-XX HH:MM:SS | INFO | Scheduler ETL iniciado. Ejecutando primera iteración...
2024-XX-XX HH:MM:SS | INFO | Ejecución programada iniciada.
2024-XX-XX HH:MM:SS | INFO | INICIO DEL PIPELINE ETL
...
2024-XX-XX HH:MM:SS | INFO | Ejecución programada completada exitosamente.
2024-XX-XX HH:MM:SS | INFO | Próxima ejecución programada en 1 hora. Ctrl+C para detener.
^C
2024-XX-XX HH:MM:SS | INFO | Scheduler detenido por el usuario.
```

**Verificación:**

```bash
# Confirmar que el log registra la ejecución programada
grep "Ejecución programada" ~/etl_course/labs/lab01/logs/etl_pipeline.log
```

---

### Paso 3: Crear el DAG de Apache Airflow

**Objetivo:** Definir un DAG con tres `PythonOperator` secuenciales que orquesten las etapas extract, transform y load del pipeline.

**Instrucciones:**

1. Crear el archivo del DAG:

```bash
export AIRFLOW_HOME=~/etl_course/labs/lab01/airflow
```

2. Crear `airflow/dags/etl_dag.py`:

```python
# airflow/dags/etl_dag.py
# DAG de Airflow para orquestación del pipeline ETL de NYC Taxi

import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

# Agregar el directorio src al path para importar el módulo
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "src"))

from etl_pipeline import extract, transform, load

# Argumentos por defecto del DAG
default_args = {
    "owner": "etl_team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Variable compartida entre tareas usando XCom
def extract_task(**kwargs):
    """Tarea de extracción: lee datos y los pasa via XCom."""
    df = extract()
    # Guardar como CSV temporal para pasar entre tareas
    tmp_path = "/tmp/etl_extract_output.csv"
    df.to_csv(tmp_path, index=False)
    return tmp_path


def transform_task(**kwargs):
    """Tarea de transformación: lee datos extraídos y aplica agregaciones."""
    import pandas as pd
    ti = kwargs["ti"]
    tmp_path = ti.xcom_pull(task_ids="extract_task")
    df = pd.read_csv(tmp_path)
    df_transformed = transform(df)
    output_path = "/tmp/etl_transform_output.csv"
    df_transformed.to_csv(output_path, index=False)
    return output_path


def load_task(**kwargs):
    """Tarea de carga: carga datos transformados en PostgreSQL y S3."""
    import pandas as pd
    ti = kwargs["ti"]
    tmp_path = ti.xcom_pull(task_ids="transform_task")
    df = pd.read_csv(tmp_path)
    load(df)


# Definición del DAG
with DAG(
    dag_id="etl_nyc_taxi_pipeline",
    default_args=default_args,
    description="Pipeline ETL automatizado para datos de NYC Taxi",
    schedule_interval="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["etl", "nyc_taxi"],
) as dag:

    t_extract = PythonOperator(
        task_id="extract_task",
        python_callable=extract_task,
    )

    t_transform = PythonOperator(
        task_id="transform_task",
        python_callable=transform_task,
    )

    t_load = PythonOperator(
        task_id="load_task",
        python_callable=load_task,
    )

    # Dependencias secuenciales
    t_extract >> t_transform >> t_load
```

3. Verificar que Airflow detecta el DAG:

```bash
export AIRFLOW_HOME=~/etl_course/labs/lab01/airflow
airflow dags list | grep etl_nyc_taxi_pipeline
```

**Salida esperada:**

```
etl_nyc_taxi_pipeline | /home/.../airflow/dags/etl_dag.py | etl_team | False
```

4. Validar la sintaxis del DAG:

```bash
python ~/etl_course/labs/lab01/airflow/dags/etl_dag.py
```

Si no hay errores, el comando termina sin output (exit code 0).

**Verificación:**

```bash
airflow dags list-import-errors
```

Debe mostrar `No data found` o una lista vacía, indicando que no hay errores de importación.

---

### Paso 4: Verificar Integración con S3 (LocalStack)

**Objetivo:** Confirmar que los archivos fueron subidos correctamente al bucket S3 emulado.

**Instrucciones:**

1. Listar el contenido del bucket usando `aws` CLI con endpoint de LocalStack:

```bash
aws --endpoint-url=http://localhost:4566 s3 ls s3://etl-processed-data/
```

Si no tienes `aws` CLI instalado, usa Python:

```python
python -c "
import boto3
s3 = boto3.client('s3', endpoint_url='http://localhost:4566',
                  aws_access_key_id='test', aws_secret_access_key='test',
                  region_name='us-east-1')
response = s3.list_objects_v2(Bucket='etl-processed-data')
for obj in response.get('Contents', []):
    print(f\"  {obj['Key']} ({obj['Size']} bytes)\")
"
```

**Salida esperada:**

```
  nyc_taxi_aggregated.csv (XXX bytes)
  nyc_taxi_clean.csv (XXXXX bytes)
```

2. Descargar y verificar el archivo desde S3:

```bash
python -c "
import boto3
s3 = boto3.client('s3', endpoint_url='http://localhost:4566',
                  aws_access_key_id='test', aws_secret_access_key='test',
                  region_name='us-east-1')
s3.download_file('etl-processed-data', 'nyc_taxi_aggregated.csv', '/tmp/verify_s3.csv')
import pandas as pd
df = pd.read_csv('/tmp/verify_s3.csv')
print(f'Filas: {len(df)}, Columnas: {list(df.columns)}')
print(df.head(3))
"
```

**Salida esperada:**

```
Filas: XX, Columnas: ['pickup_date', 'total_trips', 'avg_fare', 'avg_distance', 'total_revenue']
  pickup_date  total_trips  avg_fare  avg_distance  total_revenue
0  2024-01-XX          XXX     XX.XX          X.XX        XXXXX.XX
...
```

## Validación y Testing

Ejecutar las siguientes verificaciones para confirmar que todo el laboratorio está completo:

```bash
cd ~/etl_course/labs/lab01

echo "=== 1. Verificar módulo ETL ==="
python -c "from src.etl_pipeline import extract, transform, load; print('Módulo importado correctamente')"

echo "=== 2. Verificar tabla PostgreSQL ==="
docker exec postgres_etl psql -U etl_user -d etl_db -c "SELECT COUNT(*) as filas FROM taxi_trips_aggregated;"

echo "=== 3. Verificar bucket S3 ==="
python -c "
import boto3
s3 = boto3.client('s3', endpoint_url='http://localhost:4566',
                  aws_access_key_id='test', aws_secret_access_key='test',
                  region_name='us-east-1')
objs = s3.list_objects_v2(Bucket='etl-processed-data')
print(f\"Archivos en S3: {len(objs.get('Contents', []))}\")"

echo "=== 4. Verificar DAG de Airflow ==="
export AIRFLOW_HOME=~/etl_course/labs/lab01/airflow
airflow dags list | grep etl_nyc_taxi_pipeline && echo "DAG registrado OK"

echo "=== 5. Verificar log ==="
test -f logs/etl_pipeline.log && echo "Log existe OK" || echo "ERROR: Log no encontrado"
```

**Resultado esperado:** Los cinco checks deben mostrar resultados positivos sin errores.

## Solución de Problemas

### Problema 1: Error de conexión a LocalStack

**Síntomas:**
```
botocore.exceptions.EndpointConnectionError: Could not connect to the endpoint URL: "http://localhost:4566"
```

**Causa:** El contenedor de LocalStack no está corriendo o no ha terminado de inicializarse.

**Solución:**
```bash
# Verificar estado del contenedor
docker ps | grep localstack

# Si no está corriendo, iniciarlo
docker compose -f ~/etl_course/labs/lab01/config/docker-compose.yml up -d localstack

# Esperar a que esté listo (verificar health)
until curl -s http://localhost:4566/_localstack/health | grep -q '"s3": "available"'; do
  echo "Esperando a LocalStack..."
  sleep 2
done
echo "LocalStack listo."
```

### Problema 2: DAG no aparece en la lista de Airflow

**Síntomas:**
```bash
airflow dags list | grep etl_nyc_taxi_pipeline
# No devuelve resultados
```

**Causa:** La variable `AIRFLOW_HOME` no apunta al directorio correcto, o el archivo del DAG tiene errores de importación (por ejemplo, `src/etl_pipeline.py` no está en el `sys.path`).

**Solución:**
```bash
# 1. Verificar AIRFLOW_HOME
echo $AIRFLOW_HOME
# Debe ser: /home/<usuario>/etl_course/labs/lab01/airflow

# 2. Verificar errores de importación
export AIRFLOW_HOME=~/etl_course/labs/lab01/airflow
airflow dags list-import-errors

# 3. Si hay error de importación del módulo, verificar el path en el DAG
python -c "
from pathlib import Path
dag_path = Path('$AIRFLOW_HOME/dags/etl_dag.py')
src_path = dag_path.resolve().parent.parent.parent / 'src'
print(f'Path esperado para src: {src_path}')
print(f'Existe: {src_path.exists()}')
"

# 4. Si el path es incorrecto, editar etl_dag.py y ajustar la línea sys.path.insert
```

## Limpieza

Para liberar recursos después del laboratorio (ejecutar solo si no continuarás con el Lab 08):

```bash
# Detener LocalStack (NO ejecutar si continuarás con Lab 08)
# docker compose -f ~/etl_course/labs/lab01/config/docker-compose.yml stop localstack

# La tabla taxi_trips_aggregated y el bucket S3 son necesarios para el Lab 08
# NO eliminarlos
```

> **Nota:** La tabla `taxi_trips_aggregated` en PostgreSQL y el bucket `etl-processed-data` en LocalStack son insumos directos del Lab 08. No los elimines.

## Resumen

En este laboratorio has completado las siguientes tareas:

| Componente | Archivo | Función |
|-----------|---------|---------|
| Pipeline modular | `src/etl_pipeline.py` | Funciones `extract()`, `transform()`, `load()` con logging |
| Automatización | `src/run_etl.py` | Ejecución periódica con `schedule` cada hora |
| Orquestación | `airflow/dags/etl_dag.py` | DAG con 3 PythonOperators secuenciales |
| Almacenamiento cloud | LocalStack S3 | Bucket `etl-processed-data` con CSV procesados |

**Conceptos clave aplicados:**
- Separación de responsabilidades en etapas ETL independientes
- Patrón de pipeline modular con orquestador centralizado
- Logging estructurado para observabilidad en producción
- Integración con servicios cloud mediante emulación local

### Recursos Adicionales

- [Documentación de Apache Airflow: Conceptos de DAGs](https://airflow.apache.org/docs/apache-airflow/2.8.1/core-concepts/dags.html)
- [Documentación de LocalStack: Servicio S3](https://docs.localstack.cloud/user-guide/aws/s3/)
- [Documentación de schedule](https://schedule.readthedocs.io/en/stable/)
- [Documentación de loguru](https://loguru.readthedocs.io/en/stable/)
