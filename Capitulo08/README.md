# Práctica Final: Desarrollo de un pipeline completo de extracción

| Campo | Detalle |
|-------|---------|
| **Duración** | 85 minutos |
| **Complejidad** | Alta |
| **Nivel Bloom** | Crear |

## Descripción General

En este laboratorio final integrador, evolucionarás el pipeline ETL desarrollado en laboratorios anteriores hacia un sistema de calidad de producción. Implementarás logging estructurado con `loguru`, pruebas automatizadas con `pytest` y `great-expectations`, optimización de rendimiento con `concurrent.futures` y chunking, y documentación profesional con Git y commits semánticos. Al finalizar, tendrás un pipeline completo, observable, testeable y optimizado.

## Objetivos de Aprendizaje

- [ ] Implementar un sistema de logging estructurado con `loguru` y monitoreo de métricas (tiempos de ejecución, registros procesados, errores) con alertas básicas
- [ ] Desarrollar una suite de pruebas unitarias e integración con `pytest` y `great-expectations` para validar la calidad del pipeline ETL
- [ ] Documentar el pipeline con docstrings, README.md y control de versiones Git con commits semánticos
- [ ] Optimizar el rendimiento del pipeline aplicando chunking, indexación en PostgreSQL y paralelización con `concurrent.futures`

## Prerrequisitos

### Conocimientos Previos
- Pipeline ETL funcional (`/workspace/etl/etl_pipeline.py`) del Lab 07
- Familiaridad con pandas, SQLAlchemy y PostgreSQL
- Conceptos básicos de Git y testing en Python

### Accesos y Servicios Activos
- PostgreSQL 16.2 corriendo en contenedor `postgres_etl` (puerto 5433) con tabla `taxi_trips_clean`
- Apache Airflow 2.8.1 accesible en `http://localhost:8080`
- LocalStack 3.1.0 corriendo en puerto 4566 con bucket `etl-processed-data`
- Tabla `taxi_trips_aggregated` existente en `etl_db`

## Entorno del Laboratorio

### Software Requerido

| Herramienta | Versión |
|-------------|---------|
| Python | 3.12.1 |
| loguru | 0.7.2 |
| pytest | 8.0.0 |
| great-expectations | 0.18.9 |
| Git | 2.43.0 |
| PostgreSQL | 16.2 |
| pandas | 2.2.0 |
| concurrent.futures | stdlib |

### Configuración Inicial

```bash
# Activar el entorno virtual existente
cd ~/etl_course/labs/lab01
source venv_etl/bin/activate

# Instalar dependencias adicionales para este laboratorio
pip install loguru==0.7.2 pytest==8.0.0 great-expectations==0.18.9

# Verificar instalaciones
python -c "from loguru import logger; print('loguru OK')"
python -c "import pytest; print(f'pytest {pytest.__version__} OK')"
python -c "import great_expectations; print(f'GE {great_expectations.__version__} OK')"

# Crear directorios necesarios
mkdir -p tests great_expectations logs
```

---

## Paso 1: Implementar Logging Estructurado con Loguru

**Objetivo:** Refactorizar el pipeline para usar `loguru` con rotación de logs, decoradores de timing y registro de métricas en JSON.

### Instrucciones

1. Crea el módulo de logging en `src/logger_config.py`:

```python
"""
Configuración centralizada de logging con loguru para el pipeline ETL.
Incluye rotación de archivos, formato estructurado y métricas en JSON.
"""
import json
import time
import functools
from pathlib import Path
from datetime import datetime
from loguru import logger

# Directorio de logs
LOGS_DIR = Path(__file__).parent.parent / "logs"
LOGS_DIR.mkdir(exist_ok=True)

# Archivo de métricas
METRICS_FILE = LOGS_DIR / "metrics.json"


def setup_logger():
    """Configura loguru con handlers de consola y archivo rotativo."""
    # Remover handler por defecto
    logger.remove()

    # Handler de consola: solo INFO y superior
    logger.add(
        sink=lambda msg: print(msg, end=""),
        level="INFO",
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
               "<level>{level: <8}</level> | "
               "<cyan>{name}</cyan>:<cyan>{function}</cyan> | "
               "<level>{message}</level>",
        colorize=True,
    )

    # Handler de archivo con rotación (10 MB, retención 7 días)
    logger.add(
        sink=str(LOGS_DIR / "etl_pipeline.log"),
        level="DEBUG",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} | {message}",
        rotation="10 MB",
        retention="7 days",
        compression="zip",
        encoding="utf-8",
    )

    return logger


def registrar_metrica(etapa: str, registros: int, duracion: float,
                      estado: str = "OK", error: str = None):
    """
    Registra una métrica de ejecución en metrics.json.

    Args:
        etapa: Nombre de la fase del pipeline (extract, transform, load)
        registros: Cantidad de registros procesados
        duracion: Tiempo de ejecución en segundos
        estado: 'OK' o 'ERROR'
        error: Mensaje de error si aplica
    """
    metrica = {
        "timestamp": datetime.now().isoformat(),
        "etapa": etapa,
        "registros_procesados": registros,
        "duracion_segundos": round(duracion, 3),
        "estado": estado,
        "error": error,
    }

    # Leer métricas existentes o iniciar lista vacía
    metricas = []
    if METRICS_FILE.exists():
        try:
            metricas = json.loads(METRICS_FILE.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, FileNotFoundError):
            metricas = []

    metricas.append(metrica)
    METRICS_FILE.write_text(
        json.dumps(metricas, indent=2, ensure_ascii=False),
        encoding="utf-8"
    )


def monitorear_tiempo(etapa: str):
    """
    Decorador que mide el tiempo de ejecución de una función
    y registra la métrica automáticamente.

    Args:
        etapa: Nombre de la etapa para el registro de métricas
    """
    def decorador(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            logger.info(f"[INICIO] {etapa} - Función: {func.__name__}")
            inicio = time.perf_counter()
            try:
                resultado = func(*args, **kwargs)
                duracion = time.perf_counter() - inicio
                # Determinar cantidad de registros del resultado
                registros = len(resultado) if hasattr(resultado, '__len__') else 0
                registrar_metrica(etapa, registros, duracion, "OK")
                logger.info(
                    f"[FIN] {etapa} | Duración: {duracion:.3f}s | "
                    f"Registros: {registros} | Estado: OK"
                )
                return resultado
            except Exception as e:
                duracion = time.perf_counter() - inicio
                registrar_metrica(etapa, 0, duracion, "ERROR", str(e))
                logger.error(
                    f"[FIN] {etapa} | Duración: {duracion:.3f}s | "
                    f"Estado: ERROR | {e}"
                )
                raise
        return wrapper
    return decorador
```

2. Crea el módulo de alertas en `src/alertas.py`:

```python
"""
Sistema de alertas simuladas por email cuando el pipeline falla.
Usa smtplib con un servidor SMTP ficticio para demostración.
"""
import smtplib
from email.mime.text import MIMEText
from datetime import datetime
from loguru import logger


def enviar_alerta_fallo(etapa: str, error: str, destinatario: str = "admin@etl-pipeline.local"):
    """
    Simula el envío de una alerta por email cuando una etapa del pipeline falla.
    En producción, configurar con un servidor SMTP real.

    Args:
        etapa: Etapa que falló
        error: Descripción del error
        destinatario: Email del destinatario
    """
    asunto = f"[ALERTA ETL] Fallo en etapa: {etapa}"
    cuerpo = (
        f"Pipeline ETL - Alerta de Fallo\n"
        f"{'=' * 40}\n"
        f"Fecha/Hora: {datetime.now().isoformat()}\n"
        f"Etapa: {etapa}\n"
        f"Error: {error}\n"
        f"Acción requerida: Revisar logs en /logs/etl_pipeline.log\n"
    )

    msg = MIMEText(cuerpo)
    msg["Subject"] = asunto
    msg["From"] = "pipeline@etl-system.local"
    msg["To"] = destinatario

    try:
        # Intento de envío simulado (fallará sin servidor SMTP real)
        # En producción: smtp.gmail.com:587 con TLS
        with smtplib.SMTP("localhost", 1025, timeout=5) as server:
            server.sendmail(msg["From"], [destinatario], msg.as_string())
        logger.info(f"Alerta enviada a {destinatario}")
    except (ConnectionRefusedError, OSError):
        # En entorno de desarrollo, solo registrar la alerta en log
        logger.warning(
            f"[ALERTA SIMULADA] No se pudo enviar email. "
            f"Contenido: {asunto} -> {error}"
        )
```

3. Limpia el archivo de métricas anterior si existe:

```bash
# Inicializar metrics.json vacío
echo "[]" > logs/metrics.json
```

### Salida Esperada

```
loguru OK
pytest 8.0.0 OK
GE 0.18.9 OK
```

### Verificación

```bash
python -c "
from src.logger_config import setup_logger, registrar_metrica, METRICS_FILE
import json

log = setup_logger()
log.info('Test de logging exitoso')
registrar_metrica('test', 100, 1.234, 'OK')

data = json.loads(METRICS_FILE.read_text())
assert len(data) >= 1
assert data[-1]['etapa'] == 'test'
print('✓ Logger y métricas funcionando correctamente')
"
```

---

## Paso 2: Refactorizar el Pipeline con Logging y Optimización

**Objetivo:** Reescribir `etl_pipeline.py` incorporando logging, chunking para lectura de PostgreSQL y paralelización en la transformación.

### Instrucciones

1. Crea el pipeline optimizado en `src/etl_pipeline_v2.py`:

```python
"""
Pipeline ETL optimizado con logging estructurado, chunking y paralelización.
Versión 2.0 - Práctica Final Integradora.
"""
import os
import time
import hashlib
import pandas as pd
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from loguru import logger

# Importar módulos propios
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from src.logger_config import setup_logger, monitorear_tiempo, registrar_metrica
from src.alertas import enviar_alerta_fallo

# Cargar variables de entorno
load_dotenv(Path(__file__).parent.parent / ".env")

# Configuración de base de datos
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5433")
DB_NAME = os.getenv("DB_NAME", "etl_db")
DB_USER = os.getenv("DB_USER", "etl_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "Etl$ecure2024!")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Constantes
CHUNK_SIZE = 10_000
MAX_WORKERS = 4
DATA_DIR = Path(__file__).parent.parent / "data"
PROCESSED_DIR = DATA_DIR / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


@monitorear_tiempo("extract")
def extract_chunked() -> pd.DataFrame:
    """
    Extrae datos de PostgreSQL en chunks de 10,000 registros.
    Optimiza el uso de memoria para datasets grandes.

    Returns:
        DataFrame completo con todos los registros extraídos
    """
    engine = create_engine(DATABASE_URL)
    logger.debug(f"Conectando a {DB_HOST}:{DB_PORT}/{DB_NAME}")

    chunks = []
    query = "SELECT * FROM taxi_trips_clean"

    with engine.connect() as conn:
        for i, chunk in enumerate(pd.read_sql(query, conn, chunksize=CHUNK_SIZE)):
            chunks.append(chunk)
            logger.debug(f"Chunk {i+1} leído: {len(chunk)} registros")

    df = pd.concat(chunks, ignore_index=True)
    logger.info(f"Extracción total: {len(df)} registros en {len(chunks)} chunks")
    return df


def _transformar_chunk(chunk: pd.DataFrame, chunk_id: int) -> pd.DataFrame:
    """
    Transforma un chunk individual de datos.
    Diseñada para ejecución paralela con ThreadPoolExecutor.

    Args:
        chunk: Fragmento del DataFrame a transformar
        chunk_id: Identificador del chunk para logging

    Returns:
        DataFrame transformado
    """
    logger.debug(f"Transformando chunk {chunk_id} ({len(chunk)} registros)")

    # Eliminar filas con valores nulos en columnas críticas
    columnas_criticas = [c for c in ['fare_amount', 'trip_distance', 'pickup_datetime']
                         if c in chunk.columns]
    chunk_clean = chunk.dropna(subset=columnas_criticas)

    # Filtrar fare_amount en rango válido
    if 'fare_amount' in chunk_clean.columns:
        chunk_clean = chunk_clean[
            (chunk_clean['fare_amount'] >= 0) &
            (chunk_clean['fare_amount'] <= 500)
        ]

    # Agregar columna de fecha procesamiento
    chunk_clean = chunk_clean.copy()
    chunk_clean['processed_at'] = pd.Timestamp.now()

    return chunk_clean


@monitorear_tiempo("transform")
def transform_parallel(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica transformaciones en paralelo usando ThreadPoolExecutor.
    Divide el DataFrame en chunks y procesa con 4 workers.

    Args:
        df: DataFrame completo extraído

    Returns:
        DataFrame transformado y limpio
    """
    # Dividir en chunks para procesamiento paralelo
    n_chunks = max(1, len(df) // CHUNK_SIZE)
    chunks = [chunk for chunk in [df.iloc[i:i+CHUNK_SIZE]
              for i in range(0, len(df), CHUNK_SIZE)]]

    logger.info(f"Transformación paralela: {len(chunks)} chunks con {MAX_WORKERS} workers")

    resultados = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(_transformar_chunk, chunk, i): i
            for i, chunk in enumerate(chunks)
        }
        for future in as_completed(futures):
            chunk_id = futures[future]
            try:
                resultado = future.result()
                resultados.append(resultado)
            except Exception as e:
                logger.error(f"Error en chunk {chunk_id}: {e}")
                enviar_alerta_fallo("transform", str(e))

    df_transformado = pd.concat(resultados, ignore_index=True)

    # Métricas de calidad
    registros_perdidos = len(df) - len(df_transformado)
    retencion = (len(df_transformado) / len(df) * 100) if len(df) > 0 else 0
    logger.info(
        f"Transformación completa: {len(df_transformado)} registros "
        f"(retención: {retencion:.1f}%, perdidos: {registros_perdidos})"
    )

    if retencion < 90:
        logger.warning(f"[ALERTA] Retención por debajo del 90%: {retencion:.1f}%")
        enviar_alerta_fallo("transform", f"Retención baja: {retencion:.1f}%")

    return df_transformado


@monitorear_tiempo("load")
def load_to_postgres(df: pd.DataFrame, tabla: str = "taxi_trips_final") -> int:
    """
    Carga el DataFrame transformado a PostgreSQL.

    Args:
        df: DataFrame a cargar
        tabla: Nombre de la tabla destino

    Returns:
        Cantidad de registros cargados
    """
    engine = create_engine(DATABASE_URL)

    df.to_sql(tabla, engine, if_exists='replace', index=False, chunksize=5000)
    logger.info(f"Cargados {len(df)} registros en tabla '{tabla}'")

    return len(df)


def calcular_md5(filepath: Path) -> str:
    """Calcula el hash MD5 de un archivo para verificación de integridad."""
    hash_md5 = hashlib.md5()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


@monitorear_tiempo("export_csv")
def export_to_csv(df: pd.DataFrame) -> Path:
    """
    Exporta el DataFrame a CSV con verificación de integridad MD5.

    Returns:
        Path del archivo exportado
    """
    output_path = PROCESSED_DIR / "taxi_trips_final.csv"
    df.to_csv(output_path, index=False)

    md5 = calcular_md5(output_path)
    logger.info(f"CSV exportado: {output_path} | MD5: {md5}")

    # Guardar MD5 para verificación posterior
    md5_path = output_path.with_suffix('.md5')
    md5_path.write_text(md5)

    return output_path


def run_pipeline():
    """Ejecuta el pipeline ETL completo con logging y monitoreo."""
    setup_logger()

    logger.info("=" * 60)
    logger.info("INICIO DEL PIPELINE ETL v2.0 - Práctica Final")
    logger.info("=" * 60)

    inicio_total = time.perf_counter()

    try:
        # Fase 1: Extracción
        df_raw = extract_chunked()

        # Fase 2: Transformación paralela
        df_clean = transform_parallel(df_raw)

        # Fase 3: Carga
        load_to_postgres(df_clean)

        # Fase 4: Exportación
        export_to_csv(df_clean)

        duracion_total = time.perf_counter() - inicio_total
        logger.info(f"Pipeline completado exitosamente en {duracion_total:.2f}s")
        registrar_metrica("pipeline_total", len(df_clean), duracion_total, "OK")

    except Exception as e:
        duracion_total = time.perf_counter() - inicio_total
        logger.critical(f"Pipeline falló después de {duracion_total:.2f}s: {e}")
        registrar_metrica("pipeline_total", 0, duracion_total, "ERROR", str(e))
        enviar_alerta_fallo("pipeline_total", str(e))
        raise


if __name__ == "__main__":
    run_pipeline()
```

2. Crea el índice en PostgreSQL para optimizar consultas:

```bash
python -c "
from sqlalchemy import create_engine, text

engine = create_engine('postgresql://etl_user:Etl\$ecure2024!@localhost:5433/etl_db')
with engine.connect() as conn:
    # Crear índice si no existe
    conn.execute(text('''
        CREATE INDEX IF NOT EXISTS idx_taxi_pickup_datetime
        ON taxi_trips_clean (pickup_datetime)
    '''))
    conn.commit()
    print('✓ Índice idx_taxi_pickup_datetime creado')
"
```

3. Ejecuta el pipeline y verifica:

```bash
cd ~/etl_course/labs/lab01
python src/etl_pipeline_v2.py
```

### Salida Esperada

```
2024-XX-XX HH:MM:SS | INFO     | [INICIO] extract - Función: extract_chunked
2024-XX-XX HH:MM:SS | INFO     | Extracción total: XXXXX registros en X chunks
2024-XX-XX HH:MM:SS | INFO     | [FIN] extract | Duración: X.XXXs | Registros: XXXXX | Estado: OK
2024-XX-XX HH:MM:SS | INFO     | [INICIO] transform - Función: transform_parallel
2024-XX-XX HH:MM:SS | INFO     | Transformación paralela: X chunks con 4 workers
2024-XX-XX HH:MM:SS | INFO     | Transformación completa: XXXXX registros (retención: XX.X%)
2024-XX-XX HH:MM:SS | INFO     | [FIN] transform | Duración: X.XXXs | Registros: XXXXX | Estado: OK
2024-XX-XX HH:MM:SS | INFO     | Pipeline completado exitosamente en X.XXs
```

### Verificación

```bash
# Verificar que metrics.json se actualizó
python -c "
import json
from pathlib import Path
metrics = json.loads(Path('logs/metrics.json').read_text())
etapas = [m['etapa'] for m in metrics]
assert 'extract' in etapas, 'Falta métrica de extract'
assert 'transform' in etapas, 'Falta métrica de transform'
assert 'load' in etapas, 'Falta métrica de load'
print(f'✓ {len(metrics)} métricas registradas')
for m in metrics[-4:]:
    print(f\"  {m['etapa']}: {m['duracion_segundos']}s - {m['estado']}\")
"
```

---

## Paso 3: Desarrollar Suite de Pruebas con Pytest

**Objetivo:** Crear pruebas unitarias e integración que validen la corrección del pipeline.

### Instrucciones

1. Crea el archivo de configuración de pytest:

```bash
cat > pytest.ini << 'EOF'
[pytest]
testpaths = tests
python_files = test_*.py
python_functions = test_*
addopts = -v --tb=short
EOF
```

2. Crea la suite de pruebas en `tests/test_pipeline.py`:

```python
"""
Suite de pruebas unitarias e integración para el pipeline ETL v2.0.
Ejecutar con: pytest tests/test_pipeline.py -v
"""
import os
import sys
import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import patch, MagicMock
from sqlalchemy import create_engine, text

# Agregar directorio raíz al path
sys.path.insert(0, str(Path(__file__).parent.parent))
from src.etl_pipeline_v2 import (
    extract_chunked,
    transform_parallel,
    _transformar_chunk,
    load_to_postgres,
    calcular_md5,
    DATABASE_URL,
)


# ─── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture
def sample_dataframe():
    """DataFrame de ejemplo para pruebas unitarias."""
    return pd.DataFrame({
        'pickup_datetime': pd.date_range('2024-01-01', periods=100, freq='h'),
        'fare_amount': [round(x * 2.5, 2) for x in range(100)],
        'trip_distance': [round(x * 0.3, 2) for x in range(100)],
        'passenger_count': [1, 2, 3, 4] * 25,
    })


@pytest.fixture
def sample_dataframe_with_nulls():
    """DataFrame con valores nulos para probar limpieza."""
    df = pd.DataFrame({
        'pickup_datetime': pd.date_range('2024-01-01', periods=50, freq='h'),
        'fare_amount': [10.0] * 45 + [None] * 5,
        'trip_distance': [2.0] * 48 + [None, None],
        'passenger_count': [1] * 50,
    })
    return df


@pytest.fixture
def sample_dataframe_invalid_fares():
    """DataFrame con tarifas fuera de rango."""
    return pd.DataFrame({
        'pickup_datetime': pd.date_range('2024-01-01', periods=10, freq='h'),
        'fare_amount': [-5.0, 0.0, 10.0, 50.0, 100.0, 250.0, 500.0, 501.0, 1000.0, 25.0],
        'trip_distance': [1.0] * 10,
        'passenger_count': [1] * 10,
    })


# ─── Tests de Extracción ──────────────────────────────────────────────────────

class TestExtract:
    """Pruebas para la fase de extracción."""

    def test_extract_returns_dataframe(self):
        """Verifica que extract retorna un DataFrame no vacío."""
        from src.logger_config import setup_logger
        setup_logger()
        df = extract_chunked()
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0, "El DataFrame extraído no debe estar vacío"

    def test_extract_has_expected_columns(self):
        """Verifica que el DataFrame tiene las columnas esperadas."""
        from src.logger_config import setup_logger
        setup_logger()
        df = extract_chunked()
        columnas_esperadas = ['fare_amount', 'trip_distance']
        for col in columnas_esperadas:
            assert col in df.columns, f"Columna '{col}' no encontrada"


# ─── Tests de Transformación ──────────────────────────────────────────────────

class TestTransform:
    """Pruebas para la fase de transformación."""

    def test_transform_no_nulls(self, sample_dataframe_with_nulls):
        """Verifica que transform elimina nulos en columnas críticas."""
        from src.logger_config import setup_logger
        setup_logger()
        resultado = transform_parallel(sample_dataframe_with_nulls)
        # Verificar que no hay nulos en columnas críticas
        for col in ['fare_amount', 'trip_distance']:
            if col in resultado.columns:
                assert resultado[col].isnull().sum() == 0, \
                    f"Columna '{col}' aún tiene valores nulos"

    def test_transform_fare_in_range(self, sample_dataframe_invalid_fares):
        """Verifica que fare_amount queda entre 0 y 500 después de transform."""
        from src.logger_config import setup_logger
        setup_logger()
        resultado = transform_parallel(sample_dataframe_invalid_fares)
        assert resultado['fare_amount'].min() >= 0
        assert resultado['fare_amount'].max() <= 500

    def test_transform_adds_processed_at(self, sample_dataframe):
        """Verifica que se agrega la columna processed_at."""
        from src.logger_config import setup_logger
        setup_logger()
        resultado = transform_parallel(sample_dataframe)
        assert 'processed_at' in resultado.columns

    def test_transformar_chunk_preserves_valid_data(self, sample_dataframe):
        """Verifica que un chunk válido no pierde registros."""
        resultado = _transformar_chunk(sample_dataframe, 0)
        # Todos los registros son válidos, no debería perder ninguno
        assert len(resultado) == len(sample_dataframe)


# ─── Tests de Carga ───────────────────────────────────────────────────────────

class TestLoad:
    """Pruebas de integración para la fase de carga."""

    def test_load_row_count(self, sample_dataframe):
        """Verifica que la cantidad de filas cargadas coincide con el DataFrame."""
        from src.logger_config import setup_logger
        setup_logger()
        tabla_test = "test_pipeline_load"
        registros = load_to_postgres(sample_dataframe, tabla=tabla_test)
        assert registros == len(sample_dataframe)

        # Verificar en BD
        engine = create_engine(DATABASE_URL)
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {tabla_test}"))
            count = result.scalar()
        assert count == len(sample_dataframe)

        # Limpieza
        with engine.connect() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {tabla_test}"))
            conn.commit()


# ─── Tests de Esquema ─────────────────────────────────────────────────────────

class TestSchema:
    """Pruebas de validación de esquema."""

    def test_schema_validation(self):
        """Verifica que la tabla en BD tiene el esquema esperado."""
        engine = create_engine(DATABASE_URL)
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = 'taxi_trips_clean'
                ORDER BY ordinal_position
            """))
            columns = {row[0]: row[1] for row in result}

        assert 'fare_amount' in columns, "Columna fare_amount no existe"
        assert 'trip_distance' in columns, "Columna trip_distance no existe"
        print(f"Esquema validado: {len(columns)} columnas encontradas")
```

3. Ejecuta las pruebas:

```bash
cd ~/etl_course/labs/lab01
pytest tests/test_pipeline.py -v
```

### Salida Esperada

```
tests/test_pipeline.py::TestExtract::test_extract_returns_dataframe PASSED
tests/test_pipeline.py::TestExtract::test_extract_has_expected_columns PASSED
tests/test_pipeline.py::TestTransform::test_transform_no_nulls PASSED
tests/test_pipeline.py::TestTransform::test_transform_fare_in_range PASSED
tests/test_pipeline.py::TestTransform::test_transform_adds_processed_at PASSED
tests/test_pipeline.py::TestTransform::test_transformar_chunk_preserves_valid_data PASSED
tests/test_pipeline.py::TestLoad::test_load_row_count PASSED
tests/test_pipeline.py::TestSchema::test_schema_validation PASSED

========================= 8 passed in X.XXs =========================
```

### Verificación

```bash
pytest tests/test_pipeline.py --tb=short -q
# Debe mostrar: 8 passed
```

---

## Paso 4: Configurar Great Expectations para Validación de Datos

**Objetivo:** Crear expectations para validar la calidad de los datos del pipeline y generar reportes HTML.

### Instrucciones

1. Inicializa el proyecto de Great Expectations:

```bash
cd ~/etl_course/labs/lab01
```

2. Crea el script de configuración y validación en `tests/test_great_expectations.py`:

```python
"""
Validación de calidad de datos con Great Expectations.
Ejecutar con: python tests/test_great_expectations.py
"""
import sys
from pathlib import Path
import pandas as pd
import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest

sys.path.insert(0, str(Path(__file__).parent.parent))

# Directorios
GE_DIR = Path(__file__).parent.parent / "great_expectations"
GE_DIR.mkdir(exist_ok=True)
DATA_DIR = Path(__file__).parent.parent / "data" / "processed"


def crear_expectation_suite():
    """
    Crea y ejecuta una suite de expectations para nyc_taxi_clean.
    Valida columnas críticas, rangos de valores y conteos de filas.
    """
    # Cargar datos
    csv_path = DATA_DIR / "nyc_taxi_clean.csv"
    if not csv_path.exists():
        # Fallback: leer de la tabla PostgreSQL
        from sqlalchemy import create_engine
        engine = create_engine(
            "postgresql://etl_user:Etl$ecure2024!@localhost:5433/etl_db"
        )
        df = pd.read_sql("SELECT * FROM taxi_trips_clean", engine)
    else:
        df = pd.read_csv(csv_path)

    print(f"Dataset cargado: {len(df)} registros, {len(df.columns)} columnas")

    # Crear contexto de Great Expectations
    context = gx.get_context()

    # Crear datasource en memoria
    datasource = context.sources.add_or_update_pandas(name="taxi_datasource")
    data_asset = datasource.add_dataframe_asset(name="taxi_clean")

    batch_request = data_asset.build_batch_request(dataframe=df)

    # Crear suite de expectations
    suite_name = "nyc_taxi_clean_suite"

    # Obtener validator
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite_name,
        create_expectation_suite_with_name=suite_name,
    )

    # ─── Definir Expectations ─────────────────────────────────────────────

    # 1. Columnas críticas no deben tener nulos
    columnas_no_null = ['fare_amount', 'trip_distance', 'pickup_datetime']
    for col in columnas_no_null:
        if col in df.columns:
            validator.expect_column_values_to_not_be_null(column=col)
            print(f"  ✓ expect_column_values_to_not_be_null: {col}")

    # 2. fare_amount debe estar entre 0 y 500
    if 'fare_amount' in df.columns:
        validator.expect_column_values_to_be_between(
            column="fare_amount",
            min_value=0,
            max_value=500,
        )
        print("  ✓ expect_column_values_to_be_between: fare_amount [0, 500]")

    # 3. trip_distance debe ser positiva
    if 'trip_distance' in df.columns:
        validator.expect_column_values_to_be_between(
            column="trip_distance",
            min_value=0,
            max_value=200,
        )
        print("  ✓ expect_column_values_to_be_between: trip_distance [0, 200]")

    # 4. Conteo de filas en rango esperado
    validator.expect_table_row_count_to_be_between(
        min_value=100,
        max_value=10_000_000,
    )
    print("  ✓ expect_table_row_count_to_be_between: [100, 10M]")

    # 5. Columnas esperadas deben existir
    for col in ['fare_amount', 'trip_distance']:
        if col in df.columns:
            validator.expect_column_to_exist(column=col)

    # Guardar suite
    validator.save_expectation_suite(discard_failed_expectations=False)

    # ─── Ejecutar Validación (Checkpoint) ─────────────────────────────────

    checkpoint = context.add_or_update_checkpoint(
        name="taxi_checkpoint",
        validator=validator,
    )

    result = checkpoint.run()

    # ─── Generar Reporte ──────────────────────────────────────────────────

    # Imprimir resultados
    success = result.success
    stats = result.run_results
    print(f"\n{'=' * 50}")
    print(f"RESULTADO DE VALIDACIÓN: {'PASSED ✓' if success else 'FAILED ✗'}")
    print(f"{'=' * 50}")

    # Guardar resultado resumido
    resultado_path = GE_DIR / "validation_result.txt"
    with open(resultado_path, 'w') as f:
        f.write(f"Validación: {'PASSED' if success else 'FAILED'}\n")
        f.write(f"Registros validados: {len(df)}\n")
        f.write(f"Suite: {suite_name}\n")

    print(f"Resultado guardado en: {resultado_path}")

    # Construir Data Docs (reporte HTML)
    context.build_data_docs()
    print("Reporte HTML generado en: great_expectations/uncommitted/data_docs/")

    return success


if __name__ == "__main__":
    success = crear_expectation_suite()
    sys.exit(0 if success else 1)
```

3. Ejecuta la validación:

```bash
python tests/test_great_expectations.py
```

### Salida Esperada

```
Dataset cargado: XXXXX registros, XX columnas
  ✓ expect_column_values_to_not_be_null: fare_amount
  ✓ expect_column_values_to_not_be_null: trip_distance
  ✓ expect_column_values_to_not_be_null: pickup_datetime
  ✓ expect_column_values_to_be_between: fare_amount [0, 500]
  ✓ expect_column_values_to_be_between: trip_distance [0, 200]
  ✓ expect_table_row_count_to_be_between: [100, 10M]
==================================================
RESULTADO DE VALIDACIÓN: PASSED ✓
==================================================
```

### Verificación

```bash
# Verificar que el archivo de resultado existe
cat great_expectations/validation_result.txt
# Debe mostrar: Validación: PASSED
```

---

## Paso 5: Documentación y Control de Versiones con Git

**Objetivo:** Inicializar repositorio Git, crear documentación profesional y realizar commits semánticos.

### Instrucciones

1. Inicializa el repositorio Git:

```bash
cd ~/etl_course/labs/lab01

# Inicializar repositorio
git init

# Configurar usuario (si no está configurado)
git config user.name "ETL Developer"
git config user.email "dev@etl-pipeline.local"
```

2. Crea el archivo `.gitignore`:

```bash
cat > .gitignore << 'EOF'
# Entorno virtual
venv_etl/

# Variables de entorno
.env

# Cache de Python
__pycache__/
*.pyc
*.pyo
*.pyd
.Python

# Datos crudos y procesados (demasiado grandes para Git)
data/raw/*.csv
data/raw/*.json
data/processed/
data/raw/batches/

# Logs
logs/*.log
logs/*.log.*

# Great Expectations artifacts
great_expectations/uncommitted/

# IDE
.vscode/
.idea/
*.swp

# OS
.DS_Store
Thumbs.db
EOF
```

3. Crea el archivo `requirements.txt`:

```bash
cat > requirements.txt << 'EOF'
pandas==2.2.0
numpy==1.26.4
sqlalchemy==2.0.25
psycopg2-binary==2.9.9
python-dotenv==1.0.1
loguru==0.7.2
pytest==8.0.0
great-expectations==0.18.9
boto3==1.34.34
apache-airflow==2.8.1
openpyxl==3.1.2
pyarrow==15.0.2
lxml==5.1.0
beautifulsoup4==4.12.3
requests==2.31.0
EOF
```

4. Crea el archivo `README.md`:

```bash
cat > README.md << 'EOF'
# Pipeline ETL - NYC Taxi Data

## Arquitectura

```
┌─────────────────────────────────────────────────────────────────┐
│                    PIPELINE ETL v2.0                             │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────┐    ┌─────────────┐    ┌──────────┐    ┌───────┐ │
│  │ EXTRACT  │───▶│  TRANSFORM  │───▶│ VALIDATE │───▶│ LOAD  │ │
│  │(chunked) │    │ (parallel)  │    │   (GE)   │    │       │ │
│  └──────────┘    └─────────────┘    └──────────┘    └───────┘ │
│       │                │                  │              │      │
│       ▼                ▼                  ▼              ▼      │
│  PostgreSQL      concurrent.futures   Great Exp.    PostgreSQL  │
│  (chunks 10K)    (4 workers)         (HTML report)  + S3       │
│                                                                 │
├─────────────────────────────────────────────────────────────────┤
│  MONITOREO: loguru → logs/ + metrics.json + alertas email      │
│  ORQUESTACIÓN: Apache Airflow (DAG: nyc_taxi_etl_dag)          │
└─────────────────────────────────────────────────────────────────┘
```

## Estructura del Proyecto

```
~/etl_course/labs/lab01/
├── src/
│   ├── logger_config.py      # Configuración de loguru y métricas
│   ├── alertas.py            # Sistema de alertas por email
│   └── etl_pipeline_v2.py    # Pipeline optimizado
├── tests/
│   ├── test_pipeline.py      # Pruebas unitarias e integración
│   └── test_great_expectations.py  # Validación de calidad
├── data/
│   ├── raw/                  # Datos sin procesar
│   └── processed/            # Datos limpios y finales
├── logs/
│   ├── etl_pipeline.log      # Log rotativo
│   └── metrics.json          # Métricas de ejecución
├── great_expectations/       # Suite de validación GE
├── config/
│   └── docker-compose.yml    # PostgreSQL + LocalStack
├── .env                      # Variables de entorno (NO commiteado)
├── .env.example              # Plantilla de variables
├── .gitignore
├── requirements.txt
├── pytest.ini
└── README.md
```

## Instalación

```bash
# 1. Clonar repositorio
git clone <url> && cd etl_course/labs/lab01

# 2. Crear entorno virtual
python -m virtualenv venv_etl
source venv_etl/bin/activate

# 3. Instalar dependencias
pip install -r requirements.txt

# 4. Configurar variables de entorno
cp .env.example .env
# Editar .env con credenciales reales

# 5. Levantar servicios
docker compose -f config/docker-compose.yml up -d
```

## Ejecución

```bash
# Ejecutar pipeline completo
python src/etl_pipeline_v2.py

# Ejecutar pruebas
pytest tests/ -v

# Validar calidad de datos
python tests/test_great_expectations.py

# Ver métricas
cat logs/metrics.json | python -m json.tool
```

## Tecnologías

| Componente | Tecnología | Versión |
|-----------|-----------|---------|
| Lenguaje | Python | 3.12.1 |
| BD | PostgreSQL | 16.2 |
| Orquestación | Apache Airflow | 2.8.1 |
| Logging | loguru | 0.7.2 |
| Testing | pytest | 8.0.0 |
| Validación | Great Expectations | 0.18.9 |
| Cloud (local) | LocalStack | 3.1.0 |
| Almacenamiento | AWS S3 (via boto3) | 1.34.34 |

## Licencia

Proyecto educativo - Curso ETL
EOF
```

5. Realiza los commits semánticos:

```bash
# Primer commit: estructura base
git add .gitignore requirements.txt pytest.ini README.md
git commit -m "docs: agregar documentación inicial, .gitignore y requirements.txt"

# Segundo commit: logging
git add src/logger_config.py src/alertas.py
git commit -m "feat: implementar sistema de logging con loguru y alertas"

# Tercer commit: pipeline optimizado
git add src/etl_pipeline_v2.py
git commit -m "feat: refactorizar pipeline con chunking y paralelización"

# Cuarto commit: pruebas
git add tests/
git commit -m "test: agregar suite de pruebas pytest y validación Great Expectations"

# Quinto commit: métricas (solo el archivo de ejemplo)
git add logs/metrics.json
git commit -m "feat: agregar registro de métricas de ejecución en JSON"
```

### Salida Esperada

```
[main (root-commit) abc1234] docs: agregar documentación inicial, .gitignore y requirements.txt
 4 files changed, XX insertions(+)
[main def5678] feat: implementar sistema de logging con loguru y alertas
 2 files changed, XX insertions(+)
[main ghi9012] feat: refactorizar pipeline con chunking y paralelización
 1 file changed, XX insertions(+)
[main jkl3456] test: agregar suite de pruebas pytest y validación Great Expectations
 2 files changed, XX insertions(+)
[main mno7890] feat: agregar registro de métricas de ejecución en JSON
 1 file changed, XX insertions(+)
```

### Verificación

```bash
# Ver historial de commits
git log --oneline
# Debe mostrar 5 commits con prefijos semánticos (docs:, feat:, test:)

# Verificar que .env NO está trackeado
git status
# .env no debe aparecer en la lista
```

---

## Paso 6: Medir y Comparar Rendimiento

**Objetivo:** Ejecutar benchmarks para demostrar la mejora de rendimiento con las optimizaciones aplicadas.

### Instrucciones

1. Crea el script de benchmark en `tests/benchmark_pipeline.py`:

```python
"""
Benchmark para comparar rendimiento antes/después de optimizaciones.
Mide: lectura secuencial vs chunked, transformación serial vs paralela.
"""
import time
import sys
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine

sys.path.insert(0, str(Path(__file__).parent.parent))
from src.logger_config import setup_logger
from src.etl_pipeline_v2 import (
    extract_chunked,
    transform_parallel,
    _transformar_chunk,
    DATABASE_URL,
    CHUNK_SIZE,
)

setup_logger()


def benchmark_extract_sequential():
    """Extracción secuencial sin chunking (método original)."""
    engine = create_engine(DATABASE_URL)
    inicio = time.perf_counter()
    with engine.connect() as conn:
        df = pd.read_sql("SELECT * FROM taxi_trips_clean", conn)
    duracion = time.perf_counter() - inicio
    return duracion, len(df)


def benchmark_extract_chunked():
    """Extracción con chunking (método optimizado)."""
    engine = create_engine(DATABASE_URL)
    inicio = time.perf_counter()
    chunks = []
    with engine.connect() as conn:
        for chunk in pd.read_sql("SELECT * FROM taxi_trips_clean", conn,
                                  chunksize=CHUNK_SIZE):
            chunks.append(chunk)
    df = pd.concat(chunks, ignore_index=True)
    duracion = time.perf_counter() - inicio
    return duracion, len(df)


def benchmark_transform_serial(df):
    """Transformación serial (método original)."""
    inicio = time.perf_counter()
    resultado = _transformar_chunk(df, 0)
    duracion = time.perf_counter() - inicio
    return duracion, len(resultado)


def benchmark_transform_parallel(df):
    """Transformación paralela (método optimizado)."""
    inicio = time.perf_counter()
    resultado = transform_parallel(df)
    duracion = time.perf_counter() - inicio
    return duracion, len(resultado)


if __name__ == "__main__":
    print("=" * 60)
    print("BENCHMARK DE RENDIMIENTO - Pipeline ETL v2.0")
    print("=" * 60)

    # Benchmark de extracción
    print("\n─── EXTRACCIÓN ───")
    t_seq, n_seq = benchmark_extract_sequential()
    print(f"  Secuencial:  {t_seq:.3f}s ({n_seq} registros)")

    t_chunk, n_chunk = benchmark_extract_chunked()
    print(f"  Chunked:     {t_chunk:.3f}s ({n_chunk} registros)")

    mejora_ext = ((t_seq - t_chunk) / t_seq * 100) if t_seq > 0 else 0
    print(f"  Diferencia:  {mejora_ext:+.1f}%")

    # Benchmark de transformación
    print("\n─── TRANSFORMACIÓN ───")
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        df = pd.read_sql("SELECT * FROM taxi_trips_clean", conn)

    t_serial, n_serial = benchmark_transform_serial(df)
    print(f"  Serial:      {t_serial:.3f}s ({n_serial} registros)")

    t_parallel, n_parallel = benchmark_transform_parallel(df)
    print(f"  Paralela:    {t_parallel:.3f}s ({n_parallel} registros)")

    mejora_trans = ((t_serial - t_parallel) / t_serial * 100) if t_serial > 0 else 0
    print(f"  Diferencia:  {mejora_trans:+.1f}%")

    print("\n" + "=" * 60)
    print("RESUMEN")
    print(f"  Registros procesados: {n_chunk}")
    print(f"  Extracción: {'chunked más rápido' if mejora_ext > 0 else 'secuencial más rápido'}")
    print(f"  Transformación: {'paralela más rápida' if mejora_trans > 0 else 'serial más rápida'}")
    print("=" * 60)
```

2. Ejecuta el benchmark:

```bash
python tests/benchmark_pipeline.py
```

### Salida Esperada

```
============================================================
BENCHMARK DE RENDIMIENTO - Pipeline ETL v2.0
============================================================

─── EXTRACCIÓN ───
  Secuencial:  X.XXXs (XXXXX registros)
  Chunked:     X.XXXs (XXXXX registros)
  Diferencia:  +X.X%

─── TRANSFORMACIÓN ───
  Serial:      X.XXXs (XXXXX registros)
  Paralela:    X.XXXs (XXXXX registros)
  Diferencia:  +X.X%

============================================================
RESUMEN
  Registros procesados: XXXXX
  Extracción: chunked más rápido
  Transformación: paralela más rápida
============================================================
```

### Verificación

```bash
# Commit del benchmark
git add tests/benchmark_pipeline.py
git commit -m "test: agregar benchmark de rendimiento para comparar optimizaciones"
```

---

## Paso 7: Integración Final - Ejecución Completa

**Objetivo:** Ejecutar el pipeline completo y verificar que todos los componentes funcionan de forma integrada.

### Instrucciones

1. Ejecuta el pipeline completo:

```bash
cd ~/etl_course/labs/lab01
python src/etl_pipeline_v2.py
```

2. Ejecuta la suite completa de pruebas:

```bash
pytest tests/test_pipeline.py -v --tb=short
```

3. Ejecuta la validación de Great Expectations:

```bash
python tests/test_great_expectations.py
```

4. Verifica las métricas finales:

```bash
python -c "
import json
from pathlib import Path

metrics = json.loads(Path('logs/metrics.json').read_text())
print('═' * 60)
print('MÉTRICAS FINALES DEL PIPELINE')
print('═' * 60)
for m in metrics:
    estado_icon = '✓' if m['estado'] == 'OK' else '✗'
    print(f\"  [{estado_icon}] {m['etapa']:20s} | {m['duracion_segundos']:8.3f}s | {m['registros_procesados']:>8} reg\")
print('═' * 60)

# Resumen
total_ok = sum(1 for m in metrics if m['estado'] == 'OK')
total_err = sum(1 for m in metrics if m['estado'] == 'ERROR')
print(f'  Total exitosos: {total_ok}')
print(f'  Total errores:  {total_err}')
"
```

5. Verifica el log generado:

```bash
# Últimas 20 líneas del log
tail -20 logs/etl_pipeline.log
```

6. Realiza el commit final:

```bash
git add -A
git commit -m "feat: integración final del pipeline ETL v2.0 con logging, testing y optimización"

# Ver resumen del repositorio
git log --oneline
echo ""
echo "Total de archivos trackeados:"
git ls-files | wc -l
```

### Salida Esperada

```
═══════════════════════════════════════════════════════════════
MÉTRICAS FINALES DEL PIPELINE
═══════════════════════════════════════════════════════════════
  [✓] extract              |    X.XXXs |    XXXXX reg
  [✓] transform            |    X.XXXs |    XXXXX reg
  [✓] load                 |    X.XXXs |    XXXXX reg
  [✓] export_csv           |    X.XXXs |    XXXXX reg
  [✓] pipeline_total       |    X.XXXs |    XXXXX reg
═══════════════════════════════════════════════════════════════
  Total exitosos: 5
  Total errores:  0
```

### Verificación

```bash
# Verificación final integral
python -c "
from pathlib import Path
import json

checks = []

# 1. Pipeline log existe y tiene contenido
log_file = Path('logs/etl_pipeline.log')
checks.append(('Log file exists', log_file.exists() and log_file.stat().st_size > 0))

# 2. Metrics JSON tiene entradas
metrics = json.loads(Path('logs/metrics.json').read_text())
checks.append(('Metrics registered', len(metrics) >= 4))

# 3. CSV final generado
csv_file = Path('data/processed/taxi_trips_final.csv')
checks.append(('CSV output exists', csv_file.exists()))

# 4. MD5 checksum exists
md5_file = Path('data/processed/taxi_trips_final.md5')
checks.append(('MD5 checksum exists', md5_file.exists()))

# 5. GE validation result
ge_result = Path('great_expectations/validation_result.txt')
checks.append(('GE validation done', ge_result.exists()))

# 6. Git has commits
import subprocess
result = subprocess.run(['git', 'log', '--oneline'], capture_output=True, text=True)
commits = len(result.stdout.strip().split('\n'))
checks.append(('Git commits >= 5', commits >= 5))

print('VERIFICACIÓN FINAL')
print('─' * 40)
all_pass = True
for name, passed in checks:
    icon = '✓' if passed else '✗'
    print(f'  [{icon}] {name}')
    if not passed:
        all_pass = False

print('─' * 40)
print(f\"{'TODOS LOS CHECKS PASARON ✓' if all_pass else 'ALGUNOS CHECKS FALLARON ✗'}\")
"
```

---

## Validación y Testing

Ejecuta esta verificación completa para confirmar que todos los objetivos del laboratorio se cumplieron:

```bash
#!/bin/bash
echo "══════════════════════════════════════════"
echo "  VALIDACIÓN COMPLETA - Lab 08-00-01"
echo "══════════════════════════════════════════"

# 1. Pytest
echo -e "\n[1/4] Ejecutando pytest..."
pytest tests/test_pipeline.py -q --no-header 2>&1 | tail -3

# 2. Great Expectations
echo -e "\n[2/4] Ejecutando validación GE..."
python tests/test_great_expectations.py 2>&1 | grep "RESULTADO"

# 3. Git log
echo -e "\n[3/4] Historial de commits:"
git log --oneline | head -6

# 4. Métricas
echo -e "\n[4/4] Métricas del pipeline:"
python -c "
import json
m = json.loads(open('logs/metrics.json').read())
ok = sum(1 for x in m if x['estado']=='OK')
print(f'  Ejecuciones exitosas: {ok}/{len(m)}')
"

echo -e "\n══════════════════════════════════════════"
echo "  LABORATORIO COMPLETADO"
echo "══════════════════════════════════════════"
```

---

## Solución de Problemas

### Problema 1: `ModuleNotFoundError: No module named 'src'`

**Síntomas:** Al ejecutar pytest o el pipeline, Python no encuentra los módulos en `src/`.

**Causa:** El directorio raíz del proyecto no está en el `sys.path` de Python, y no existe un `__init__.py` en `src/`.

**Solución:**

```bash
# Opción 1: Crear __init__.py
touch src/__init__.py

# Opción 2: Instalar el proyecto en modo editable (crear setup.py mínimo)
cat > setup.py << 'EOF'
from setuptools import setup, find_packages
setup(name="etl_pipeline", version="2.0", packages=find_packages())
EOF
pip install -e .

# Opción 3: Ejecutar desde el directorio correcto con PYTHONPATH
PYTHONPATH=~/etl_course/labs/lab01 pytest tests/ -v
```

### Problema 2: `OperationalError: connection refused` al conectar a PostgreSQL

**Síntomas:** El pipeline falla en la fase de extracción con error de conexión a la base de datos en puerto 5433.

**Causa:** El contenedor Docker `postgres_etl` no está corriendo o el puerto no está mapeado correctamente.

**Solución:**

```bash
# Verificar que el contenedor está corriendo
docker ps | grep postgres_etl

# Si no está corriendo, levantarlo
docker compose -f config/docker-compose.yml up -d

# Verificar conectividad
python -c "
from sqlalchemy import create_engine, text
engine = create_engine('postgresql://etl_user:Etl\$ecure2024!@localhost:5433/etl_db')
with engine.connect() as conn:
    result = conn.execute(text('SELECT 1'))
    print(f'Conexión OK: {result.scalar()}')
"

# Si el puerto está ocupado, verificar qué proceso lo usa
lsof -i :5433
```

---

## Limpieza

```bash
# Eliminar tabla de pruebas creada durante testing
python -c "
from sqlalchemy import create_engine, text
engine = create_engine('postgresql://etl_user:Etl\$ecure2024!@localhost:5433/etl_db')
with engine.connect() as conn:
    conn.execute(text('DROP TABLE IF EXISTS test_pipeline_load'))
    conn.execute(text('DROP TABLE IF EXISTS taxi_trips_final'))
    conn.commit()
print('Tablas de prueba eliminadas')
"

# Limpiar logs antiguos (opcional, mantener para referencia)
# rm -f logs/*.log.zip

# El entorno virtual y los datos procesados se mantienen para uso futuro
```

---

## Resumen

En este laboratorio final has construido un pipeline ETL de calidad de producción que integra:

| Componente | Implementación |
|-----------|---------------|
| **Logging** | loguru con rotación 10 MB, retención 7 días, métricas JSON |
| **Monitoreo** | Decoradores de timing, registro de métricas, alertas simuladas |
| **Testing** | 8 pruebas con pytest (unitarias + integración) |
| **Validación** | Great Expectations con 6 expectations y reporte HTML |
| **Optimización** | Chunking (10K), paralelización (4 workers), índice PostgreSQL |
| **Documentación** | README.md con diagrama ASCII, commits semánticos |
| **Versionado** | Git con 6+ commits siguiendo convenciones semánticas |

### Conceptos Clave Aplicados

- El **logging estructurado** reemplaza `print()` y permite diagnóstico post-mortem profesional
- Los **decoradores de monitoreo** separan la lógica de negocio de la observabilidad
- El **chunking** reduce el consumo de memoria al leer datasets grandes
- La **paralelización con ThreadPoolExecutor** aprovecha múltiples núcleos para transformaciones
- **Great Expectations** automatiza la validación de calidad sin código ad-hoc
- Los **commits semánticos** facilitan la generación de changelogs y la colaboración

### Recursos Adicionales

- [Documentación de loguru](https://loguru.readthedocs.io/)
- [Great Expectations Docs](https://docs.greatexpectations.io/)
- [Conventional Commits](https://www.conventionalcommits.org/)
- [Python concurrent.futures](https://docs.python.org/3/library/concurrent.futures.html)
