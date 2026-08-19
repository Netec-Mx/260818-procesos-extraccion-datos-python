# Extracción desde una base de datos relacional

## Metadata

| Campo | Valor |
|-------|-------|
| **Duración** | 60 minutos |
| **Complejidad** | Media |
| **Nivel Bloom** | Aplicar |
| **Tecnologías** | PostgreSQL 16.2, Docker 26.0.0, Docker Compose 2.26.0, SQLAlchemy 2.0.29, psycopg2-binary 2.9.9, python-dotenv 1.0.1, pandas 2.2.1, pyarrow 15.0.2 |

## Descripción General

En este laboratorio levantarás una instancia de PostgreSQL 16.2 mediante Docker Compose, poblarás la base de datos `etl_db` con los datos del dataset Titanic (generado como Parquet en el Laboratorio 2), y construirás un script Python que establece conexiones seguras con SQLAlchemy, ejecuta consultas parametrizadas y realiza extracción paginada por lotes exportando los resultados a archivos CSV numerados. Este ejercicio consolida los conceptos de cadenas de conexión, pool de conexiones y context managers vistos en la lección 3.1.

## Objetivos de Aprendizaje

- [ ] Levantar PostgreSQL 16.2 en Docker y poblar la tabla `titanic_passengers` con datos importados desde el archivo Parquet del Laboratorio 2.
- [ ] Establecer conexiones seguras con SQLAlchemy 2.0.29 cargando credenciales desde un archivo `.env` con python-dotenv.
- [ ] Ejecutar consultas SQL parametrizadas para extraer subconjuntos filtrados de datos.
- [ ] Implementar extracción por lotes (batch extraction) con LIMIT/OFFSET, exportando cada lote a archivos CSV numerados en `data/raw/batches/`.

## Prerrequisitos

### Conocimientos previos
- SQL básico: `SELECT`, `WHERE`, `LIMIT`, `OFFSET`
- Manejo de variables de entorno y archivos `.env`
- Conceptos de cadenas de conexión y `Engine` de SQLAlchemy (Lección 3.1)
- Uso básico de pandas para lectura de Parquet y manipulación de DataFrames

### Acceso y archivos requeridos
- Laboratorio 2 completado: archivo `~/etl_course/labs/lab01/data/processed/titanic.parquet` existente
- Docker 26.0.0 instalado y servicio Docker en ejecución (`docker --version`)
- Docker Compose 2.26.0 disponible (`docker compose version`)
- Entorno virtual `venv_etl` activo
- Conexión a Internet para descargar la imagen `postgres:16.2`

## Entorno del Laboratorio

### Software requerido

| Componente | Versión | Verificación |
|-----------|---------|--------------|
| Docker | 26.0.0+ | `docker --version` |
| Docker Compose | 2.26.0+ | `docker compose version` |
| Python | 3.12.1 | `python --version` |
| pandas | 2.2.1 | `pip show pandas` |
| pyarrow | 15.0.2 | `pip show pyarrow` |
| SQLAlchemy | 2.0.29 | `pip show sqlalchemy` |
| psycopg2-binary | 2.9.9 | `pip show psycopg2-binary` |
| python-dotenv | 1.0.1 | `pip show python-dotenv` |

### Estructura de directorios utilizada

```
~/etl_course/labs/lab01/
├── .env
├── .env.example
├── config/
│   └── docker-compose.yml
├── data/
│   ├── processed/
│   │   └── titanic.parquet    ← generado en Lab 02
│   └── raw/
│       └── batches/           ← salida de este laboratorio
├── src/
│   └── lab03_db_extraction.py ← script principal
└── venv_etl/
```

## Paso a Paso

---

### Paso 1: Instalar dependencias adicionales en el entorno virtual

**Objetivo:** Agregar `psycopg2-binary` y `python-dotenv` al entorno virtual existente.

**Instrucciones:**

1. Navega al directorio raíz del proyecto y activa el entorno virtual:

```bash
cd ~/etl_course/labs/lab01
source venv_etl/bin/activate   # Linux/macOS
# Windows: .\venv_etl\Scripts\activate
```

2. Instala las dependencias necesarias:

```bash
pip install psycopg2-binary==2.9.9 python-dotenv==1.0.1
```

3. Verifica la instalación:

```bash
pip show psycopg2-binary python-dotenv | grep -E "^(Name|Version)"
```

**Salida esperada:**

```
Name: psycopg2-binary
Version: 2.9.9
Name: python-dotenv
Version: 1.0.1
```

**Verificación:** Ambos paquetes aparecen con las versiones correctas.

---

### Paso 2: Configurar el archivo `.env` con credenciales de PostgreSQL

**Objetivo:** Definir las credenciales de conexión en un archivo `.env` para evitar hardcodear contraseñas en el código fuente.

**Instrucciones:**

1. Crea (o actualiza) el archivo `.env` en el directorio raíz del proyecto:

```bash
cat > ~/etl_course/labs/lab01/.env << 'EOF'
DB_HOST=localhost
DB_PORT=5433
DB_NAME=etl_db
DB_USER=etl_user
DB_PASSWORD=Etl$ecure2024!
DB_SCHEMA=public
EOF
```

2. Crea el archivo `.env.example` como plantilla (sin valores sensibles):

```bash
cat > ~/etl_course/labs/lab01/.env.example << 'EOF'
DB_HOST=
DB_PORT=
DB_NAME=
DB_USER=
DB_PASSWORD=
DB_SCHEMA=
EOF
```

3. Verifica que `.env` está incluido en `.gitignore`:

```bash
grep -q "^\.env$" .gitignore && echo "OK: .env está en .gitignore" || echo ".env" >> .gitignore
```

**Salida esperada:**

```
OK: .env está en .gitignore
```

**Verificación:** El archivo `.env` existe con las 6 variables definidas y no será rastreado por Git.

---

### Paso 3: Crear el archivo `docker-compose.yml` y levantar PostgreSQL

**Objetivo:** Configurar y ejecutar un contenedor PostgreSQL 16.2 accesible en el puerto 5433 del host.

**Instrucciones:**

1. Crea el archivo `docker-compose.yml` en el directorio `config/`:

```bash
cat > ~/etl_course/labs/lab01/config/docker-compose.yml << 'EOF'
version: "3.9"

services:
  postgres_etl:
    image: postgres:16.2
    container_name: postgres_etl
    environment:
      POSTGRES_USER: etl_user
      POSTGRES_PASSWORD: "Etl$$ecure2024!"
      POSTGRES_DB: etl_db
    ports:
      - "5433:5432"
    volumes:
      - pgdata_etl:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U etl_user -d etl_db"]
      interval: 5s
      timeout: 5s
      retries: 5
    restart: unless-stopped

volumes:
  pgdata_etl:
EOF
```

> **Nota:** El doble `$$` en la variable `POSTGRES_PASSWORD` es necesario para escapar el símbolo `$` dentro de Docker Compose.

2. Levanta el contenedor:

```bash
cd ~/etl_course/labs/lab01/config
docker compose up -d
```

3. Espera a que el contenedor esté saludable (máximo 30 segundos):

```bash
echo "Esperando que PostgreSQL esté listo..."
for i in $(seq 1 6); do
  STATUS=$(docker inspect --format='{{.State.Health.Status}}' postgres_etl 2>/dev/null)
  if [ "$STATUS" = "healthy" ]; then
    echo "PostgreSQL está listo (healthy)."
    break
  fi
  echo "  Intento $i/6 - Estado: $STATUS"
  sleep 5
done
```

4. Verifica la conexión desde la línea de comandos:

```bash
docker exec postgres_etl psql -U etl_user -d etl_db -c "SELECT version();"
```

**Salida esperada:**

```
                                                    version
----------------------------------------------------------------------------------------------------------------
 PostgreSQL 16.2 (Debian 16.2-1.pgdg120+2) on x86_64-pc-linux-gnu, compiled by gcc (Debian 12.2.0-14) 12.2.0...
(1 row)
```

**Verificación:** El contenedor `postgres_etl` está en estado `healthy` y responde a consultas SQL.

---

### Paso 4: Crear el script de extracción `lab03_db_extraction.py`

**Objetivo:** Implementar el script completo con tres secciones: (1) creación de tabla e inserción de datos, (2) consultas parametrizadas, y (3) extracción paginada por lotes.

**Instrucciones:**

1. Crea el archivo `src/lab03_db_extraction.py`:

```bash
cat > ~/etl_course/labs/lab01/src/lab03_db_extraction.py << 'PYTHON'
"""
Lab 03: Extracción desde una base de datos relacional.
Secciones:
  1. Creación de tabla e inserción de datos desde Parquet.
  2. Consultas SQL parametrizadas con SQLAlchemy.
  3. Extracción paginada por lotes (batch extraction).
"""

import os
import sys
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, inspect

# ─────────────────────────────────────────────
# Configuración de rutas y variables de entorno
# ─────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5433")
DB_NAME = os.getenv("DB_NAME", "etl_db")
DB_USER = os.getenv("DB_USER", "etl_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_SCHEMA = os.getenv("DB_SCHEMA", "public")

PARQUET_PATH = PROJECT_ROOT / "data" / "processed" / "titanic.parquet"
BATCHES_DIR = PROJECT_ROOT / "data" / "raw" / "batches"

# Crear directorio de lotes si no existe
BATCHES_DIR.mkdir(parents=True, exist_ok=True)

# ─────────────────────────────────────────────
# Construcción del Engine con SQLAlchemy
# ─────────────────────────────────────────────
CONNECTION_STRING = (
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

engine = create_engine(
    CONNECTION_STRING,
    echo=False,
    pool_size=5,
    max_overflow=10,
    pool_pre_ping=True
)


def verificar_conexion():
    """Verifica que la conexión a la base de datos sea exitosa."""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            row = result.fetchone()
            print(f"[OK] Conexión exitosa a PostgreSQL. Test: {row[0]}")
            return True
    except Exception as e:
        print(f"[ERROR] No se pudo conectar a la base de datos: {e}")
        return False


# ═══════════════════════════════════════════════
# SECCIÓN 1: Creación de tabla e inserción de datos
# ═══════════════════════════════════════════════
def crear_tabla_e_insertar_datos():
    """
    Crea la tabla titanic_passengers y la puebla
    con los datos del archivo Parquet del Lab 02.
    """
    print("\n" + "=" * 60)
    print("SECCIÓN 1: Creación de tabla e inserción de datos")
    print("=" * 60)

    # Verificar que el archivo Parquet existe
    if not PARQUET_PATH.exists():
        print(f"[ERROR] No se encontró el archivo: {PARQUET_PATH}")
        print("Asegúrate de haber completado el Laboratorio 2.")
        sys.exit(1)

    # Leer datos desde Parquet
    df = pd.read_parquet(PARQUET_PATH)
    print(f"[INFO] Registros leídos desde Parquet: {len(df)}")
    print(f"[INFO] Columnas: {list(df.columns)}")

    # DDL para crear la tabla
    create_table_sql = text("""
        CREATE TABLE IF NOT EXISTS titanic_passengers (
            passenger_id SERIAL PRIMARY KEY,
            survived INTEGER,
            pclass INTEGER,
            name VARCHAR(200),
            sex VARCHAR(10),
            age FLOAT,
            sibsp INTEGER,
            parch INTEGER,
            ticket VARCHAR(50),
            fare FLOAT,
            cabin VARCHAR(50),
            embarked VARCHAR(5)
        );
    """)

    with engine.begin() as conn:
        # Crear tabla
        conn.execute(create_table_sql)
        print("[OK] Tabla 'titanic_passengers' creada (o ya existía).")

        # Verificar si la tabla ya tiene datos
        count_result = conn.execute(
            text("SELECT COUNT(*) FROM titanic_passengers")
        )
        existing_count = count_result.scalar()

        if existing_count > 0:
            print(f"[INFO] La tabla ya contiene {existing_count} registros. "
                  "Se omite la inserción.")
            return

    # Insertar datos usando pandas to_sql (eficiente para lotes grandes)
    # Normalizar nombres de columnas a minúsculas
    df.columns = [col.lower() for col in df.columns]

    # Renombear columnas si es necesario para coincidir con la tabla
    column_mapping = {
        "passengerid": "passenger_id",
        "survived": "survived",
        "pclass": "pclass",
        "name": "name",
        "sex": "sex",
        "age": "age",
        "sibsp": "sibsp",
        "parch": "parch",
        "ticket": "ticket",
        "fare": "fare",
        "cabin": "cabin",
        "embarked": "embarked"
    }

    # Seleccionar solo las columnas que existen en el DataFrame
    available_cols = [col for col in column_mapping.keys() if col in df.columns]
    df_insert = df[available_cols].rename(columns=column_mapping)

    # Eliminar la columna passenger_id si existe (la BD la genera automáticamente)
    if "passenger_id" in df_insert.columns:
        df_insert = df_insert.drop(columns=["passenger_id"])

    # Insertar con pandas to_sql
    df_insert.to_sql(
        "titanic_passengers",
        engine,
        schema=DB_SCHEMA,
        if_exists="append",
        index=False,
        method="multi"
    )

    # Verificar inserción
    with engine.connect() as conn:
        count_result = conn.execute(
            text("SELECT COUNT(*) FROM titanic_passengers")
        )
        total = count_result.scalar()
        print(f"[OK] Datos insertados correctamente. Total registros: {total}")


# ═══════════════════════════════════════════════
# SECCIÓN 2: Consultas SQL parametrizadas
# ═══════════════════════════════════════════════
def consultas_parametrizadas():
    """
    Ejecuta consultas SQL parametrizadas con SQLAlchemy
    para extraer subconjuntos de datos filtrados.
    """
    print("\n" + "=" * 60)
    print("SECCIÓN 2: Consultas SQL parametrizadas")
    print("=" * 60)

    with engine.connect() as conn:
        # Consulta 1: Filtrar por clase y sexo
        print("\n--- Consulta 1: Mujeres de primera clase ---")
        query_1 = text("""
            SELECT passenger_id, name, age, fare, survived
            FROM titanic_passengers
            WHERE pclass = :clase AND sex = :sexo
            ORDER BY fare DESC
            LIMIT :limite
        """)

        result_1 = conn.execute(
            query_1,
            {"clase": 1, "sexo": "female", "limite": 5}
        )
        df_q1 = pd.DataFrame(result_1.fetchall(), columns=result_1.keys())
        print(df_q1.to_string(index=False))
        print(f"Registros obtenidos: {len(df_q1)}")

        # Consulta 2: Estadísticas agrupadas con subquery
        print("\n--- Consulta 2: Tasa de supervivencia por clase ---")
        query_2 = text("""
            SELECT
                pclass,
                COUNT(*) AS total_pasajeros,
                SUM(survived) AS sobrevivientes,
                ROUND(AVG(survived) * 100, 2) AS tasa_supervivencia,
                ROUND(AVG(age)::numeric, 1) AS edad_promedio
            FROM titanic_passengers
            WHERE age IS NOT NULL
            GROUP BY pclass
            ORDER BY pclass
        """)

        result_2 = conn.execute(query_2)
        df_q2 = pd.DataFrame(result_2.fetchall(), columns=result_2.keys())
        print(df_q2.to_string(index=False))

        # Consulta 3: Filtro por rango de edad (parametrizado)
        print("\n--- Consulta 3: Pasajeros entre edad mínima y máxima ---")
        query_3 = text("""
            SELECT passenger_id, name, age, pclass, survived
            FROM titanic_passengers
            WHERE age BETWEEN :edad_min AND :edad_max
            ORDER BY age ASC
            LIMIT :limite
        """)

        result_3 = conn.execute(
            query_3,
            {"edad_min": 0, "edad_max": 10, "limite": 10}
        )
        df_q3 = pd.DataFrame(result_3.fetchall(), columns=result_3.keys())
        print(df_q3.to_string(index=False))
        print(f"Niños (0-10 años) encontrados: {len(df_q3)}")

        # Consulta 4: Subquery - pasajeros con tarifa superior al promedio
        print("\n--- Consulta 4: Pasajeros con tarifa > promedio ---")
        query_4 = text("""
            SELECT passenger_id, name, fare, pclass
            FROM titanic_passengers
            WHERE fare > (
                SELECT AVG(fare) FROM titanic_passengers
            )
            ORDER BY fare DESC
            LIMIT :limite
        """)

        result_4 = conn.execute(query_4, {"limite": 5})
        df_q4 = pd.DataFrame(result_4.fetchall(), columns=result_4.keys())
        print(df_q4.to_string(index=False))


# ═══════════════════════════════════════════════
# SECCIÓN 3: Extracción paginada por lotes
# ═══════════════════════════════════════════════
def extraccion_por_lotes(batch_size: int = 100):
    """
    Extrae todos los registros de la tabla en lotes de tamaño
    configurable usando LIMIT/OFFSET y exporta cada lote a CSV.

    Args:
        batch_size: Número de registros por lote (default: 100).
    """
    print("\n" + "=" * 60)
    print(f"SECCIÓN 3: Extracción paginada por lotes (batch_size={batch_size})")
    print("=" * 60)

    # Obtener el total de registros
    with engine.connect() as conn:
        total_result = conn.execute(
            text("SELECT COUNT(*) FROM titanic_passengers")
        )
        total_registros = total_result.scalar()
        print(f"[INFO] Total de registros en la tabla: {total_registros}")

    # Calcular número de lotes
    num_batches = (total_registros + batch_size - 1) // batch_size
    print(f"[INFO] Lotes a generar: {num_batches} (de {batch_size} registros c/u)")

    # Limpiar directorio de lotes previos
    for archivo in BATCHES_DIR.glob("batch_*.csv"):
        archivo.unlink()

    # Query parametrizada para paginación
    paginated_query = text("""
        SELECT passenger_id, survived, pclass, name, sex,
               age, sibsp, parch, ticket, fare, cabin, embarked
        FROM titanic_passengers
        ORDER BY passenger_id ASC
        LIMIT :limit OFFSET :offset
    """)

    archivos_generados = []

    with engine.connect() as conn:
        for batch_num in range(num_batches):
            offset = batch_num * batch_size

            # Ejecutar consulta paginada
            result = conn.execute(
                paginated_query,
                {"limit": batch_size, "offset": offset}
            )

            rows = result.fetchall()
            if not rows:
                break

            # Convertir a DataFrame
            df_batch = pd.DataFrame(rows, columns=result.keys())

            # Generar nombre de archivo con numeración de 3 dígitos
            batch_filename = f"batch_{batch_num + 1:03d}.csv"
            batch_path = BATCHES_DIR / batch_filename

            # Exportar a CSV
            df_batch.to_csv(batch_path, index=False, encoding="utf-8")
            archivos_generados.append(batch_filename)

            print(f"  [OK] {batch_filename} → {len(df_batch)} registros "
                  f"(offset: {offset})")

    print(f"\n[RESUMEN] Archivos generados: {len(archivos_generados)}")
    print(f"[RESUMEN] Directorio de salida: {BATCHES_DIR}")

    # Verificación final: contar registros totales en todos los CSVs
    total_en_csvs = 0
    for csv_file in sorted(BATCHES_DIR.glob("batch_*.csv")):
        df_check = pd.read_csv(csv_file)
        total_en_csvs += len(df_check)

    print(f"[VERIFICACIÓN] Total registros en CSVs: {total_en_csvs}")
    print(f"[VERIFICACIÓN] Total registros en BD:   {total_registros}")

    if total_en_csvs == total_registros:
        print("[OK] ¡Extracción por lotes completada sin pérdida de datos!")
    else:
        print("[ADVERTENCIA] Discrepancia en el conteo de registros.")

    return archivos_generados


# ═══════════════════════════════════════════════
# Ejecución principal
# ═══════════════════════════════════════════════
if __name__ == "__main__":
    print("=" * 60)
    print("LAB 03: Extracción desde Base de Datos Relacional")
    print("=" * 60)

    # Verificar conexión
    if not verificar_conexion():
        sys.exit(1)

    # Sección 1: Crear tabla e insertar datos
    crear_tabla_e_insertar_datos()

    # Sección 2: Consultas parametrizadas
    consultas_parametrizadas()

    # Sección 3: Extracción por lotes
    extraccion_por_lotes(batch_size=100)

    # Liberar recursos
    engine.dispose()
    print("\n[FIN] Pool de conexiones liberado. Laboratorio completado.")
PYTHON
```

**Salida esperada:** El archivo `src/lab03_db_extraction.py` se crea correctamente.

**Verificación:**

```bash
ls -la ~/etl_course/labs/lab01/src/lab03_db_extraction.py
```

---

### Paso 5: Ejecutar el script y validar la Sección 1 (Creación e inserción)

**Objetivo:** Ejecutar el script para crear la tabla `titanic_passengers` y poblarla con los datos del Parquet.

**Instrucciones:**

1. Regresa al directorio raíz del proyecto:

```bash
cd ~/etl_course/labs/lab01
```

2. Ejecuta el script:

```bash
python src/lab03_db_extraction.py
```

**Salida esperada (Sección 1):**

```
============================================================
LAB 03: Extracción desde Base de Datos Relacional
============================================================
[OK] Conexión exitosa a PostgreSQL. Test: 1

============================================================
SECCIÓN 1: Creación de tabla e inserción de datos
============================================================
[INFO] Registros leídos desde Parquet: 891
[INFO] Columnas: ['Survived', 'Pclass', 'Name', 'Sex', 'Age', 'SibSp', 'Parch', 'Ticket', 'Fare', 'Cabin', 'Embarked']
[OK] Tabla 'titanic_passengers' creada (o ya existía).
[OK] Datos insertados correctamente. Total registros: 891
```

> **Nota:** Los nombres de columnas pueden variar según cómo se generó el Parquet en el Lab 02. El script normaliza las columnas a minúsculas automáticamente.

**Verificación:** Confirma la existencia de datos directamente en PostgreSQL:

```bash
docker exec postgres_etl psql -U etl_user -d etl_db -c \
  "SELECT COUNT(*) AS total, COUNT(DISTINCT pclass) AS clases FROM titanic_passengers;"
```

Salida esperada:

```
 total | clases
-------+--------
   891 |      3
```

---

### Paso 6: Validar la Sección 2 (Consultas parametrizadas)

**Objetivo:** Verificar que las consultas parametrizadas se ejecutan correctamente con filtros dinámicos.

**Instrucciones:**

La Sección 2 se ejecutó automáticamente en el paso anterior. Revisa la salida del terminal para confirmar:

**Salida esperada (Sección 2):**

```
============================================================
SECCIÓN 2: Consultas SQL parametrizadas
============================================================

--- Consulta 1: Mujeres de primera clase ---
 passenger_id                              name   age     fare  survived
          ...                               ...   ...      ...       ...
Registros obtenidos: 5

--- Consulta 2: Tasa de supervivencia por clase ---
 pclass  total_pasajeros  sobrevivientes  tasa_supervivencia  edad_promedio
      1              ...             ...                ...            ...
      2              ...             ...                ...            ...
      3              ...             ...                ...            ...

--- Consulta 3: Pasajeros entre edad mínima y máxima ---
 passenger_id          name   age  pclass  survived
          ...           ...   ...     ...       ...
Niños (0-10 años) encontrados: 10

--- Consulta 4: Pasajeros con tarifa > promedio ---
 passenger_id                              name     fare  pclass
          ...                               ...      ...     ...
```

**Verificación:** Las 4 consultas retornan datos sin errores. Los parámetros `:clase`, `:sexo`, `:limite`, `:edad_min`, `:edad_max` se inyectan de forma segura (sin concatenación de strings).

---

### Paso 7: Validar la Sección 3 (Extracción por lotes)

**Objetivo:** Confirmar que se generaron los archivos CSV de lotes con la totalidad de los registros.

**Instrucciones:**

1. Revisa la salida del terminal para la Sección 3:

**Salida esperada (Sección 3):**

```
============================================================
SECCIÓN 3: Extracción paginada por lotes (batch_size=100)
============================================================
[INFO] Total de registros en la tabla: 891
[INFO] Lotes a generar: 9 (de 100 registros c/u)
  [OK] batch_001.csv → 100 registros (offset: 0)
  [OK] batch_002.csv → 100 registros (offset: 100)
  [OK] batch_003.csv → 100 registros (offset: 200)
  [OK] batch_004.csv → 100 registros (offset: 300)
  [OK] batch_005.csv → 100 registros (offset: 400)
  [OK] batch_006.csv → 100 registros (offset: 500)
  [OK] batch_007.csv → 100 registros (offset: 600)
  [OK] batch_008.csv → 100 registros (offset: 700)
  [OK] batch_009.csv → 91 registros (offset: 800)

[RESUMEN] Archivos generados: 9
[RESUMEN] Directorio de salida: .../data/raw/batches
[VERIFICACIÓN] Total registros en CSVs: 891
[VERIFICACIÓN] Total registros en BD:   891
[OK] ¡Extracción por lotes completada sin pérdida de datos!

[FIN] Pool de conexiones liberado. Laboratorio completado.
```

2. Verifica los archivos generados:

```bash
ls -la ~/etl_course/labs/lab01/data/raw/batches/
```

**Salida esperada:**

```
batch_001.csv
batch_002.csv
batch_003.csv
batch_004.csv
batch_005.csv
batch_006.csv
batch_007.csv
batch_008.csv
batch_009.csv
```

3. Inspecciona el contenido del primer y último lote:

```bash
head -3 ~/etl_course/labs/lab01/data/raw/batches/batch_001.csv
echo "---"
wc -l ~/etl_course/labs/lab01/data/raw/batches/batch_009.csv
```

**Salida esperada:**

```
passenger_id,survived,pclass,name,sex,age,sibsp,parch,ticket,fare,cabin,embarked
1,0,3,"Braund, Mr. Owen Harris",male,22.0,1,0,A/5 21171,7.25,,S
2,1,1,"Cumings, Mrs. John Bradley...",female,38.0,1,0,PC 17599,71.2833,C85,C
---
92 batch_009.csv
```

> El último lote tiene 92 líneas: 1 encabezado + 91 registros de datos.

**Verificación:** Se generaron exactamente 9 archivos CSV y la suma de registros (100×8 + 91 = 891) coincide con el total en la base de datos.

---

## Validación y Pruebas

Ejecuta las siguientes verificaciones para confirmar que el laboratorio se completó correctamente:

### Test 1: Integridad de la base de datos

```bash
docker exec postgres_etl psql -U etl_user -d etl_db -c "
SELECT
    COUNT(*) AS total_registros,
    COUNT(DISTINCT pclass) AS clases_distintas,
    COUNT(DISTINCT sex) AS sexos_distintos,
    ROUND(AVG(age)::numeric, 2) AS edad_promedio
FROM titanic_passengers;
"
```

**Resultado esperado:**

```
 total_registros | clases_distintas | sexos_distintos | edad_promedio
-----------------+------------------+-----------------+---------------
             891 |                3 |               2 |         29.70
```

### Test 2: Consistencia de archivos por lotes

```bash
cd ~/etl_course/labs/lab01
python -c "
import pandas as pd
from pathlib import Path

batches_dir = Path('data/raw/batches')
total = 0
for f in sorted(batches_dir.glob('batch_*.csv')):
    df = pd.read_csv(f)
    total += len(df)
    print(f'{f.name}: {len(df)} registros')

print(f'\nTotal en CSVs: {total}')
assert total == 891, f'ERROR: Se esperaban 891, se obtuvieron {total}'
print('✓ Validación exitosa: 891 registros extraídos sin pérdida.')
"
```

### Test 3: Consulta parametrizada de verificación

```bash
cd ~/etl_course/labs/lab01
python -c "
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from pathlib import Path
import os

load_dotenv(Path('.env'))
engine = create_engine(
    f\"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}\"
)

with engine.connect() as conn:
    r = conn.execute(text('SELECT COUNT(*) FROM titanic_passengers WHERE survived = :s'), {'s': 1})
    sobrevivientes = r.scalar()
    print(f'Sobrevivientes: {sobrevivientes}')
    assert sobrevivientes > 0, 'No se encontraron sobrevivientes'
    print('✓ Consulta parametrizada funciona correctamente.')

engine.dispose()
"
```

---

## Solución de Problemas

### Problema 1: Error de conexión `connection refused` al ejecutar el script

**Síntomas:**

```
[ERROR] No se pudo conectar a la base de datos: 
(psycopg2.OperationalError) connection to server at "localhost" (127.0.0.1), port 5433 failed: Connection refused
```

**Causa:** El contenedor Docker `postgres_etl` no está en ejecución o no ha terminado de inicializarse. Esto puede ocurrir si Docker no está activo, si el contenedor falló al arrancar, o si se ejecutó el script antes de que PostgreSQL completara su inicialización.

**Solución:**

```bash
# 1. Verificar que Docker está corriendo
docker info > /dev/null 2>&1 && echo "Docker activo" || echo "Docker NO activo"

# 2. Verificar estado del contenedor
docker ps -a --filter "name=postgres_etl" --format "{{.Status}}"

# 3. Si no está corriendo, levantarlo
cd ~/etl_course/labs/lab01/config
docker compose up -d

# 4. Esperar a que esté healthy
sleep 10
docker inspect --format='{{.State.Health.Status}}' postgres_etl

# 5. Si hay errores, revisar logs
docker logs postgres_etl --tail 20
```

---

### Problema 2: Error `UndefinedTable` o `relation "titanic_passengers" does not exist`

**Síntomas:**

```
sqlalchemy.exc.ProgrammingError: (psycopg2.errors.UndefinedTable) 
relation "titanic_passengers" does not exist
```

**Causa:** El script se ejecutó parcialmente (por ejemplo, falló durante la inserción) y la tabla no se creó correctamente, o el volumen Docker fue eliminado y la base de datos está vacía.

**Solución:**

```bash
# 1. Verificar si la tabla existe
docker exec postgres_etl psql -U etl_user -d etl_db -c "\dt titanic_passengers"

# 2. Si no existe, eliminar datos residuales y re-ejecutar
docker exec postgres_etl psql -U etl_user -d etl_db -c "DROP TABLE IF EXISTS titanic_passengers CASCADE;"

# 3. Re-ejecutar el script completo
cd ~/etl_course/labs/lab01
python src/lab03_db_extraction.py
```

Si el problema persiste y el archivo Parquet tiene columnas con nombres diferentes a los esperados, inspecciona las columnas:

```bash
python -c "
import pandas as pd
df = pd.read_parquet('data/processed/titanic.parquet')
print('Columnas:', list(df.columns))
print('Shape:', df.shape)
print(df.head(2))
"
```

Ajusta el `column_mapping` en el script si los nombres difieren.

---

## Limpieza

Si deseas liberar recursos al terminar el laboratorio (opcional — el contenedor se reutilizará en laboratorios posteriores):

```bash
# Detener el contenedor (mantiene los datos en el volumen)
cd ~/etl_course/labs/lab01/config
docker compose stop

# Para eliminar completamente (contenedor + volumen + datos):
# docker compose down -v
# ADVERTENCIA: Esto borra todos los datos de la BD.
```

> **Recomendación:** NO ejecutes `docker compose down -v` si planeas continuar con los laboratorios 4 y 5, ya que reutilizan la tabla `titanic_passengers`.

Para limpiar solo los archivos de lotes generados:

```bash
rm -f ~/etl_course/labs/lab01/data/raw/batches/batch_*.csv
```

---

## Resumen

En este laboratorio has logrado:

| Concepto | Implementación |
|----------|----------------|
| **Contenedor PostgreSQL** | Docker Compose con healthcheck, puerto 5433, credenciales seguras |
| **Conexión con SQLAlchemy** | `create_engine()` con `pool_pre_ping=True`, `pool_size=5` |
| **Gestión de credenciales** | Archivo `.env` + `python-dotenv` (nunca hardcoded) |
| **Creación de tabla** | DDL con `CREATE TABLE IF NOT EXISTS` + transacción con `engine.begin()` |
| **Inserción masiva** | `pandas.to_sql()` con `method="multi"` para eficiencia |
| **Consultas parametrizadas** | `text()` con parámetros `:nombre` para prevenir SQL injection |
| **Extracción por lotes** | `LIMIT/OFFSET` con paginación configurable |
| **Exportación a CSV** | Archivos numerados `batch_001.csv` a `batch_009.csv` |
| **Context managers** | `with engine.connect()` y `with engine.begin()` para manejo seguro de transacciones |

### Conceptos clave aplicados de la Lección 3.1

- La cadena de conexión `postgresql+psycopg2://...` selecciona el dialecto PostgreSQL con el driver psycopg2.
- El `Engine` gestiona un pool de conexiones reutilizables, evitando el overhead de abrir/cerrar conexiones por cada operación.
- El parámetro `pool_pre_ping=True` verifica la salud de las conexiones antes de reutilizarlas.
- `engine.dispose()` libera todas las conexiones del pool al finalizar el proceso.

### Recursos adicionales

- [Documentación SQLAlchemy 2.0 — Engine Configuration](https://docs.sqlalchemy.org/en/20/core/engines.html)
- [PostgreSQL LIMIT/OFFSET — Documentación oficial](https://www.postgresql.org/docs/16/queries-limit.html)
- [python-dotenv — Documentación](https://saurabh-kumar.com/python-dotenv/)
- [pandas.DataFrame.to_sql — Referencia](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_sql.html)
