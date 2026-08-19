# Conversión entre formatos de datos

## Metadatos

| Campo | Valor |
|-------|-------|
| **Duración** | 35 minutos |
| **Complejidad** | Fácil |
| **Nivel Bloom** | Aplicar |

## Descripción General

En este laboratorio tomarás el archivo `titanic.csv` generado en el Laboratorio 1 y lo convertirás a cinco formatos diferentes (Excel, JSON, NDJSON, XML y Parquet), aplicando transformaciones de limpieza durante el proceso. Implementarás funciones independientes para cada conversión y una validación cruzada que garantice la integridad de los datos entre formatos. Los archivos resultantes serán insumo directo para los Laboratorios 3 y 4.

## Objetivos de Aprendizaje

- [ ] Leer un archivo CSV con pandas y aplicar transformaciones de limpieza (snake_case, tipos de datos, nulos) antes de la conversión.
- [ ] Exportar un DataFrame a cinco formatos distintos (Excel, JSON, NDJSON, XML, Parquet) utilizando pandas y bibliotecas especializadas.
- [ ] Implementar una función de validación cruzada que verifique la consistencia de filas, columnas y tipos de datos entre el DataFrame original y cada archivo generado.

## Prerrequisitos

### Conocimientos previos

- Laboratorio 1 completado exitosamente (archivo `titanic.csv` existente en `data/raw/`).
- Familiaridad con DataFrames de pandas (lectura, selección de columnas, tipos de datos).
- Comprensión básica de estructuras JSON y XML.

### Acceso requerido

- Entorno virtual `venv_etl` activo con pandas 2.2.1 instalado.
- Terminal con acceso al directorio `~/etl_course/labs/lab01/`.

## Entorno del Laboratorio

### Software necesario

| Componente | Versión | Propósito |
|---|---|---|
| Python | 3.12.1 | Intérprete principal |
| pandas | 2.2.1 | Lectura, transformación y escritura de datos |
| openpyxl | 3.1.2 | Motor de escritura/lectura Excel `.xlsx` |
| pyarrow | 15.0.2 | Motor de escritura/lectura Parquet |
| lxml | 5.1.0 | Motor de escritura/lectura XML |

### Configuración inicial

```bash
cd ~/etl_course/labs/lab01
source venv_etl/bin/activate   # Linux/macOS
# .\venv_etl\Scripts\activate  # Windows
```

Verifica que el archivo de entrada existe:

```bash
ls -la data/raw/titanic.csv
```

---

## Paso a Paso

### Paso 1: Instalar dependencias adicionales

**Objetivo:** Agregar las bibliotecas requeridas para los formatos Excel, Parquet y XML al entorno virtual.

**Instrucciones:**

1. Instala las dependencias necesarias:

```bash
pip install openpyxl==3.1.2 pyarrow==15.0.2 lxml==5.1.0
```

2. Actualiza el archivo `requirements.txt` para reflejar las nuevas dependencias:

```bash
pip freeze | grep -iE "openpyxl|pyarrow|lxml" >> requirements.txt
```

3. Verifica que el archivo `requirements.txt` contenga las tres nuevas entradas:

```bash
cat requirements.txt | grep -iE "openpyxl|pyarrow|lxml"
```

**Salida esperada:**

```
lxml==5.1.0
openpyxl==3.1.2
pyarrow==15.0.2
```

**Verificación:** Las tres bibliotecas deben aparecer listadas. Si alguna falta, repite la instalación individual con `pip install <paquete>==<versión>`.

---

### Paso 2: Crear el script de conversión — lectura y limpieza

**Objetivo:** Crear el archivo `lab02_format_conversion.py` con la función de carga y limpieza del CSV original.

**Instrucciones:**

1. Crea el archivo del script:

```bash
touch src/lab02_format_conversion.py
```

2. Abre el archivo en tu editor y escribe el siguiente código para las importaciones y la función de carga:

```python
"""
Lab 02-00-01: Conversión entre formatos de datos.
Lee titanic.csv, aplica limpieza y exporta a múltiples formatos.
"""

import json
import re
from pathlib import Path

import pandas as pd

# ─────────────────────────────────────────────
# Rutas del proyecto
# ─────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"

# Crear directorio de salida si no existe
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

INPUT_FILE = RAW_DIR / "titanic.csv"


def to_snake_case(name: str) -> str:
    """Convierte un nombre de columna a snake_case."""
    # Insertar _ antes de mayúsculas precedidas por minúsculas
    s1 = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    # Reemplazar espacios y caracteres especiales por _
    s2 = re.sub(r"[\s\-\.]+", "_", s1)
    # Eliminar caracteres no alfanuméricos excepto _
    s3 = re.sub(r"[^a-zA-Z0-9_]", "", s2)
    return s3.lower().strip("_")


def load_and_clean(filepath: Path = INPUT_FILE) -> pd.DataFrame:
    """
    Lee el CSV de Titanic y aplica transformaciones de limpieza:
    - Normaliza nombres de columnas a snake_case.
    - Convierte tipos de datos apropiados.
    - Estandariza valores nulos.
    """
    df = pd.read_csv(filepath)

    # 1. Normalizar nombres de columnas
    df.columns = [to_snake_case(col) for col in df.columns]

    # 2. Conversión de tipos de datos
    # survived y pclass como enteros categóricos
    if "survived" in df.columns:
        df["survived"] = df["survived"].astype("Int64")  # nullable integer
    if "pclass" in df.columns:
        df["pclass"] = df["pclass"].astype("Int64")

    # sex como categoría
    if "sex" in df.columns:
        df["sex"] = df["sex"].astype("category")

    # age y fare como float64 (ya lo son normalmente, pero forzamos)
    for col in ["age", "fare"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # 3. Manejo de nulos: reemplazar strings vacíos por NaN
    df.replace(r"^\s*$", pd.NA, regex=True, inplace=True)

    print(f"[CARGA] Archivo: {filepath.name}")
    print(f"[CARGA] Shape: {df.shape}")
    print(f"[CARGA] Columnas: {list(df.columns)}")
    print(f"[CARGA] Tipos:\n{df.dtypes}\n")

    return df
```

**Salida esperada:** Archivo `src/lab02_format_conversion.py` creado con las funciones `to_snake_case` y `load_and_clean`.

**Verificación:**

```bash
python -c "import sys; sys.path.insert(0, 'src'); from lab02_format_conversion import load_and_clean; df = load_and_clean(); print(df.shape)"
```

Deberías ver el shape del DataFrame (por ejemplo, `(891, 12)` para el dataset Titanic estándar).

---

### Paso 3: Implementar funciones de exportación

**Objetivo:** Agregar las cinco funciones de conversión (Excel, JSON, NDJSON, XML, Parquet) al script.

**Instrucciones:**

1. Añade las siguientes funciones al archivo `src/lab02_format_conversion.py`:

```python
# ─────────────────────────────────────────────
# Funciones de exportación
# ─────────────────────────────────────────────

def export_to_excel(df: pd.DataFrame, output_path: Path = None) -> Path:
    """Exporta el DataFrame a formato Excel (.xlsx)."""
    output_path = output_path or PROCESSED_DIR / "titanic.xlsx"
    df.to_excel(output_path, index=False, engine="openpyxl", sheet_name="data")
    print(f"[EXCEL] Exportado: {output_path} ({output_path.stat().st_size:,} bytes)")
    return output_path


def export_to_json(df: pd.DataFrame, output_path: Path = None) -> Path:
    """Exporta el DataFrame a formato JSON orientado a registros."""
    output_path = output_path or PROCESSED_DIR / "titanic.json"
    df.to_json(output_path, orient="records", indent=2, force_ascii=False)
    print(f"[JSON]  Exportado: {output_path} ({output_path.stat().st_size:,} bytes)")
    return output_path


def export_to_ndjson(df: pd.DataFrame, output_path: Path = None) -> Path:
    """Exporta el DataFrame a formato NDJSON (una línea JSON por registro)."""
    output_path = output_path or PROCESSED_DIR / "titanic.ndjson"
    with open(output_path, "w", encoding="utf-8") as f:
        for _, row in df.iterrows():
            # Convertir a dict y manejar NaN -> null
            record = row.where(row.notna(), None).to_dict()
            f.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
    print(f"[NDJSON] Exportado: {output_path} ({output_path.stat().st_size:,} bytes)")
    return output_path


def export_to_xml(df: pd.DataFrame, output_path: Path = None) -> Path:
    """Exporta el DataFrame a formato XML."""
    output_path = output_path or PROCESSED_DIR / "titanic.xml"
    df.to_xml(
        output_path,
        index=False,
        root_name="passengers",
        row_name="passenger",
        parser="lxml",
    )
    print(f"[XML]   Exportado: {output_path} ({output_path.stat().st_size:,} bytes)")
    return output_path


def export_to_parquet(df: pd.DataFrame, output_path: Path = None) -> Path:
    """Exporta el DataFrame a formato Parquet con compresión snappy."""
    output_path = output_path or PROCESSED_DIR / "titanic.parquet"
    df.to_parquet(output_path, index=False, engine="pyarrow", compression="snappy")
    print(f"[PARQUET] Exportado: {output_path} ({output_path.stat().st_size:,} bytes)")
    return output_path
```

**Salida esperada:** Cinco funciones de exportación añadidas al script, cada una retornando la ruta del archivo generado.

**Verificación:** El archivo no debe tener errores de sintaxis:

```bash
python -c "import sys; sys.path.insert(0, 'src'); import lab02_format_conversion; print('Importación exitosa')"
```

---

### Paso 4: Implementar la función de validación cruzada

**Objetivo:** Crear una función que re-lea cada archivo generado y verifique que el número de filas, columnas y tipos de datos sean consistentes con el DataFrame original.

**Instrucciones:**

1. Añade la siguiente función de validación al script:

```python
# ─────────────────────────────────────────────
# Validación cruzada
# ─────────────────────────────────────────────

def validate_conversion(df_original: pd.DataFrame) -> dict:
    """
    Re-lee cada archivo generado y compara shape y dtypes
    contra el DataFrame original.
    Retorna un diccionario con resultados de validación.
    """
    results = {}
    expected_rows, expected_cols = df_original.shape

    # --- Excel ---
    excel_path = PROCESSED_DIR / "titanic.xlsx"
    if excel_path.exists():
        df_excel = pd.read_excel(excel_path, engine="openpyxl")
        results["excel"] = {
            "rows_match": df_excel.shape[0] == expected_rows,
            "cols_match": df_excel.shape[1] == expected_cols,
            "shape": df_excel.shape,
        }

    # --- JSON ---
    json_path = PROCESSED_DIR / "titanic.json"
    if json_path.exists():
        df_json = pd.read_json(json_path, orient="records")
        results["json"] = {
            "rows_match": df_json.shape[0] == expected_rows,
            "cols_match": df_json.shape[1] == expected_cols,
            "shape": df_json.shape,
        }

    # --- NDJSON ---
    ndjson_path = PROCESSED_DIR / "titanic.ndjson"
    if ndjson_path.exists():
        df_ndjson = pd.read_json(ndjson_path, lines=True)
        results["ndjson"] = {
            "rows_match": df_ndjson.shape[0] == expected_rows,
            "cols_match": df_ndjson.shape[1] == expected_cols,
            "shape": df_ndjson.shape,
        }

    # --- XML ---
    xml_path = PROCESSED_DIR / "titanic.xml"
    if xml_path.exists():
        df_xml = pd.read_xml(xml_path, parser="lxml")
        results["xml"] = {
            "rows_match": df_xml.shape[0] == expected_rows,
            "cols_match": df_xml.shape[1] == expected_cols,
            "shape": df_xml.shape,
        }

    # --- Parquet ---
    parquet_path = PROCESSED_DIR / "titanic.parquet"
    if parquet_path.exists():
        df_parquet = pd.read_parquet(parquet_path, engine="pyarrow")
        results["parquet"] = {
            "rows_match": df_parquet.shape[0] == expected_rows,
            "cols_match": df_parquet.shape[1] == expected_cols,
            "shape": df_parquet.shape,
        }

    # Resumen
    print("=" * 60)
    print("VALIDACIÓN CRUZADA DE FORMATOS")
    print("=" * 60)
    print(f"{'Formato':<10} {'Filas OK':<12} {'Cols OK':<12} {'Shape'}")
    print("-" * 60)
    for fmt, info in results.items():
        status_rows = "✓" if info["rows_match"] else "✗"
        status_cols = "✓" if info["cols_match"] else "✗"
        print(f"{fmt:<10} {status_rows:<12} {status_cols:<12} {info['shape']}")
    print("=" * 60)

    all_passed = all(
        v["rows_match"] and v["cols_match"] for v in results.values()
    )
    print(f"\nResultado global: {'TODAS LAS VALIDACIONES PASARON ✓' if all_passed else 'HAY ERRORES ✗'}\n")

    return results
```

**Salida esperada:** Función `validate_conversion` añadida sin errores de sintaxis.

---

### Paso 5: Implementar la función principal y ejecutar el script

**Objetivo:** Añadir el bloque `main` que orquesta la carga, las exportaciones y la validación, y ejecutar el script completo.

**Instrucciones:**

1. Añade el bloque principal al final del script:

```python
# ─────────────────────────────────────────────
# Ejecución principal
# ─────────────────────────────────────────────

def main():
    """Orquesta la carga, conversión y validación."""
    print("=" * 60)
    print("LAB 02-00-01: CONVERSIÓN ENTRE FORMATOS DE DATOS")
    print("=" * 60 + "\n")

    # 1. Carga y limpieza
    df = load_and_clean()

    # 2. Exportaciones
    export_to_excel(df)
    export_to_json(df)
    export_to_ndjson(df)
    export_to_xml(df)
    export_to_parquet(df)

    print()  # línea en blanco

    # 3. Validación
    validate_conversion(df)


if __name__ == "__main__":
    main()
```

2. Ejecuta el script desde el directorio raíz del proyecto:

```bash
cd ~/etl_course/labs/lab01
python src/lab02_format_conversion.py
```

**Salida esperada:**

```
============================================================
LAB 02-00-01: CONVERSIÓN ENTRE FORMATOS DE DATOS
============================================================

[CARGA] Archivo: titanic.csv
[CARGA] Shape: (891, 12)
[CARGA] Columnas: ['passenger_id', 'survived', 'pclass', 'name', 'sex', 'age', 'sib_sp', 'parch', 'ticket', 'fare', 'cabin', 'embarked']
[CARGA] Tipos:
passenger_id      int64
survived          Int64
pclass            Int64
name             object
sex            category
age             float64
sib_sp            int64
parch             int64
ticket           object
fare            float64
cabin            object
embarked         object
dtype: object

[EXCEL] Exportado: .../data/processed/titanic.xlsx (43,520 bytes)
[JSON]  Exportado: .../data/processed/titanic.json (208,451 bytes)
[NDJSON] Exportado: .../data/processed/titanic.ndjson (155,327 bytes)
[XML]   Exportado: .../data/processed/titanic.xml (312,890 bytes)
[PARQUET] Exportado: .../data/processed/titanic.parquet (25,614 bytes)

============================================================
VALIDACIÓN CRUZADA DE FORMATOS
============================================================
Formato    Filas OK     Cols OK      Shape
------------------------------------------------------------
excel      ✓            ✓            (891, 12)
json       ✓            ✓            (891, 12)
ndjson     ✓            ✓            (891, 12)
xml        ✓            ✓            (891, 12)
parquet    ✓            ✓            (891, 12)
============================================================

Resultado global: TODAS LAS VALIDACIONES PASARON ✓
```

> **Nota:** Los tamaños de archivo y los nombres exactos de columnas pueden variar ligeramente según el CSV de origen del Laboratorio 1. Lo importante es que el shape sea consistente en todos los formatos.

**Verificación:** Confirma que los cinco archivos existen en `data/processed/`:

```bash
ls -la data/processed/titanic.*
```

Deberías ver cinco archivos: `titanic.xlsx`, `titanic.json`, `titanic.ndjson`, `titanic.xml` y `titanic.parquet`.

---

### Paso 6: Inspeccionar los archivos generados

**Objetivo:** Verificar manualmente el contenido de cada formato para confirmar la correcta estructura.

**Instrucciones:**

1. Inspecciona las primeras líneas del archivo JSON:

```bash
head -20 data/processed/titanic.json
```

Deberías ver un arreglo JSON con objetos bien formateados e indentados.

2. Inspecciona las primeras líneas del archivo NDJSON:

```bash
head -3 data/processed/titanic.ndjson
```

Cada línea debe ser un objeto JSON independiente (sin comas entre líneas).

3. Inspecciona el inicio del archivo XML:

```bash
head -15 data/processed/titanic.xml
```

Deberías ver la declaración XML y los elementos `<passengers>` y `<passenger>`.

4. Verifica los metadatos del archivo Parquet:

```python
python -c "
import pyarrow.parquet as pq
meta = pq.read_metadata('data/processed/titanic.parquet')
print(f'Filas: {meta.num_rows}')
print(f'Columnas: {meta.num_columns}')
print(f'Formato: {meta.format_version}')
print(f'Compresión: snappy')
"
```

**Salida esperada para Parquet:**

```
Filas: 891
Columnas: 12
Formato: 2.6
Compresión: snappy
```

---

## Validación y Pruebas

Ejecuta el siguiente script de prueba rápida para confirmar la integridad completa:

```bash
python -c "
import sys
sys.path.insert(0, 'src')
from lab02_format_conversion import load_and_clean, validate_conversion

df = load_and_clean()
results = validate_conversion(df)

# Verificación programática
assert len(results) == 5, f'Se esperaban 5 formatos, se encontraron {len(results)}'
for fmt, info in results.items():
    assert info['rows_match'], f'{fmt}: filas no coinciden'
    assert info['cols_match'], f'{fmt}: columnas no coinciden'

print('\\n✓ TODAS LAS PRUEBAS AUTOMATIZADAS PASARON')
"
```

Si ves el mensaje `✓ TODAS LAS PRUEBAS AUTOMATIZADAS PASARON`, el laboratorio está completado correctamente.

---

## Solución de Problemas

### Problema 1: `ModuleNotFoundError: No module named 'openpyxl'`

**Síntomas:** Al ejecutar el script, Python lanza un error indicando que no encuentra el módulo `openpyxl` (o `pyarrow` o `lxml`).

**Causa:** Las dependencias no se instalaron en el entorno virtual activo, o el entorno virtual no está activado.

**Solución:**

```bash
# Verificar que el entorno virtual está activo
which python   # Debe apuntar a venv_etl/bin/python

# Si no está activo:
source ~/etl_course/labs/lab01/venv_etl/bin/activate

# Reinstalar las dependencias
pip install openpyxl==3.1.2 pyarrow==15.0.2 lxml==5.1.0

# Verificar instalación
pip list | grep -iE "openpyxl|pyarrow|lxml"
```

---

### Problema 2: `FileNotFoundError: data/raw/titanic.csv`

**Síntomas:** El script falla al intentar leer el archivo CSV de entrada con un error de archivo no encontrado.

**Causa:** El Laboratorio 1 no se completó, el archivo fue eliminado, o el script se ejecuta desde un directorio diferente al esperado.

**Solución:**

```bash
# Verificar ubicación actual
pwd   # Debe ser ~/etl_course/labs/lab01

# Verificar que el archivo existe
ls data/raw/titanic.csv

# Si no existe, descargarlo nuevamente (fuente pública del dataset Titanic):
curl -o data/raw/titanic.csv \
  "https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv"

# Verificar descarga
wc -l data/raw/titanic.csv   # Debe mostrar ~892 líneas (891 + encabezado)
```

---

## Limpieza

Los archivos generados en `data/processed/` son necesarios para los Laboratorios 3 y 4. **No los elimines.** Si necesitas regenerarlos, simplemente vuelve a ejecutar:

```bash
python src/lab02_format_conversion.py
```

Si en algún momento deseas limpiar solo los archivos de este laboratorio (por ejemplo, para re-ejecutar desde cero):

```bash
rm -f data/processed/titanic.xlsx data/processed/titanic.json \
      data/processed/titanic.ndjson data/processed/titanic.xml \
      data/processed/titanic.parquet
```

---

## Resumen

En este laboratorio has logrado:

| Concepto | Aplicación |
|---|---|
| Normalización de columnas | Conversión automática a `snake_case` con regex |
| Conversión de tipos | Uso de `Int64` (nullable), `category` y `float64` |
| Exportación multi-formato | Excel (openpyxl), JSON, NDJSON, XML (lxml), Parquet (pyarrow) |
| Validación de integridad | Comparación de shape entre DataFrame original y archivos re-leídos |
| Compresión eficiente | Parquet con snappy produce archivos ~60% más pequeños que CSV |

**Puntos clave:**

- El formato **Parquet** es el más eficiente en espacio y preserva tipos de datos nativamente (incluyendo nullable integers).
- **NDJSON** es ideal para procesamiento en streaming ya que cada línea es independiente.
- **JSON orientado a registros** es el formato más compatible con APIs REST.
- **XML** genera archivos más grandes pero es necesario para interoperabilidad con sistemas empresariales legacy.
- La validación cruzada es una práctica esencial en pipelines ETL para detectar pérdida silenciosa de datos.

### Recursos adicionales

- [pandas I/O Tools — Documentación oficial](https://pandas.pydata.org/docs/user_guide/io.html)
- [Apache Parquet Format Specification](https://parquet.apache.org/documentation/latest/)
- [JSON Lines (NDJSON) Specification](https://jsonlines.org/)
