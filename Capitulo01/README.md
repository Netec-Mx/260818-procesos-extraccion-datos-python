# Preparación del entorno y primer proceso de extracción

## Metadata

| Campo | Valor |
|-------|-------|
| **Duración** | 30 minutos |
| **Complejidad** | Fácil |
| **Nivel Bloom** | Aplicar |
| **Tecnologías** | Python 3.12.1, virtualenv 20.25.1, pip 24.0, pandas 2.2.1, requests 2.31.0, SQLAlchemy 2.0.29 |

---

## Visión General

En este laboratorio construirás desde cero el entorno de trabajo profesional que utilizarás durante todo el curso. Crearás la estructura de directorios estándar del proyecto, configurarás un entorno virtual aislado con las dependencias esenciales y ejecutarás tu primer script de extracción de datos. El script descargará el dataset público Titanic en formato CSV, lo persistirá en disco y generará un reporte de validación básico con pandas.

---

## Objetivos de Aprendizaje

Al completar este laboratorio serás capaz de:

- [ ] Crear y activar un entorno virtual con `virtualenv 20.25.1` usando Python 3.12.1, verificando la instalación correcta de las bibliotecas esenciales.
- [ ] Estructurar un proyecto de extracción de datos con la convención de directorios estándar (`src/`, `data/raw/`, `data/processed/`, `notebooks/`, `tests/`, `config/`).
- [ ] Ejecutar un script de extracción que descargue un dataset CSV público con `requests`, lo cargue con `pandas` y genere un reporte de validación (shape, dtypes, nulls).
- [ ] Gestionar dependencias del proyecto mediante `requirements.txt` con versiones exactas fijadas (pinned).

---

## Prerrequisitos

### Conocimientos previos

| Requisito | Detalle |
|-----------|---------|
| Terminal/línea de comandos | Comandos básicos: `cd`, `mkdir`, `ls` (Linux/macOS) o `dir` (Windows) |
| Python básico | Variables, funciones `print()`, ejecución de scripts `.py` |
| Conceptos ETL | Haber leído la Lección 1.1 sobre el proceso de extracción de datos |

### Acceso requerido

- Python 3.12.1 instalado y accesible desde terminal (`python --version` o `python3 --version`)
- pip 24.0 disponible (`pip --version`)
- Conexión a Internet activa (descarga de paquetes PyPI y dataset público)

---

## Entorno del Laboratorio

### Hardware mínimo

| Recurso | Mínimo | Recomendado |
|---------|--------|-------------|
| RAM | 8 GB | 16 GB |
| CPU | 2 núcleos | 4 núcleos |
| Disco libre | 10 GB | 20 GB |
| Internet | 10 Mbps | 50 Mbps |

### Software requerido

| Componente | Versión |
|------------|---------|
| Python | 3.12.1 |
| pip | 24.0 |
| virtualenv | 20.25.1 |
| Sistema Operativo | Ubuntu 22.04 LTS / macOS 13+ / Windows 11 con WSL2 |

### Convención de comandos

A lo largo de este laboratorio se muestran comandos para **Linux/macOS** como predeterminados. Cuando exista diferencia con Windows, se indicará explícitamente con la etiqueta `(Windows)`.

---

## Paso a Paso

### Paso 1: Verificar la instalación de Python y pip

**Objetivo:** Confirmar que Python 3.12.1 y pip 24.0 están disponibles en el sistema antes de crear el entorno del proyecto.

**Instrucciones:**

1. Abre una terminal (o PowerShell en Windows).

2. Verifica la versión de Python:

```bash
python3 --version
```

> **(Windows):** Si `python3` no es reconocido, usa `python --version`.

3. Verifica la versión de pip:

```bash
python3 -m pip --version
```

4. Si pip no está en la versión 24.0, actualízalo:

```bash
python3 -m pip install --upgrade pip==24.0
```

**Salida esperada:**

```
Python 3.12.1
pip 24.0 from /usr/lib/python3.12/site-packages/pip (python 3.12)
```

**Verificación:** Ambos comandos deben reportar las versiones exactas indicadas. Si Python muestra una versión diferente a 3.12.x, consulta la sección de Troubleshooting.

---

### Paso 2: Crear la estructura de directorios del proyecto

**Objetivo:** Establecer la jerarquía de carpetas estándar que será reutilizada en todos los laboratorios posteriores del curso.

**Instrucciones:**

1. Crea el directorio raíz del proyecto y navega a él:

```bash
mkdir -p ~/etl_course/labs/lab01
cd ~/etl_course/labs/lab01
```

> **(Windows PowerShell):**
> ```powershell
> New-Item -ItemType Directory -Force -Path "$HOME\etl_course\labs\lab01"
> Set-Location "$HOME\etl_course\labs\lab01"
> ```

2. Crea toda la estructura de subdirectorios:

```bash
mkdir -p src data/raw/batches data/processed config logs tests notebooks
```

> **(Windows PowerShell):**
> ```powershell
> "src","data/raw/batches","data/processed","config","logs","tests","notebooks" | ForEach-Object { New-Item -ItemType Directory -Force -Path $_ }
> ```

3. Verifica la estructura creada:

```bash
find . -type d | sort
```

> **(Windows PowerShell):** `Get-ChildItem -Recurse -Directory | Select-Object FullName`

**Salida esperada:**

```
.
./config
./data
./data/processed
./data/raw
./data/raw/batches
./logs
./notebooks
./src
./tests
```

**Verificación:** La estructura debe contener exactamente los 8 subdirectorios listados. Esta organización sigue la convención del curso y será extendida (nunca recreada) en laboratorios futuros.

---

### Paso 3: Crear y activar el entorno virtual

**Objetivo:** Aislar las dependencias del proyecto en un entorno virtual dedicado llamado `venv_etl`, evitando conflictos con paquetes del sistema.

**Instrucciones:**

1. Instala `virtualenv` en la versión exacta requerida (si no lo tienes):

```bash
python3 -m pip install virtualenv==20.25.1
```

2. Crea el entorno virtual dentro del directorio del proyecto:

```bash
python3 -m virtualenv venv_etl --python=python3.12
```

3. Activa el entorno virtual:

**Linux/macOS:**
```bash
source venv_etl/bin/activate
```

**Windows (PowerShell):**
```powershell
.\venv_etl\Scripts\Activate.ps1
```

**Windows (cmd):**
```cmd
.\venv_etl\Scripts\activate.bat
```

4. Confirma que el entorno está activo verificando la ruta del intérprete:

```bash
which python
```

> **(Windows):** `where python` o `Get-Command python`

**Salida esperada:**

```
(venv_etl) user@host:~/etl_course/labs/lab01$
```

El prompt debe mostrar el prefijo `(venv_etl)` y el comando `which python` debe apuntar a:

```
/home/<usuario>/etl_course/labs/lab01/venv_etl/bin/python
```

**Verificación:** Si el prefijo `(venv_etl)` aparece en el prompt, el entorno está correctamente activado.

---

### Paso 4: Instalar las dependencias del proyecto

**Objetivo:** Instalar las bibliotecas esenciales del curso con versiones fijadas y generar el archivo `requirements.txt` para reproducibilidad.

**Instrucciones:**

1. Asegúrate de estar en el directorio raíz del proyecto con el entorno activado:

```bash
cd ~/etl_course/labs/lab01
```

2. Crea el archivo `requirements.txt` con las dependencias exactas:

```bash
cat << 'EOF' > requirements.txt
pandas==2.2.1
numpy==1.26.4
requests==2.31.0
SQLAlchemy==2.0.29
openpyxl==3.1.2
pyarrow==15.0.2
lxml==5.1.0
EOF
```

> **(Windows PowerShell):**
> ```powershell
> @"
> pandas==2.2.1
> numpy==1.26.4
> requests==2.31.0
> SQLAlchemy==2.0.29
> openpyxl==3.1.2
> pyarrow==15.0.2
> lxml==5.1.0
> "@ | Set-Content -Path requirements.txt -Encoding UTF8
> ```

3. Instala todas las dependencias:

```bash
pip install -r requirements.txt
```

4. Verifica que las bibliotecas clave están instaladas correctamente:

```bash
python -c "import pandas; print(f'pandas: {pandas.__version__}')"
python -c "import requests; print(f'requests: {requests.__version__}')"
python -c "import sqlalchemy; print(f'SQLAlchemy: {sqlalchemy.__version__}')"
```

**Salida esperada:**

```
pandas: 2.2.1
requests: 2.31.0
SQLAlchemy: 2.0.29
```

**Verificación:** Las tres bibliotecas deben reportar exactamente las versiones especificadas. Si alguna versión difiere, revisa que el entorno virtual esté activado y reinstala con `pip install -r requirements.txt --force-reinstall`.

---

### Paso 5: Configurar archivos de proyecto (.gitignore y .env.example)

**Objetivo:** Crear los archivos de configuración que protegen credenciales y excluyen artefactos innecesarios del control de versiones.

**Instrucciones:**

1. Crea el archivo `.gitignore`:

```bash
cat << 'EOF' > .gitignore
# Entorno virtual
venv_etl/

# Variables de entorno
.env

# Cache de Python
__pycache__/
*.pyc
*.pyo

# Datos crudos y procesados (se regeneran con scripts)
data/raw/*.csv
data/raw/*.json
data/processed/

# Logs
logs/*.log

# IDE
.vscode/
.idea/
EOF
```

2. Crea el archivo `.env.example` como plantilla de referencia:

```bash
cat << 'EOF' > .env.example
# Credenciales PostgreSQL (Lab 03+)
DB_HOST=localhost
DB_PORT=5433
DB_NAME=etl_db
DB_USER=etl_user
DB_PASSWORD=
DB_SCHEMA=public

# API Keys (Lab 04+)
API_KEY=
API_SECRET=
EOF
```

3. Crea el archivo `.env` con las credenciales reales del curso:

```bash
cat << 'EOF' > .env
DB_HOST=localhost
DB_PORT=5433
DB_NAME=etl_db
DB_USER=etl_user
DB_PASSWORD=Etl$ecure2024!
DB_SCHEMA=public
EOF
```

4. Verifica que `.env` no será rastreado por git:

```bash
cat .gitignore | grep ".env"
```

**Salida esperada:**

```
.env
```

**Verificación:** El archivo `.env` contiene las credenciales reales y está listado en `.gitignore`. El archivo `.env.example` sirve como documentación para otros desarrolladores.

---

### Paso 6: Crear y ejecutar el script de extracción

**Objetivo:** Escribir un script Python que descargue el dataset Titanic desde una URL pública, lo guarde en `data/raw/titanic.csv` y genere un reporte de validación por consola.

**Instrucciones:**

1. Crea el archivo del script principal:

```bash
cat << 'SCRIPT' > src/lab01_extraction.py
"""
Lab 01 - Primer proceso de extracción de datos
================================================
Este script descarga el dataset Titanic desde un repositorio público,
lo persiste en data/raw/titanic.csv y genera un reporte de validación.

Fuente: Dataset Titanic (dominio público)
Destino: data/raw/titanic.csv
"""

import os
import sys
import requests
import pandas as pd
from pathlib import Path


def descargar_dataset(url: str, destino: Path) -> Path:
    """
    Descarga un archivo CSV desde una URL pública y lo guarda en disco.
    
    Args:
        url: URL del archivo CSV a descargar.
        destino: Ruta local donde se guardará el archivo.
    
    Returns:
        Path del archivo descargado.
    
    Raises:
        requests.exceptions.HTTPError: Si la respuesta HTTP indica error.
        requests.exceptions.ConnectionError: Si no hay conexión a Internet.
    """
    print(f"[INFO] Descargando dataset desde: {url}")
    
    respuesta = requests.get(url, timeout=30)
    respuesta.raise_for_status()
    
    # Crear directorio destino si no existe
    destino.parent.mkdir(parents=True, exist_ok=True)
    
    # Escribir contenido en disco
    destino.write_bytes(respuesta.content)
    
    tamano_kb = len(respuesta.content) / 1024
    print(f"[OK] Archivo guardado en: {destino}")
    print(f"[OK] Tamaño: {tamano_kb:.1f} KB")
    
    return destino


def generar_reporte_validacion(df: pd.DataFrame) -> None:
    """
    Imprime un reporte de validación básico del DataFrame.
    
    Incluye: dimensiones, tipos de datos, valores nulos por columna
    y primeras 5 filas como muestra.
    """
    print("\n" + "=" * 60)
    print("REPORTE DE VALIDACIÓN - Dataset Titanic")
    print("=" * 60)
    
    # Dimensiones
    filas, columnas = df.shape
    print(f"\n📊 Dimensiones: {filas} filas × {columnas} columnas")
    
    # Tipos de datos
    print(f"\n📋 Tipos de datos por columna:")
    print("-" * 40)
    for col in df.columns:
        print(f"  {col:<20} {str(df[col].dtype):<15}")
    
    # Valores nulos
    nulos = df.isnull().sum()
    total_nulos = nulos.sum()
    print(f"\n⚠️  Valores nulos totales: {total_nulos}")
    print("-" * 40)
    for col, count in nulos.items():
        if count > 0:
            porcentaje = (count / filas) * 100
            print(f"  {col:<20} {count:>5} ({porcentaje:.1f}%)")
    
    # Muestra de datos
    print(f"\n🔍 Primeras 5 filas:")
    print("-" * 40)
    print(df.head().to_string(index=False))
    
    print("\n" + "=" * 60)
    print("[OK] Reporte de validación completado exitosamente.")
    print("=" * 60)


def main():
    """Función principal del script de extracción."""
    
    # Configuración de rutas
    # El script se ejecuta desde la raíz del proyecto: ~/etl_course/labs/lab01/
    raiz_proyecto = Path(__file__).resolve().parent.parent
    ruta_destino = raiz_proyecto / "data" / "raw" / "titanic.csv"
    
    # URL del dataset Titanic (fuente pública en GitHub)
    url_dataset = (
        "https://raw.githubusercontent.com/datasciencedojo/"
        "datasets/master/titanic.csv"
    )
    
    try:
        # Paso 1: Descargar el dataset
        archivo = descargar_dataset(url_dataset, ruta_destino)
        
        # Paso 2: Cargar con pandas
        print(f"\n[INFO] Cargando dataset con pandas...")
        df = pd.read_csv(archivo)
        print(f"[OK] Dataset cargado correctamente.")
        
        # Paso 3: Generar reporte de validación
        generar_reporte_validacion(df)
        
        # Paso 4: Confirmar persistencia
        print(f"\n[INFO] Archivo persistido en: {ruta_destino}")
        print(f"[INFO] Proceso de extracción finalizado con éxito.")
        
    except requests.exceptions.ConnectionError:
        print("[ERROR] No se pudo conectar a Internet. Verifica tu conexión.")
        sys.exit(1)
    except requests.exceptions.HTTPError as e:
        print(f"[ERROR] Error HTTP al descargar: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"[ERROR] Error inesperado: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
SCRIPT
```

2. Ejecuta el script desde la raíz del proyecto:

```bash
cd ~/etl_course/labs/lab01
python src/lab01_extraction.py
```

**Salida esperada:**

```
[INFO] Descargando dataset desde: https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv
[OK] Archivo guardado en: /home/<usuario>/etl_course/labs/lab01/data/raw/titanic.csv
[OK] Tamaño: 60.3 KB

[INFO] Cargando dataset con pandas...
[OK] Dataset cargado correctamente.

============================================================
REPORTE DE VALIDACIÓN - Dataset Titanic
============================================================

📊 Dimensiones: 891 filas × 12 columnas

📋 Tipos de datos por columna:
----------------------------------------
  PassengerId          int64          
  Survived             int64          
  Pclass               int64          
  Name                 object         
  Sex                  object         
  Age                  float64        
  SibSp                int64          
  Parch                int64          
  Ticket               object         
  Fare                 float64        
  Cabin                object         
  Embarked             object         

⚠️  Valores nulos totales: 866
----------------------------------------
  Age                    177 (19.9%)
  Cabin                  687 (77.1%)
  Embarked                 2 (0.2%)

🔍 Primeras 5 filas:
----------------------------------------
...

============================================================
[OK] Reporte de validación completado exitosamente.
============================================================

[INFO] Archivo persistido en: /home/<usuario>/etl_course/labs/lab01/data/raw/titanic.csv
[INFO] Proceso de extracción finalizado con éxito.
```

**Verificación:** El script debe completarse sin errores, el archivo `data/raw/titanic.csv` debe existir y el reporte debe mostrar 891 filas × 12 columnas.

---

### Paso 7: Verificar la integridad del archivo descargado

**Objetivo:** Confirmar que el archivo CSV se descargó correctamente y es legible por pandas de forma independiente al script.

**Instrucciones:**

1. Verifica que el archivo existe y tiene contenido:

```bash
ls -la data/raw/titanic.csv
```

2. Cuenta las líneas del archivo (encabezado + 891 registros = 892 líneas):

```bash
wc -l data/raw/titanic.csv
```

3. Ejecuta una verificación rápida con Python interactivo:

```bash
python -c "
import pandas as pd
df = pd.read_csv('data/raw/titanic.csv')
assert df.shape == (891, 12), f'Shape inesperado: {df.shape}'
assert 'PassengerId' in df.columns, 'Columna PassengerId no encontrada'
assert df['Survived'].isin([0, 1]).all(), 'Valores inválidos en Survived'
print('[OK] Todas las verificaciones pasaron correctamente.')
print(f'     Shape: {df.shape}')
print(f'     Columnas: {list(df.columns)}')
"
```

**Salida esperada:**

```
892 data/raw/titanic.csv
[OK] Todas las verificaciones pasaron correctamente.
     Shape: (891, 12)
     Columnas: ['PassengerId', 'Survived', 'Pclass', 'Name', 'Sex', 'Age', 'SibSp', 'Parch', 'Ticket', 'Fare', 'Cabin', 'Embarked']
```

**Verificación:** El archivo debe tener exactamente 892 líneas y las tres aserciones deben pasar sin error.

---

## Validación y Testing

Ejecuta la siguiente secuencia completa de validación para confirmar que todo el laboratorio se completó correctamente:

```bash
cd ~/etl_course/labs/lab01

echo "=== 1. Verificando estructura de directorios ==="
for dir in src data/raw data/raw/batches data/processed config logs tests notebooks; do
    if [ -d "$dir" ]; then
        echo "  [OK] $dir/"
    else
        echo "  [FAIL] $dir/ NO EXISTE"
    fi
done

echo ""
echo "=== 2. Verificando entorno virtual ==="
if [ -d "venv_etl" ]; then
    echo "  [OK] venv_etl/ existe"
else
    echo "  [FAIL] venv_etl/ NO EXISTE"
fi

echo ""
echo "=== 3. Verificando dependencias instaladas ==="
python -c "
import pandas, requests, sqlalchemy, numpy, openpyxl, pyarrow, lxml
pkgs = {
    'pandas': (pandas.__version__, '2.2.1'),
    'requests': (requests.__version__, '2.31.0'),
    'SQLAlchemy': (sqlalchemy.__version__, '2.0.29'),
    'numpy': (numpy.__version__, '1.26.4'),
}
for name, (actual, expected) in pkgs.items():
    status = '[OK]' if actual == expected else '[WARN]'
    print(f'  {status} {name}: {actual} (esperado: {expected})')
"

echo ""
echo "=== 4. Verificando archivos de configuración ==="
for file in requirements.txt .gitignore .env .env.example; do
    if [ -f "$file" ]; then
        echo "  [OK] $file"
    else
        echo "  [FAIL] $file NO EXISTE"
    fi
done

echo ""
echo "=== 5. Verificando dataset descargado ==="
if [ -f "data/raw/titanic.csv" ]; then
    lines=$(wc -l < data/raw/titanic.csv)
    echo "  [OK] titanic.csv existe ($lines líneas)"
else
    echo "  [FAIL] titanic.csv NO EXISTE"
fi

echo ""
echo "=== 6. Verificando script de extracción ==="
if [ -f "src/lab01_extraction.py" ]; then
    echo "  [OK] src/lab01_extraction.py existe"
else
    echo "  [FAIL] src/lab01_extraction.py NO EXISTE"
fi

echo ""
echo "=== VALIDACIÓN COMPLETA ==="
```

**Resultado esperado:** Todos los ítems deben mostrar `[OK]`.

---

## Troubleshooting

### Problema 1: Error de conexión al descargar el dataset

**Síntomas:**
```
[ERROR] No se pudo conectar a Internet. Verifica tu conexión.
requests.exceptions.ConnectionError: HTTPSConnectionPool(host='raw.githubusercontent.com', port=443): Max retries exceeded
```

**Causa:** La máquina no tiene acceso a Internet, el firewall corporativo bloquea conexiones a `raw.githubusercontent.com`, o hay un proxy configurado que no se está utilizando.

**Solución:**

1. Verifica conectividad básica:
```bash
ping -c 3 raw.githubusercontent.com
```

2. Si estás detrás de un proxy, configura las variables de entorno antes de ejecutar el script:
```bash
export HTTP_PROXY=http://proxy.empresa.com:8080
export HTTPS_PROXY=http://proxy.empresa.com:8080
```

3. Como alternativa temporal, descarga el archivo manualmente desde un navegador y colócalo en `data/raw/titanic.csv`:
```bash
# Descarga manual alternativa con curl
curl -L -o data/raw/titanic.csv "https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv"
```

---

### Problema 2: `python3` no reconocido o versión incorrecta en Windows

**Síntomas:**
```
'python3' is not recognized as an internal or external command
```
O bien:
```
Python 3.11.x  (versión diferente a 3.12.1)
```

**Causa:** En Windows, Python se instala como `python` (no `python3`). Si hay múltiples versiones instaladas, el PATH puede apuntar a una versión diferente.

**Solución:**

1. En Windows, usa `python` en lugar de `python3`:
```powershell
python --version
```

2. Si la versión es incorrecta, usa el Python Launcher para seleccionar la versión específica:
```powershell
py -3.12 --version
py -3.12 -m virtualenv venv_etl
```

3. Verifica las versiones instaladas:
```powershell
py --list
```

4. Si Python 3.12.1 no aparece, descárgalo desde [python.org](https://www.python.org/downloads/release/python-3121/) y durante la instalación marca la opción **"Add Python to PATH"**.

---

## Limpieza

Este laboratorio **no requiere limpieza** ya que la estructura creada y el dataset descargado serán utilizados directamente en el Laboratorio 02 (conversión entre formatos de archivo). El entorno virtual `venv_etl` se mantendrá activo y se extenderá con nuevas dependencias en laboratorios futuros.

Si necesitas desactivar el entorno virtual temporalmente:

```bash
deactivate
```

Para reactivarlo en sesiones futuras:

```bash
cd ~/etl_course/labs/lab01
source venv_etl/bin/activate  # Linux/macOS
# .\venv_etl\Scripts\Activate.ps1  # Windows PowerShell
```

---

## Resumen

En este laboratorio has completado con éxito las siguientes tareas fundamentales:

| Tarea | Resultado |
|-------|-----------|
| Verificación del entorno base | Python 3.12.1 + pip 24.0 confirmados |
| Estructura de directorios | 8 subdirectorios creados bajo `~/etl_course/labs/lab01/` |
| Entorno virtual | `venv_etl` creado con virtualenv 20.25.1 |
| Dependencias | 7 bibliotecas instaladas con versiones fijadas |
| Configuración | `.gitignore`, `.env` y `.env.example` creados |
| Script de extracción | `src/lab01_extraction.py` ejecutado exitosamente |
| Dataset | `data/raw/titanic.csv` — 891 filas × 12 columnas |

Has aplicado en la práctica los conceptos de la Lección 1.1: extrajiste datos desde una fuente pública (archivo CSV en un repositorio), los persististe localmente y validaste su integridad. Este es el primer paso del ciclo ETL: la **extracción** confiable y automatizada de datos.

### Conexión con el siguiente laboratorio

En el **Lab 02**, utilizarás el archivo `data/raw/titanic.csv` generado aquí para convertirlo a múltiples formatos (Excel, JSON, NDJSON, XML, Parquet), practicando la lectura y escritura entre formatos con pandas y bibliotecas especializadas.

### Recursos adicionales

- [Documentación oficial de virtualenv](https://virtualenv.pypa.io/en/latest/)
- [pandas — Guía de lectura de CSV](https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html)
- [requests — Quickstart](https://requests.readthedocs.io/en/latest/user/quickstart/)
- [Real Python — Python Virtual Environments](https://realpython.com/python-virtual-environments-a-primer/)
