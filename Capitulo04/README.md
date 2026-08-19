# Consumo de una API pública

## Metadata

| Campo | Valor |
|-------|-------|
| **Duración** | 60 minutos |
| **Complejidad** | Media |
| **Nivel Bloom** | Aplicar |

## Descripción general

En este laboratorio consumirás dos APIs REST públicas — Open-Meteo (sin autenticación) y GitHub (con Personal Access Token) — para extraer datos meteorológicos históricos y metadatos de repositorios. Implementarás un cliente HTTP robusto con reintentos exponenciales, paginación y manejo de errores, normalizando las respuestas JSON en DataFrames de pandas listos para análisis posterior.

## Objetivos de aprendizaje

- [ ] Consumir la API de Open-Meteo para extraer datos meteorológicos históricos y la API de GitHub con autenticación por token para listar repositorios de una organización.
- [ ] Implementar manejo de errores HTTP (4xx/5xx), paginación con parámetros `page`/`per_page` y reintentos exponenciales con `tenacity`.
- [ ] Normalizar respuestas JSON anidadas en DataFrames planos con `pandas.json_normalize` y persistir los resultados en archivos JSON.
- [ ] Generar un reporte comparativo de la estructura de datos obtenida de ambas APIs.

## Prerrequisitos

### Conocimientos requeridos

- Laboratorio 3 completado (archivo `.env` existente con credenciales de PostgreSQL)
- Conceptos básicos de HTTP: métodos GET, códigos de estado, headers (Lección 4.1)
- Manejo de archivos JSON y DataFrames de pandas (Laboratorio 2)
- Uso de entornos virtuales Python

### Accesos necesarios

- Cuenta de GitHub con **Personal Access Token** (PAT) generado con scope `public_repo`
- Acceso a Internet estable para consumir APIs públicas
- Entorno virtual `venv_etl` activo

> **Cómo generar un PAT de GitHub:** Ve a GitHub → Settings → Developer settings → Personal access tokens → Tokens (classic) → Generate new token. Selecciona el scope `public_repo` y copia el token generado.

## Entorno del laboratorio

### Software requerido

| Componente | Versión |
|------------|---------|
| Python | 3.12.1 |
| pandas | 2.2.1 |
| requests | 2.31.0 |
| httpx | 0.27.0 |
| tenacity | 8.2.3 |
| python-dotenv | 1.0.1 |

### Estructura de archivos resultante

```
~/etl_course/labs/lab01/
├── .env                          # Variables de entorno (extendido)
├── src/
│   ├── lab04_api_extraction.py   # Script principal
│   ├── api_client.py             # Cliente base HTTP
│   ├── extractor_weather.py      # Extractor Open-Meteo
│   └── extractor_github.py       # Extractor GitHub paginado
├── data/
│   ├── raw/
│   │   ├── api_weather.json      # Datos meteorológicos
│   │   └── api_github_repos.json # Repositorios GitHub
│   └── processed/
└── logs/
    └── api_extraction.log        # Log de ejecución
```

---

## Paso 1: Preparar el entorno y las dependencias

**Objetivo:** Instalar las nuevas bibliotecas (`httpx`, `tenacity`) en el entorno virtual existente y configurar las variables de entorno necesarias.

### Instrucciones

1. Navega al directorio raíz del proyecto y activa el entorno virtual:

```bash
cd ~/etl_course/labs/lab01
source venv_etl/bin/activate  # Linux/macOS
# Windows: .\venv_etl\Scripts\activate
```

2. Instala las nuevas dependencias:

```bash
pip install httpx==0.27.0 tenacity==8.2.3
```

3. Verifica la instalación:

```bash
pip show httpx tenacity | grep -E "^(Name|Version)"
```

4. Agrega las nuevas variables al archivo `.env`:

```bash
cat >> .env << 'EOF'

# --- Lab 04: APIs ---
GITHUB_TOKEN=ghp_TU_TOKEN_AQUI
OPEN_METEO_BASE_URL=https://archive-api.open-meteo.com/v1/archive
EOF
```

5. Reemplaza `ghp_TU_TOKEN_AQUI` con tu Personal Access Token real:

```bash
# Usa sed o edita manualmente el archivo .env
nano .env
```

6. Actualiza el archivo `.env.example` como plantilla:

```bash
cat >> .env.example << 'EOF'

# --- Lab 04: APIs ---
GITHUB_TOKEN=
OPEN_METEO_BASE_URL=https://archive-api.open-meteo.com/v1/archive
EOF
```

### Salida esperada

```
Name: httpx
Version: 0.27.0
Name: tenacity
Version: 8.2.3
```

### Verificación

```bash
python -c "import httpx; import tenacity; print('OK: httpx', httpx.__version__, '| tenacity', tenacity.__version__)"
```

---

## Paso 2: Crear el cliente base HTTP

**Objetivo:** Implementar un módulo reutilizable que encapsule la configuración de `httpx` con timeout, logging, headers de autenticación y reintentos exponenciales.

### Instrucciones

1. Crea el archivo `src/api_client.py`:

```python
"""
Módulo: api_client.py
Cliente HTTP base con httpx, reintentos exponenciales y logging.
"""

import logging
import httpx
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

# Configuración del logger
logger = logging.getLogger(__name__)


class APIClientError(Exception):
    """Excepción personalizada para errores del cliente API."""

    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        self.message = message
        super().__init__(f"HTTP {status_code}: {message}")


class RateLimitError(APIClientError):
    """Excepción específica para errores 429 (Too Many Requests)."""
    pass


class ServerError(APIClientError):
    """Excepción específica para errores 5xx."""
    pass


class BaseAPIClient:
    """
    Cliente HTTP base con:
    - Timeout configurable (30s por defecto)
    - Headers personalizados
    - Logging de requests/responses
    - Reintentos exponenciales para errores transitorios
    """

    def __init__(
        self,
        base_url: str,
        headers: dict | None = None,
        timeout: float = 30.0,
    ):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.default_headers = {
            "Accept": "application/json",
            "User-Agent": "ETLCourse-Lab04/1.0",
        }
        if headers:
            self.default_headers.update(headers)

        self.client = httpx.Client(
            base_url=self.base_url,
            headers=self.default_headers,
            timeout=self.timeout,
        )
        logger.info(
            "Cliente inicializado: base_url=%s, timeout=%s",
            self.base_url,
            self.timeout,
        )

    @retry(
        retry=retry_if_exception_type((RateLimitError, ServerError)),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        stop=stop_after_attempt(3),
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    def get(self, endpoint: str, params: dict | None = None) -> dict | list:
        """
        Realiza una solicitud GET con reintentos exponenciales.

        Args:
            endpoint: Ruta relativa al base_url (e.g., '/v1/forecast')
            params: Parámetros de consulta

        Returns:
            Respuesta deserializada como dict o list

        Raises:
            RateLimitError: Si el servidor responde con 429
            ServerError: Si el servidor responde con 5xx
            APIClientError: Para otros errores HTTP (4xx)
        """
        url = endpoint if endpoint.startswith("http") else endpoint
        logger.info("GET %s/%s params=%s", self.base_url, endpoint, params)

        response = self.client.get(endpoint, params=params)

        # Log de la respuesta
        logger.info(
            "Respuesta: status=%d, content-length=%s",
            response.status_code,
            response.headers.get("content-length", "N/A"),
        )

        # Manejo de códigos de estado
        if response.status_code == 429:
            retry_after = response.headers.get("Retry-After", "desconocido")
            logger.warning("Rate limit alcanzado. Retry-After: %s", retry_after)
            raise RateLimitError(429, f"Rate limit excedido. Retry-After: {retry_after}")

        if 500 <= response.status_code < 600:
            logger.error("Error del servidor: %d", response.status_code)
            raise ServerError(response.status_code, response.text[:200])

        if 400 <= response.status_code < 500:
            logger.error(
                "Error del cliente: %d - %s",
                response.status_code,
                response.text[:200],
            )
            raise APIClientError(response.status_code, response.text[:200])

        return response.json()

    def close(self):
        """Cierra la conexión del cliente HTTP."""
        self.client.close()
        logger.info("Cliente cerrado.")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
```

### Salida esperada

El archivo se crea sin errores de sintaxis.

### Verificación

```bash
python -c "from src.api_client import BaseAPIClient; print('Módulo api_client importado correctamente')"
```

---

## Paso 3: Implementar el extractor de Open-Meteo

**Objetivo:** Crear un módulo que solicite datos de temperatura y precipitación para Madrid durante 2023 usando la API de archivo de Open-Meteo.

### Instrucciones

1. Crea el archivo `src/extractor_weather.py`:

```python
"""
Módulo: extractor_weather.py
Extractor de datos meteorológicos históricos de Open-Meteo.
"""

import logging
from src.api_client import BaseAPIClient

logger = logging.getLogger(__name__)

# Coordenadas predefinidas: Madrid, España
MADRID_LAT = 40.4168
MADRID_LON = -3.7038

# Rango de fechas: año 2023 completo
START_DATE = "2023-01-01"
END_DATE = "2023-12-31"

# Variables meteorológicas a extraer
DAILY_VARIABLES = "temperature_2m_max,temperature_2m_min,precipitation_sum"


def extract_weather_data(base_url: str) -> dict:
    """
    Extrae datos meteorológicos diarios de Open-Meteo para Madrid (2023).

    Args:
        base_url: URL base de la API de archivo de Open-Meteo

    Returns:
        dict con la respuesta completa de la API (incluye metadata y datos)
    """
    logger.info(
        "Iniciando extracción meteorológica: lat=%s, lon=%s, rango=%s a %s",
        MADRID_LAT,
        MADRID_LON,
        START_DATE,
        END_DATE,
    )

    params = {
        "latitude": MADRID_LAT,
        "longitude": MADRID_LON,
        "start_date": START_DATE,
        "end_date": END_DATE,
        "daily": DAILY_VARIABLES,
        "timezone": "Europe/Madrid",
    }

    with BaseAPIClient(base_url=base_url) as client:
        data = client.get("", params=params)

    # Validar que la respuesta contiene datos diarios
    if "daily" not in data:
        raise ValueError("La respuesta de Open-Meteo no contiene la clave 'daily'")

    num_registros = len(data["daily"].get("time", []))
    logger.info("Datos meteorológicos extraídos: %d registros diarios", num_registros)

    return data


def get_weather_metadata(data: dict) -> dict:
    """
    Extrae metadatos de la respuesta de Open-Meteo.

    Args:
        data: Respuesta completa de la API

    Returns:
        dict con información de localización y unidades
    """
    return {
        "latitude": data.get("latitude"),
        "longitude": data.get("longitude"),
        "elevation": data.get("elevation"),
        "timezone": data.get("timezone"),
        "timezone_abbreviation": data.get("timezone_abbreviation"),
        "daily_units": data.get("daily_units", {}),
        "total_records": len(data.get("daily", {}).get("time", [])),
    }
```

### Salida esperada

El módulo se importa sin errores.

### Verificación

```bash
python -c "from src.extractor_weather import extract_weather_data, MADRID_LAT; print(f'Coordenadas Madrid: lat={MADRID_LAT}')"
```

---

## Paso 4: Implementar el extractor paginado de GitHub

**Objetivo:** Crear un módulo que liste repositorios de una organización pública de GitHub iterando páginas hasta agotar resultados o alcanzar 500 registros.

### Instrucciones

1. Crea el archivo `src/extractor_github.py`:

```python
"""
Módulo: extractor_github.py
Extractor paginado de repositorios de GitHub con autenticación por token.
"""

import logging
from src.api_client import BaseAPIClient, APIClientError

logger = logging.getLogger(__name__)

# Configuración de paginación
PER_PAGE = 30  # Repositorios por página (máximo GitHub: 100)
MAX_REPOS = 500  # Límite máximo de repositorios a extraer
GITHUB_BASE_URL = "https://api.github.com"
DEFAULT_ORG = "python"  # Organización pública de ejemplo


def extract_github_repos(
    token: str,
    org: str = DEFAULT_ORG,
    max_repos: int = MAX_REPOS,
    per_page: int = PER_PAGE,
) -> list[dict]:
    """
    Extrae repositorios públicos de una organización de GitHub con paginación.

    Args:
        token: Personal Access Token de GitHub
        org: Nombre de la organización (default: 'python')
        max_repos: Número máximo de repositorios a extraer
        per_page: Repositorios por página

    Returns:
        Lista de diccionarios con datos de cada repositorio
    """
    logger.info(
        "Iniciando extracción de repos GitHub: org=%s, max=%d, per_page=%d",
        org,
        max_repos,
        per_page,
    )

    headers = {
        "Authorization": f"Bearer {token}",
        "X-GitHub-Api-Version": "2022-11-28",
    }

    all_repos = []
    page = 1

    with BaseAPIClient(base_url=GITHUB_BASE_URL, headers=headers) as client:
        while len(all_repos) < max_repos:
            params = {
                "page": page,
                "per_page": per_page,
                "type": "public",
                "sort": "updated",
            }

            logger.info("Solicitando página %d...", page)

            try:
                repos_page = client.get(f"/orgs/{org}/repos", params=params)
            except APIClientError as e:
                logger.error(
                    "Error al obtener página %d: %s", page, e.message
                )
                break

            # Si la página está vacía, no hay más resultados
            if not repos_page:
                logger.info("Página %d vacía. Fin de la paginación.", page)
                break

            all_repos.extend(repos_page)
            logger.info(
                "Página %d: %d repos obtenidos (total acumulado: %d)",
                page,
                len(repos_page),
                len(all_repos),
            )

            # Si la página devolvió menos de per_page, es la última
            if len(repos_page) < per_page:
                logger.info("Última página alcanzada (menos de %d resultados).", per_page)
                break

            page += 1

    # Truncar al máximo si se excedió
    all_repos = all_repos[:max_repos]
    logger.info("Extracción completada: %d repositorios totales.", len(all_repos))

    return all_repos


def extract_repo_fields(repos: list[dict]) -> list[dict]:
    """
    Extrae campos relevantes de cada repositorio para simplificar el dataset.

    Args:
        repos: Lista completa de repositorios (respuesta cruda de GitHub)

    Returns:
        Lista de diccionarios con campos seleccionados
    """
    fields = []
    for repo in repos:
        fields.append({
            "id": repo.get("id"),
            "name": repo.get("name"),
            "full_name": repo.get("full_name"),
            "description": repo.get("description"),
            "html_url": repo.get("html_url"),
            "language": repo.get("language"),
            "stargazers_count": repo.get("stargazers_count"),
            "forks_count": repo.get("forks_count"),
            "open_issues_count": repo.get("open_issues_count"),
            "created_at": repo.get("created_at"),
            "updated_at": repo.get("updated_at"),
            "pushed_at": repo.get("pushed_at"),
            "size": repo.get("size"),
            "default_branch": repo.get("default_branch"),
            "license_name": (repo.get("license") or {}).get("name"),
            "owner_login": (repo.get("owner") or {}).get("login"),
        })
    return fields
```

### Salida esperada

El módulo se importa correctamente.

### Verificación

```bash
python -c "from src.extractor_github import extract_github_repos, DEFAULT_ORG; print(f'Organización por defecto: {DEFAULT_ORG}')"
```

---

## Paso 5: Crear el script principal de orquestación

**Objetivo:** Integrar ambos extractores en un script principal que cargue configuración desde `.env`, ejecute las extracciones, normalice los datos en DataFrames y genere un reporte comparativo.

### Instrucciones

1. Crea el archivo `src/lab04_api_extraction.py`:

```python
"""
Script principal: lab04_api_extraction.py
Orquesta la extracción de datos desde Open-Meteo y GitHub,
normaliza los resultados y genera un reporte comparativo.
"""

import json
import logging
import sys
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
import os

# Asegurar que el directorio raíz esté en el path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.extractor_weather import extract_weather_data, get_weather_metadata
from src.extractor_github import extract_github_repos, extract_repo_fields

# --- Configuración de logging ---
LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "api_extraction.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# --- Rutas de salida ---
DATA_RAW = PROJECT_ROOT / "data" / "raw"
DATA_RAW.mkdir(parents=True, exist_ok=True)

WEATHER_OUTPUT = DATA_RAW / "api_weather.json"
GITHUB_OUTPUT = DATA_RAW / "api_github_repos.json"


def load_config() -> dict:
    """Carga variables de entorno desde .env"""
    env_path = PROJECT_ROOT / ".env"
    load_dotenv(env_path)

    config = {
        "github_token": os.getenv("GITHUB_TOKEN"),
        "open_meteo_base_url": os.getenv("OPEN_METEO_BASE_URL"),
    }

    # Validar que las variables requeridas estén presentes
    if not config["github_token"] or config["github_token"] == "ghp_TU_TOKEN_AQUI":
        logger.warning(
            "GITHUB_TOKEN no configurado. La extracción de GitHub será omitida."
        )
        config["github_token"] = None

    if not config["open_meteo_base_url"]:
        config["open_meteo_base_url"] = "https://archive-api.open-meteo.com/v1/archive"

    return config


def normalize_weather_to_dataframe(data: dict) -> pd.DataFrame:
    """
    Normaliza la respuesta JSON anidada de Open-Meteo en un DataFrame plano.

    La estructura de Open-Meteo tiene los datos diarios como listas paralelas:
    {"daily": {"time": [...], "temperature_2m_max": [...], ...}}
    """
    daily = data.get("daily", {})
    df = pd.DataFrame(daily)
    df.rename(columns={"time": "date"}, inplace=True)
    df["date"] = pd.to_datetime(df["date"])

    # Agregar metadatos como columnas constantes
    df["latitude"] = data.get("latitude")
    df["longitude"] = data.get("longitude")
    df["timezone"] = data.get("timezone")

    logger.info("DataFrame meteorológico: %d filas x %d columnas", *df.shape)
    return df


def normalize_github_to_dataframe(repos: list[dict]) -> pd.DataFrame:
    """
    Normaliza la lista de repositorios de GitHub en un DataFrame plano
    usando pandas json_normalize para manejar campos anidados.
    """
    df = pd.json_normalize(repos, sep="_")

    # Convertir columnas de fecha
    date_cols = ["created_at", "updated_at", "pushed_at"]
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], utc=True)

    logger.info("DataFrame GitHub: %d filas x %d columnas", *df.shape)
    return df


def generate_comparison_report(df_weather: pd.DataFrame, df_github: pd.DataFrame):
    """Genera un reporte comparativo de estructura de ambos datasets."""
    report = []
    report.append("=" * 70)
    report.append("REPORTE COMPARATIVO DE ESTRUCTURA DE DATOS")
    report.append("=" * 70)

    report.append("\n--- Open-Meteo (Datos Meteorológicos) ---")
    report.append(f"  Registros: {len(df_weather)}")
    report.append(f"  Columnas:  {len(df_weather.columns)}")
    report.append(f"  Columnas:  {list(df_weather.columns)}")
    report.append(f"  Tipos de datos:")
    for col, dtype in df_weather.dtypes.items():
        report.append(f"    - {col}: {dtype}")
    report.append(f"  Valores nulos: {df_weather.isnull().sum().sum()}")
    report.append(f"  Rango de fechas: {df_weather['date'].min()} a {df_weather['date'].max()}")

    report.append("\n--- GitHub (Repositorios) ---")
    report.append(f"  Registros: {len(df_github)}")
    report.append(f"  Columnas:  {len(df_github.columns)}")
    report.append(f"  Primeras 10 columnas: {list(df_github.columns[:10])}")
    report.append(f"  Tipos de datos (primeras 10):")
    for col, dtype in list(df_github.dtypes.items())[:10]:
        report.append(f"    - {col}: {dtype}")
    report.append(f"  Valores nulos totales: {df_github.isnull().sum().sum()}")

    report.append("\n--- Comparación ---")
    report.append(f"  {'Métrica':<30} {'Open-Meteo':<15} {'GitHub':<15}")
    report.append(f"  {'-'*30} {'-'*15} {'-'*15}")
    report.append(f"  {'Filas':<30} {len(df_weather):<15} {len(df_github):<15}")
    report.append(f"  {'Columnas':<30} {len(df_weather.columns):<15} {len(df_github.columns):<15}")
    report.append(f"  {'Nulos totales':<30} {df_weather.isnull().sum().sum():<15} {df_github.isnull().sum().sum():<15}")
    report.append(f"  {'Memoria (KB)':<30} {df_weather.memory_usage(deep=True).sum()//1024:<15} {df_github.memory_usage(deep=True).sum()//1024:<15}")
    report.append("=" * 70)

    report_text = "\n".join(report)
    print(report_text)
    logger.info("Reporte comparativo generado.")
    return report_text


def main():
    """Función principal de orquestación."""
    logger.info("=" * 50)
    logger.info("INICIO: Lab 04 - Extracción de APIs")
    logger.info("=" * 50)

    config = load_config()

    # --- Extracción 1: Open-Meteo ---
    logger.info("--- Fase 1: Extracción Open-Meteo ---")
    try:
        weather_data = extract_weather_data(config["open_meteo_base_url"])
        weather_metadata = get_weather_metadata(weather_data)
        logger.info("Metadata meteorológica: %s", weather_metadata)

        # Guardar JSON crudo
        with open(WEATHER_OUTPUT, "w", encoding="utf-8") as f:
            json.dump(weather_data, f, ensure_ascii=False, indent=2)
        logger.info("Datos guardados en: %s", WEATHER_OUTPUT)

        # Normalizar a DataFrame
        df_weather = normalize_weather_to_dataframe(weather_data)
        print("\n--- Vista previa: Datos meteorológicos ---")
        print(df_weather.head())
        print(f"\nEstadísticas descriptivas:")
        print(df_weather.describe())

    except Exception as e:
        logger.error("Error en extracción Open-Meteo: %s", e)
        df_weather = pd.DataFrame()

    # --- Extracción 2: GitHub ---
    logger.info("\n--- Fase 2: Extracción GitHub ---")
    df_github = pd.DataFrame()

    if config["github_token"]:
        try:
            raw_repos = extract_github_repos(
                token=config["github_token"],
                org="python",
                max_repos=500,
                per_page=30,
            )

            # Extraer campos relevantes
            repos_clean = extract_repo_fields(raw_repos)

            # Guardar JSON
            with open(GITHUB_OUTPUT, "w", encoding="utf-8") as f:
                json.dump(repos_clean, f, ensure_ascii=False, indent=2)
            logger.info("Datos guardados en: %s", GITHUB_OUTPUT)

            # Normalizar a DataFrame
            df_github = normalize_github_to_dataframe(repos_clean)
            print("\n--- Vista previa: Repositorios GitHub ---")
            print(df_github[["name", "language", "stargazers_count", "updated_at"]].head(10))

        except Exception as e:
            logger.error("Error en extracción GitHub: %s", e)
    else:
        logger.warning("Extracción de GitHub omitida (token no configurado).")

    # --- Fase 3: Reporte comparativo ---
    logger.info("\n--- Fase 3: Reporte comparativo ---")
    if not df_weather.empty and not df_github.empty:
        generate_comparison_report(df_weather, df_github)
    elif not df_weather.empty:
        print("\nSolo se obtuvieron datos meteorológicos.")
        print(f"  Shape: {df_weather.shape}")
    else:
        logger.warning("No se obtuvieron datos suficientes para el reporte.")

    logger.info("FINALIZADO: Lab 04 - Extracción de APIs")


if __name__ == "__main__":
    main()
```

2. Crea un archivo `src/__init__.py` vacío si no existe (necesario para importaciones):

```bash
touch src/__init__.py
```

### Salida esperada

El script se crea sin errores de sintaxis.

### Verificación

```bash
python -c "import ast; ast.parse(open('src/lab04_api_extraction.py').read()); print('Sintaxis válida')"
```

---

## Paso 6: Ejecutar la extracción completa

**Objetivo:** Ejecutar el script principal y verificar que ambas APIs responden correctamente, los datos se persisten en disco y los DataFrames se generan con la estructura esperada.

### Instrucciones

1. Asegúrate de estar en el directorio raíz del proyecto con el entorno activado:

```bash
cd ~/etl_course/labs/lab01
source venv_etl/bin/activate
```

2. Ejecuta el script principal:

```bash
python -m src.lab04_api_extraction
```

> **Nota:** Si usas `python -m`, Python trata `src` como paquete. Alternativamente puedes ejecutar `python src/lab04_api_extraction.py`.

3. Observa la salida en consola. Deberías ver:
   - Logs de conexión a Open-Meteo
   - 365 registros meteorológicos extraídos
   - Paginación de repositorios GitHub (varias páginas)
   - Reporte comparativo al final

4. Verifica que los archivos de salida existen:

```bash
ls -lh data/raw/api_weather.json data/raw/api_github_repos.json
```

5. Inspecciona los primeros registros del archivo meteorológico:

```bash
python -c "
import json
with open('data/raw/api_weather.json') as f:
    data = json.load(f)
print('Claves principales:', list(data.keys()))
print('Variables diarias:', list(data.get('daily', {}).keys()))
print('Primeros 3 días:', data['daily']['time'][:3])
print('Temp máx primeros 3 días:', data['daily']['temperature_2m_max'][:3])
"
```

6. Inspecciona los repositorios de GitHub:

```bash
python -c "
import json
with open('data/raw/api_github_repos.json') as f:
    repos = json.load(f)
print(f'Total repositorios: {len(repos)}')
print(f'Primer repo: {repos[0][\"name\"]}')
print(f'Campos disponibles: {list(repos[0].keys())}')
"
```

### Salida esperada

```
--- Vista previa: Datos meteorológicos ---
        date  temperature_2m_max  temperature_2m_min  precipitation_sum  latitude  longitude       timezone
0 2023-01-01                12.3                 2.1                0.0   40.4168    -3.7038  Europe/Madrid
1 2023-01-02                10.8                 1.5                0.2   40.4168    -3.7038  Europe/Madrid
...

--- Vista previa: Repositorios GitHub ---
                    name language  stargazers_count                updated_at
0               cpython   Python             XXXXX  2024-XX-XX ...
1               peps      Python             XXXXX  2024-XX-XX ...
...

======================================================================
REPORTE COMPARATIVO DE ESTRUCTURA DE DATOS
======================================================================
...
  Métrica                        Open-Meteo      GitHub
  Filas                          365             XX
  Columnas                       7               16
...
```

> Los valores exactos de GitHub variarán según el estado actual de la organización `python`.

### Verificación

```bash
# Verificar que el archivo de log se generó
cat logs/api_extraction.log | tail -5

# Verificar tamaño mínimo de archivos
python -c "
from pathlib import Path
w = Path('data/raw/api_weather.json').stat().st_size
g = Path('data/raw/api_github_repos.json').stat().st_size
print(f'api_weather.json: {w/1024:.1f} KB')
print(f'api_github_repos.json: {g/1024:.1f} KB')
assert w > 1000, 'Archivo meteorológico demasiado pequeño'
assert g > 500, 'Archivo GitHub demasiado pequeño'
print('✓ Ambos archivos tienen tamaño válido')
"
```

---

## Paso 7: Verificar la normalización y calidad de datos

**Objetivo:** Validar que los DataFrames resultantes tienen la estructura correcta, tipos de datos apropiados y sin anomalías graves.

### Instrucciones

1. Crea y ejecuta un script de validación rápida:

```bash
python -c "
import json
import pandas as pd

# --- Validar datos meteorológicos ---
with open('data/raw/api_weather.json') as f:
    weather = json.load(f)

df_w = pd.DataFrame(weather['daily'])
df_w['time'] = pd.to_datetime(df_w['time'])

print('=== VALIDACIÓN OPEN-METEO ===')
print(f'Filas: {len(df_w)} (esperado: 365)')
assert len(df_w) == 365, f'ERROR: Se esperaban 365 filas, se obtuvieron {len(df_w)}'
print(f'Columnas: {list(df_w.columns)}')
print(f'Nulos por columna:')
print(df_w.isnull().sum())
print(f'Rango temp máx: {df_w[\"temperature_2m_max\"].min():.1f} a {df_w[\"temperature_2m_max\"].max():.1f} °C')
print()

# --- Validar datos GitHub ---
with open('data/raw/api_github_repos.json') as f:
    repos = json.load(f)

df_g = pd.DataFrame(repos)
print('=== VALIDACIÓN GITHUB ===')
print(f'Filas: {len(df_g)}')
assert len(df_g) > 0, 'ERROR: No se obtuvieron repositorios'
print(f'Columnas: {list(df_g.columns)}')
print(f'Repos con lenguaje definido: {df_g[\"language\"].notna().sum()}/{len(df_g)}')
print(f'Top 5 lenguajes:')
print(df_g['language'].value_counts().head())
print()
print('✓ TODAS LAS VALIDACIONES PASARON')
"
```

### Salida esperada

```
=== VALIDACIÓN OPEN-METEO ===
Filas: 365 (esperado: 365)
Columnas: ['time', 'temperature_2m_max', 'temperature_2m_min', 'precipitation_sum']
Nulos por columna:
time                  0
temperature_2m_max    0
temperature_2m_min    0
precipitation_sum     0
dtype: int64
Rango temp máx: X.X a XX.X °C

=== VALIDACIÓN GITHUB ===
Filas: XX
Columnas: ['id', 'name', 'full_name', ...]
...
✓ TODAS LAS VALIDACIONES PASARON
```

### Verificación

La ejecución debe terminar sin `AssertionError`. Si alguna aserción falla, revisa los pasos anteriores.

---

## Validación y testing

Ejecuta las siguientes comprobaciones finales para confirmar que el laboratorio se completó exitosamente:

```bash
python -c "
from pathlib import Path
import json

print('=== CHECKLIST FINAL ===')
checks = []

# 1. Archivos de código existen
for f in ['src/api_client.py', 'src/extractor_weather.py', 'src/extractor_github.py', 'src/lab04_api_extraction.py']:
    exists = Path(f).exists()
    checks.append(exists)
    print(f'  [{'✓' if exists else '✗'}] {f}')

# 2. Archivos de datos existen y tienen contenido
for f in ['data/raw/api_weather.json', 'data/raw/api_github_repos.json']:
    p = Path(f)
    exists = p.exists() and p.stat().st_size > 100
    checks.append(exists)
    print(f'  [{'✓' if exists else '✗'}] {f} ({p.stat().st_size//1024}KB)' if p.exists() else f'  [✗] {f}')

# 3. Log existe
log_exists = Path('logs/api_extraction.log').exists()
checks.append(log_exists)
print(f'  [{'✓' if log_exists else '✗'}] logs/api_extraction.log')

# 4. Datos meteorológicos tienen 365 registros
with open('data/raw/api_weather.json') as f:
    w = json.load(f)
weather_ok = len(w.get('daily', {}).get('time', [])) == 365
checks.append(weather_ok)
print(f'  [{'✓' if weather_ok else '✗'}] 365 registros meteorológicos')

# 5. GitHub tiene al menos 1 repo
with open('data/raw/api_github_repos.json') as f:
    g = json.load(f)
github_ok = len(g) > 0
checks.append(github_ok)
print(f'  [{'✓' if github_ok else '✗'}] {len(g)} repositorios GitHub')

print(f'\nResultado: {sum(checks)}/{len(checks)} verificaciones pasaron')
if all(checks):
    print('🎉 ¡Laboratorio completado exitosamente!')
else:
    print('⚠️  Revisa los items marcados con ✗')
"
```

---

## Solución de problemas

### Problema 1: Error 401 Unauthorized al consultar GitHub

**Síntomas:**
```
ERROR src.api_client: Error del cliente: 401 - {"message":"Bad credentials"...}
```

**Causa:** El Personal Access Token (PAT) en el archivo `.env` es inválido, ha expirado o no fue copiado correctamente. Los tokens clásicos de GitHub comienzan con `ghp_` y los fine-grained con `github_pat_`.

**Solución:**

1. Verifica que el token en `.env` no tenga espacios ni saltos de línea extra:
```bash
grep GITHUB_TOKEN .env
```

2. Prueba el token directamente con curl:
```bash
curl -H "Authorization: Bearer $(grep GITHUB_TOKEN .env | cut -d= -f2)" https://api.github.com/user
```

3. Si el token expiró, genera uno nuevo en GitHub → Settings → Developer settings → Personal access tokens y actualiza `.env`.

---

### Problema 2: Timeout o ConnectionError al conectar con Open-Meteo

**Síntomas:**
```
httpx.ConnectTimeout: timed out
```
o
```
httpx.ConnectError: [Errno -2] Name or service not known
```

**Causa:** Problema de conectividad a Internet, DNS no resuelve el dominio `archive-api.open-meteo.com`, o un firewall/proxy corporativo bloquea la conexión saliente.

**Solución:**

1. Verifica la conectividad básica:
```bash
ping -c 3 archive-api.open-meteo.com
curl -I "https://archive-api.open-meteo.com/v1/archive?latitude=40.42&longitude=-3.70&start_date=2023-01-01&end_date=2023-01-02&daily=temperature_2m_max"
```

2. Si estás detrás de un proxy, configura las variables de entorno:
```bash
export HTTPS_PROXY=http://tu-proxy:puerto
```

3. Si el problema persiste, aumenta el timeout en `api_client.py` (de 30 a 60 segundos) o verifica que la URL en `.env` sea exactamente `https://archive-api.open-meteo.com/v1/archive`.

---

## Limpieza

Este laboratorio genera archivos que serán utilizados en el **Laboratorio 5** como referencia de estructura de datos. **No elimines** los archivos de datos generados.

Si necesitas re-ejecutar el laboratorio desde cero:

```bash
# Eliminar solo los archivos de salida (los datos se regeneran)
rm -f data/raw/api_weather.json data/raw/api_github_repos.json
rm -f logs/api_extraction.log
```

Si deseas desinstalar las dependencias añadidas (no recomendado si continúas con los siguientes labs):

```bash
pip uninstall httpx tenacity -y
```

---

## Resumen

### Conceptos clave aplicados

| Concepto | Implementación en este lab |
|----------|---------------------------|
| Cliente HTTP con headers | `BaseAPIClient` con `httpx`, timeout de 30s y `User-Agent` personalizado |
| Autenticación por token | Header `Authorization: Bearer <token>` para GitHub API |
| Reintentos exponenciales | Decorador `@retry` de tenacity con `wait_exponential(min=1, max=10)` y 3 intentos |
| Manejo de errores HTTP | Excepciones personalizadas para 429, 4xx y 5xx |
| Paginación | Iteración con `page`/`per_page` hasta agotar resultados o alcanzar límite |
| Normalización JSON | `pd.json_normalize()` y `pd.DataFrame()` para aplanar estructuras anidadas |
| Gestión de credenciales | Variables en `.env` cargadas con `python-dotenv` |

### Archivos generados

- `src/api_client.py` — Cliente HTTP reutilizable con reintentos
- `src/extractor_weather.py` — Extractor de Open-Meteo
- `src/extractor_github.py` — Extractor paginado de GitHub
- `src/lab04_api_extraction.py` — Orquestador principal
- `data/raw/api_weather.json` — 365 registros meteorológicos de Madrid 2023
- `data/raw/api_github_repos.json` — Repositorios de la organización `python`
- `logs/api_extraction.log` — Registro de ejecución

### Conexión con laboratorios posteriores

Los datos meteorológicos extraídos en `data/raw/api_weather.json` se utilizarán en el **Laboratorio 5** como referencia de estructura para comparar contra datos scrapeados de un sitio web de clima, validando consistencia entre fuentes de datos.

### Recursos adicionales

- [Documentación Open-Meteo API](https://open-meteo.com/en/docs)
- [GitHub REST API Documentation](https://docs.github.com/en/rest)
- [httpx — Documentación oficial](https://www.python-httpx.org/)
- [tenacity — Retry library for Python](https://tenacity.readthedocs.io/)
