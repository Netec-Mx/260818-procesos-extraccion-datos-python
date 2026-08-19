# Extracción de información desde un sitio web

## Metadatos

| Campo | Valor |
|-------|-------|
| **Duración** | 60 minutos |
| **Complejidad** | Alta |
| **Nivel Bloom** | Aplicar |
| **Tecnologías** | Python 3.12.1, requests 2.31.0, beautifulsoup4 4.12.3, lxml 5.1.0, selenium 4.18.1, Google Chrome 123.0, pandas 2.2.1 |

---

## Descripción General

En este laboratorio implementarás un pipeline completo de web scraping que abarca dos escenarios reales: extracción de datos tabulares desde un sitio web estático (books.toscrape.com) usando `requests` + `BeautifulSoup`, y extracción de contenido dinámico renderizado con JavaScript (quotes.toscrape.com/scroll) usando `Selenium` con scroll infinito. Aplicarás buenas prácticas profesionales incluyendo verificación de `robots.txt`, delays aleatorios, rotación de User-Agent y manejo robusto de excepciones.

---

## Objetivos de Aprendizaje

Al completar este laboratorio serás capaz de:

- [ ] Extraer datos tabulares de las 50 páginas de books.toscrape.com implementando paginación iterativa y almacenando 1000 registros en CSV
- [ ] Automatizar la extracción de contenido dinámico con Selenium en modo headless, detectando el final del scroll infinito mediante JavaScript executor
- [ ] Implementar buenas prácticas de scraping: verificación de robots.txt, delays aleatorios (1-3s), rotación de User-Agent y manejo de excepciones
- [ ] Integrar los resultados del scraping con el pipeline existente del curso, validando la estructura contra datos previos en PostgreSQL

---

## Prerrequisitos

### Conocimientos Requeridos
- Laboratorios 1 al 4 completados (estructura de proyecto, formatos de archivo, PostgreSQL, APIs REST)
- Comprensión básica de HTML: estructura DOM, selectores CSS, atributos `class`, `id`, `href`
- Familiaridad con el protocolo HTTP (métodos GET, códigos de estado)

### Acceso y Recursos
- Google Chrome 123.0 instalado en el sistema
- Docker con contenedor `postgres_etl` en ejecución (del Laboratorio 3)
- Entorno virtual `venv_etl` activo con dependencias previas
- Conexión a Internet estable (acceso a books.toscrape.com y quotes.toscrape.com)

---

## Entorno del Laboratorio

### Software Requerido

| Componente | Versión | Propósito |
|------------|---------|-----------|
| Python | 3.12.1 | Runtime principal |
| requests | 2.31.0 | Solicitudes HTTP para scraping estático |
| beautifulsoup4 | 4.12.3 | Parseo de HTML |
| lxml | 5.1.0 | Parser rápido para BeautifulSoup |
| selenium | 4.18.1 | Automatización de navegador para contenido dinámico |
| pandas | 2.2.1 | Estructuración y exportación de datos |
| Google Chrome | 123.0 | Navegador para Selenium |

### Configuración Inicial

```bash
# Navegar al directorio raíz del proyecto
cd ~/etl_course/labs/lab01/

# Activar el entorno virtual
# Linux/macOS:
source venv_etl/bin/activate
# Windows (PowerShell):
# .\venv_etl\Scripts\activate

# Verificar que el entorno está activo
which python
```

---

## Paso 1: Instalar Dependencias de Selenium y Verificar Chrome

**Objetivo:** Instalar selenium 4.18.1 en el entorno virtual existente y confirmar que Google Chrome está disponible para la automatización.

### Instrucciones

1. Instale selenium en el entorno virtual:

```bash
pip install selenium==4.18.1
```

2. Verifique que todas las dependencias necesarias están instaladas:

```bash
pip list | grep -iE "requests|beautifulsoup4|lxml|selenium|pandas"
```

3. Confirme la versión de Google Chrome instalada:

```bash
# Linux:
google-chrome --version

# macOS:
/Applications/Google\ Chrome.app/Contents/MacOS/Google\ Chrome --version

# Windows (PowerShell):
# (Get-Item "C:\Program Files\Google\Chrome\Application\chrome.exe").VersionInfo.FileVersion
```

4. Verifique que Selenium Manager puede gestionar ChromeDriver automáticamente creando un script de prueba rápida:

```bash
python -c "
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
options = Options()
options.add_argument('--headless')
options.add_argument('--no-sandbox')
driver = webdriver.Chrome(options=options)
print(f'ChromeDriver OK - Título: {driver.title}')
driver.quit()
print('Selenium configurado correctamente')
"
```

### Salida Esperada

```
requests          2.31.0
beautifulsoup4    4.12.3
lxml              5.1.0
selenium          4.18.1
pandas            2.2.1
```

```
Google Chrome 123.0.xxxx.xx
```

```
ChromeDriver OK - Título: 
Selenium configurado correctamente
```

### Verificación

Si el script de prueba ejecuta sin errores y muestra "Selenium configurado correctamente", el entorno está listo. Selenium Manager descarga ChromeDriver automáticamente la primera vez.

---

## Paso 2: Crear el Script Principal con Verificación de robots.txt

**Objetivo:** Crear el archivo `lab05_web_scraping.py` con la función de verificación de robots.txt y las importaciones necesarias.

### Instrucciones

1. Cree el archivo del script principal:

```bash
touch ~/etl_course/labs/lab01/src/lab05_web_scraping.py
```

2. Abra el archivo y escriba la sección de importaciones y configuración:

```python
#!/usr/bin/env python3
"""
Lab 05 - Web Scraping: Extracción de datos estáticos y dinámicos.
Sitios objetivo:
  - books.toscrape.com (estático, requests + BeautifulSoup)
  - quotes.toscrape.com/scroll (dinámico, Selenium)
"""

import csv
import json
import logging
import os
import random
import time
from urllib.parse import urljoin
from urllib.robotparser import RobotFileParser

import pandas as pd
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    NoSuchElementException,
    WebDriverException,
)

# --- Configuración de logging ---
LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs")
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, "lab05_scraping.log")),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# --- Rutas de salida ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
os.makedirs(RAW_DIR, exist_ok=True)

# --- User-Agents para rotación ---
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:124.0) Gecko/20100101 Firefox/124.0",
]


def get_random_headers():
    """Retorna headers HTTP con User-Agent aleatorio."""
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
    }


def check_robots_txt(base_url: str, path: str = "/") -> bool:
    """
    Verifica si el scraping está permitido según robots.txt.
    
    Args:
        base_url: URL base del sitio (ej: https://books.toscrape.com)
        path: Ruta específica a verificar
        
    Returns:
        True si el scraping está permitido, False en caso contrario.
    """
    robots_url = urljoin(base_url, "/robots.txt")
    logger.info(f"Verificando robots.txt en: {robots_url}")
    
    try:
        rp = RobotFileParser()
        rp.set_url(robots_url)
        rp.read()
        
        allowed = rp.can_fetch("*", urljoin(base_url, path))
        
        if allowed:
            logger.info(f"✓ Scraping PERMITIDO para {base_url}{path}")
        else:
            logger.warning(f"✗ Scraping NO PERMITIDO para {base_url}{path}")
        
        return allowed
        
    except Exception as e:
        logger.warning(f"No se pudo leer robots.txt ({e}). Procediendo con precaución.")
        return True  # Si no hay robots.txt, se asume permitido


def polite_delay(min_seconds: float = 1.0, max_seconds: float = 3.0):
    """Aplica un delay aleatorio entre requests para ser respetuoso."""
    delay = random.uniform(min_seconds, max_seconds)
    time.sleep(delay)
```

3. Guarde el archivo.

### Salida Esperada

Archivo `src/lab05_web_scraping.py` creado con las importaciones, configuración de logging, lista de User-Agents, y las funciones `check_robots_txt()`, `get_random_headers()` y `polite_delay()`.

### Verificación

```bash
python -c "
import sys
sys.path.insert(0, 'src')
from lab05_web_scraping import check_robots_txt, get_random_headers, polite_delay
result = check_robots_txt('https://books.toscrape.com')
print(f'robots.txt verificado: permitido={result}')
headers = get_random_headers()
print(f'User-Agent seleccionado: {headers[\"User-Agent\"][:50]}...')
"
```

Debe mostrar que el scraping está permitido y un User-Agent aleatorio.

---

## Paso 3: Implementar el Scraper Estático (books.toscrape.com)

**Objetivo:** Implementar la función que extrae título, precio, rating y disponibilidad de los 1000 libros en 50 páginas, con paginación iterativa y delays.

### Instrucciones

1. Agregue la siguiente función al archivo `src/lab05_web_scraping.py`:

```python
# --- Mapeo de ratings de texto a número ---
RATING_MAP = {
    "One": 1,
    "Two": 2,
    "Three": 3,
    "Four": 4,
    "Five": 5,
}


def scrape_books_static() -> pd.DataFrame:
    """
    Extrae datos de los 1000 libros de books.toscrape.com.
    
    Itera las 50 páginas de paginación, extrayendo para cada libro:
    - title: título del libro
    - price: precio en libras (float)
    - rating: calificación numérica (1-5)
    - availability: estado de disponibilidad (texto)
    
    Returns:
        DataFrame con los datos de todos los libros extraídos.
    """
    BASE_URL = "https://books.toscrape.com/"
    
    # Verificar robots.txt antes de iniciar
    if not check_robots_txt(BASE_URL):
        logger.error("Scraping no permitido por robots.txt. Abortando.")
        return pd.DataFrame()
    
    all_books = []
    total_pages = 50
    
    logger.info(f"Iniciando extracción de {total_pages} páginas de books.toscrape.com")
    
    for page_num in range(1, total_pages + 1):
        # Construir URL de la página
        if page_num == 1:
            page_url = BASE_URL
        else:
            page_url = f"{BASE_URL}catalogue/page-{page_num}.html"
        
        try:
            # Solicitud HTTP con headers rotativos
            headers = get_random_headers()
            response = requests.get(page_url, headers=headers, timeout=30)
            response.raise_for_status()
            
            # Parsear HTML con lxml
            soup = BeautifulSoup(response.text, "lxml")
            
            # Seleccionar todos los artículos de libros
            articles = soup.select("article.product_pod")
            
            if not articles:
                logger.warning(f"Página {page_num}: No se encontraron libros.")
                continue
            
            for article in articles:
                try:
                    # Título: está en el atributo title del enlace dentro de h3
                    title_tag = article.select_one("h3 > a")
                    title = title_tag["title"] if title_tag else "Sin título"
                    
                    # Precio: dentro de <p class="price_color">
                    price_tag = article.select_one("p.price_color")
                    price_text = price_tag.get_text(strip=True) if price_tag else "£0.00"
                    # Remover símbolo de libra y convertir a float
                    price = float(price_text.replace("£", "").replace("Â", "").strip())
                    
                    # Rating: clase CSS del elemento <p class="star-rating X">
                    rating_tag = article.select_one("p.star-rating")
                    if rating_tag:
                        rating_classes = rating_tag.get("class", [])
                        # La segunda clase indica el rating (One, Two, ..., Five)
                        rating_word = [c for c in rating_classes if c != "star-rating"]
                        rating = RATING_MAP.get(rating_word[0], 0) if rating_word else 0
                    else:
                        rating = 0
                    
                    # Disponibilidad: <p class="instock availability">
                    avail_tag = article.select_one("p.availability")
                    availability = avail_tag.get_text(strip=True) if avail_tag else "Unknown"
                    
                    all_books.append({
                        "title": title,
                        "price": price,
                        "rating": rating,
                        "availability": availability,
                    })
                    
                except (AttributeError, ValueError, IndexError) as e:
                    logger.warning(f"Error parseando libro en página {page_num}: {e}")
                    continue
            
            logger.info(
                f"Página {page_num}/{total_pages} completada: "
                f"{len(articles)} libros extraídos (total acumulado: {len(all_books)})"
            )
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error HTTP en página {page_num}: {e}")
            continue
        
        # Delay entre páginas (buena práctica)
        if page_num < total_pages:
            polite_delay(1.0, 3.0)
    
    # Crear DataFrame
    df = pd.DataFrame(all_books)
    logger.info(f"Extracción completada: {len(df)} libros totales")
    
    # Guardar en CSV
    output_path = os.path.join(RAW_DIR, "books_static.csv")
    df.to_csv(output_path, index=False, encoding="utf-8")
    logger.info(f"Datos guardados en: {output_path}")
    
    return df
```

2. Guarde el archivo.

### Salida Esperada

La función está definida y lista para ejecutarse. Al invocarla, producirá logs como:

```
2024-XX-XX 10:00:01 [INFO] Verificando robots.txt en: https://books.toscrape.com/robots.txt
2024-XX-XX 10:00:01 [INFO] ✓ Scraping PERMITIDO para https://books.toscrape.com/
2024-XX-XX 10:00:01 [INFO] Iniciando extracción de 50 páginas de books.toscrape.com
2024-XX-XX 10:00:02 [INFO] Página 1/50 completada: 20 libros extraídos (total acumulado: 20)
...
2024-XX-XX 10:03:45 [INFO] Página 50/50 completada: 20 libros extraídos (total acumulado: 1000)
2024-XX-XX 10:03:45 [INFO] Extracción completada: 1000 libros totales
2024-XX-XX 10:03:45 [INFO] Datos guardados en: .../data/raw/books_static.csv
```

### Verificación

La verificación completa se realizará en el Paso 6 al ejecutar el script. Por ahora, valide la sintaxis:

```bash
python -c "import ast; ast.parse(open('src/lab05_web_scraping.py').read()); print('Sintaxis OK')"
```

---

## Paso 4: Implementar el Scraper Dinámico (quotes.toscrape.com/scroll)

**Objetivo:** Implementar la función que usa Selenium en modo headless para extraer citas de una página con scroll infinito, detectando el final mediante comparación de alturas del DOM.

### Instrucciones

1. Agregue la siguiente función al archivo `src/lab05_web_scraping.py`:

```python
def scrape_quotes_dynamic() -> list[dict]:
    """
    Extrae citas de quotes.toscrape.com/scroll usando Selenium.
    
    El sitio carga contenido dinámicamente mediante scroll infinito.
    Se detecta el final comparando la altura del documento antes y
    después de cada scroll.
    
    Returns:
        Lista de diccionarios con keys: quote, author, tags
    """
    BASE_URL = "https://quotes.toscrape.com"
    TARGET_URL = f"{BASE_URL}/scroll"
    
    # Verificar robots.txt
    if not check_robots_txt(BASE_URL, "/scroll"):
        logger.error("Scraping no permitido por robots.txt. Abortando.")
        return []
    
    logger.info(f"Iniciando extracción dinámica de: {TARGET_URL}")
    
    # Configurar Chrome en modo headless
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument(f"user-agent={random.choice(USER_AGENTS)}")
    chrome_options.add_argument("--window-size=1920,1080")
    
    driver = None
    all_quotes = []
    
    try:
        # Iniciar el navegador (Selenium Manager gestiona ChromeDriver)
        driver = webdriver.Chrome(options=chrome_options)
        driver.set_page_load_timeout(30)
        
        # Navegar a la página
        driver.get(TARGET_URL)
        logger.info("Página cargada. Iniciando scroll infinito...")
        
        # Esperar a que el primer contenido esté disponible
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.quote"))
        )
        
        # Scroll infinito: detectar final por altura del DOM
        scroll_count = 0
        max_scrolls = 50  # Límite de seguridad
        
        while scroll_count < max_scrolls:
            # Obtener altura actual del documento
            last_height = driver.execute_script("return document.body.scrollHeight")
            
            # Hacer scroll hasta el final
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            
            # Esperar a que se cargue nuevo contenido
            time.sleep(2)
            
            # Obtener nueva altura
            new_height = driver.execute_script("return document.body.scrollHeight")
            
            scroll_count += 1
            logger.info(f"Scroll #{scroll_count}: altura {last_height} -> {new_height}")
            
            # Si la altura no cambió, hemos llegado al final
            if new_height == last_height:
                logger.info("Final del contenido detectado (altura sin cambios).")
                break
        
        # Extraer todas las citas visibles en la página
        quote_elements = driver.find_elements(By.CSS_SELECTOR, "div.quote")
        logger.info(f"Elementos de cita encontrados: {len(quote_elements)}")
        
        for element in quote_elements:
            try:
                # Texto de la cita
                text_el = element.find_element(By.CSS_SELECTOR, "span.text")
                quote_text = text_el.text.strip().strip("\u201c\u201d")
                
                # Autor
                author_el = element.find_element(By.CSS_SELECTOR, "small.author")
                author = author_el.text.strip()
                
                # Tags
                tag_elements = element.find_elements(By.CSS_SELECTOR, "a.tag")
                tags = [tag.text.strip() for tag in tag_elements]
                
                all_quotes.append({
                    "quote": quote_text,
                    "author": author,
                    "tags": tags,
                })
                
            except NoSuchElementException as e:
                logger.warning(f"Elemento no encontrado en una cita: {e}")
                continue
        
        logger.info(f"Extracción dinámica completada: {len(all_quotes)} citas")
        
    except WebDriverException as e:
        logger.error(f"Error de WebDriver: {e}")
    except TimeoutException as e:
        logger.error(f"Timeout esperando carga de página: {e}")
    except Exception as e:
        logger.error(f"Error inesperado en scraper dinámico: {e}")
    finally:
        if driver:
            driver.quit()
            logger.info("Navegador cerrado correctamente.")
    
    # Guardar en JSON
    if all_quotes:
        output_path = os.path.join(RAW_DIR, "quotes_dynamic.json")
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(all_quotes, f, ensure_ascii=False, indent=2)
        logger.info(f"Datos guardados en: {output_path}")
    
    return all_quotes
```

2. Guarde el archivo.

### Salida Esperada

La función queda definida. Al ejecutarse, producirá logs como:

```
2024-XX-XX 10:04:00 [INFO] Verificando robots.txt en: https://quotes.toscrape.com/robots.txt
2024-XX-XX 10:04:00 [INFO] ✓ Scraping PERMITIDO para https://quotes.toscrape.com/scroll
2024-XX-XX 10:04:00 [INFO] Iniciando extracción dinámica de: https://quotes.toscrape.com/scroll
2024-XX-XX 10:04:03 [INFO] Página cargada. Iniciando scroll infinito...
2024-XX-XX 10:04:05 [INFO] Scroll #1: altura 1200 -> 2400
...
2024-XX-XX 10:04:25 [INFO] Final del contenido detectado (altura sin cambios).
2024-XX-XX 10:04:25 [INFO] Elementos de cita encontrados: 100
2024-XX-XX 10:04:25 [INFO] Extracción dinámica completada: 100 citas
2024-XX-XX 10:04:25 [INFO] Navegador cerrado correctamente.
2024-XX-XX 10:04:25 [INFO] Datos guardados en: .../data/raw/quotes_dynamic.json
```

### Verificación

```bash
python -c "import ast; ast.parse(open('src/lab05_web_scraping.py').read()); print('Sintaxis OK')"
```

---

## Paso 5: Implementar la Validación Cruzada con PostgreSQL y el Bloque Main

**Objetivo:** Agregar la función de comparación estructural con datos del Titanic en PostgreSQL y el bloque `main` que orquesta la ejecución completa.

### Instrucciones

1. Agregue las siguientes funciones al final de `src/lab05_web_scraping.py`:

```python
def validate_pipeline_integration():
    """
    Compara la estructura de los datos scrapeados con los datos del
    dataset Titanic en PostgreSQL para validar el pipeline completo.
    """
    from dotenv import load_dotenv
    from sqlalchemy import create_engine, text
    
    # Cargar variables de entorno
    env_path = os.path.join(BASE_DIR, ".env")
    load_dotenv(env_path)
    
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5433")
    db_name = os.getenv("DB_NAME", "etl_db")
    db_user = os.getenv("DB_USER", "etl_user")
    db_password = os.getenv("DB_PASSWORD", "")
    
    logger.info("=" * 60)
    logger.info("VALIDACIÓN DE INTEGRACIÓN DEL PIPELINE")
    logger.info("=" * 60)
    
    # --- Datos scrapeados ---
    books_path = os.path.join(RAW_DIR, "books_static.csv")
    quotes_path = os.path.join(RAW_DIR, "quotes_dynamic.json")
    
    if os.path.exists(books_path):
        df_books = pd.read_csv(books_path)
        logger.info(f"[Books CSV] Filas: {len(df_books)}, Columnas: {list(df_books.columns)}")
        logger.info(f"[Books CSV] Tipos: {dict(df_books.dtypes)}")
    else:
        logger.warning(f"Archivo no encontrado: {books_path}")
        df_books = pd.DataFrame()
    
    if os.path.exists(quotes_path):
        with open(quotes_path, "r", encoding="utf-8") as f:
            quotes_data = json.load(f)
        logger.info(f"[Quotes JSON] Registros: {len(quotes_data)}")
        if quotes_data:
            logger.info(f"[Quotes JSON] Keys: {list(quotes_data[0].keys())}")
    else:
        logger.warning(f"Archivo no encontrado: {quotes_path}")
    
    # --- Datos en PostgreSQL (Titanic del Lab 3) ---
    try:
        connection_string = (
            f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        )
        engine = create_engine(connection_string)
        
        with engine.connect() as conn:
            # Verificar si existe la tabla titanic
            result = conn.execute(text(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'public' AND table_name = 'titanic'"
            ))
            tables = [row[0] for row in result]
            
            if "titanic" in tables:
                df_titanic = pd.read_sql("SELECT * FROM titanic LIMIT 5", engine)
                logger.info(f"[PostgreSQL Titanic] Columnas: {list(df_titanic.columns)}")
                logger.info(f"[PostgreSQL Titanic] Tipos: {dict(df_titanic.dtypes)}")
                
                # Comparación estructural
                logger.info("\n--- COMPARACIÓN ESTRUCTURAL ---")
                logger.info(f"Books: {len(df_books)} filas x {len(df_books.columns)} cols")
                logger.info(f"Titanic: {df_titanic.shape[0]} filas (muestra) x {len(df_titanic.columns)} cols")
                logger.info("Ambos datasets son tabulares y aptos para análisis con pandas.")
            else:
                logger.info("[PostgreSQL] Tabla 'titanic' no encontrada. Omitiendo comparación.")
        
        engine.dispose()
        
    except Exception as e:
        logger.warning(f"No se pudo conectar a PostgreSQL: {e}")
        logger.info("La validación de BD se omite. Los archivos scrapeados son válidos.")
    
    logger.info("=" * 60)
    logger.info("VALIDACIÓN COMPLETADA")
    logger.info("=" * 60)


def main():
    """Función principal que orquesta la ejecución del laboratorio."""
    logger.info("=" * 60)
    logger.info("LAB 05 - WEB SCRAPING: INICIO")
    logger.info("=" * 60)
    
    start_time = time.time()
    
    # --- Sección 1: Scraping estático ---
    logger.info("\n{'='*40}")
    logger.info("SECCIÓN 1: Scraping Estático (books.toscrape.com)")
    logger.info("=" * 40)
    
    df_books = scrape_books_static()
    
    if not df_books.empty:
        logger.info(f"\nResumen de datos extraídos:")
        logger.info(f"  - Total de libros: {len(df_books)}")
        logger.info(f"  - Precio promedio: £{df_books['price'].mean():.2f}")
        logger.info(f"  - Rating promedio: {df_books['rating'].mean():.1f}/5")
        logger.info(f"  - Distribución de ratings:\n{df_books['rating'].value_counts().sort_index().to_string()}")
    
    # --- Sección 2: Scraping dinámico ---
    logger.info(f"\n{'='*40}")
    logger.info("SECCIÓN 2: Scraping Dinámico (quotes.toscrape.com/scroll)")
    logger.info("=" * 40)
    
    quotes = scrape_quotes_dynamic()
    
    if quotes:
        logger.info(f"\nResumen de datos extraídos:")
        logger.info(f"  - Total de citas: {len(quotes)}")
        authors = set(q["author"] for q in quotes)
        logger.info(f"  - Autores únicos: {len(authors)}")
        all_tags = [tag for q in quotes for tag in q["tags"]]
        logger.info(f"  - Tags totales: {len(all_tags)} ({len(set(all_tags))} únicos)")
    
    # --- Sección 3: Validación de integración ---
    logger.info(f"\n{'='*40}")
    logger.info("SECCIÓN 3: Validación de Integración")
    logger.info("=" * 40)
    
    validate_pipeline_integration()
    
    # --- Resumen final ---
    elapsed = time.time() - start_time
    logger.info(f"\nTiempo total de ejecución: {elapsed:.1f} segundos")
    logger.info("LAB 05 - WEB SCRAPING: FINALIZADO")


if __name__ == "__main__":
    main()
```

2. Instale `python-dotenv` si no está disponible (necesario para la validación):

```bash
pip install python-dotenv
```

3. Guarde el archivo completo.

### Salida Esperada

El archivo `src/lab05_web_scraping.py` está completo con todas las funciones necesarias.

### Verificación

```bash
python -c "
import ast
with open('src/lab05_web_scraping.py') as f:
    tree = ast.parse(f.read())
functions = [node.name for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)]
print(f'Funciones definidas: {functions}')
assert 'check_robots_txt' in functions
assert 'scrape_books_static' in functions
assert 'scrape_quotes_dynamic' in functions
assert 'validate_pipeline_integration' in functions
assert 'main' in functions
print('✓ Todas las funciones requeridas están presentes')
"
```

---

## Paso 6: Ejecutar el Script Completo

**Objetivo:** Ejecutar el pipeline de scraping completo y verificar que los archivos de salida se generan correctamente.

### Instrucciones

1. Asegúrese de estar en el directorio raíz del proyecto:

```bash
cd ~/etl_course/labs/lab01/
```

2. Ejecute el script completo:

```bash
python src/lab05_web_scraping.py
```

> **Nota:** La ejecución tomará aproximadamente 3-5 minutos debido a los delays respetuosos entre requests (1-3 segundos × 50 páginas) y el tiempo de scroll en Selenium.

3. Mientras se ejecuta, observe los logs en tiempo real. Debería ver el progreso página por página.

4. Una vez completado, verifique los archivos generados:

```bash
# Verificar que los archivos existen
ls -la data/raw/books_static.csv
ls -la data/raw/quotes_dynamic.json

# Contar líneas del CSV (1000 libros + 1 header = 1001)
wc -l data/raw/books_static.csv

# Ver las primeras 5 líneas del CSV
head -5 data/raw/books_static.csv

# Ver estructura del JSON
python -c "
import json
with open('data/raw/quotes_dynamic.json') as f:
    data = json.load(f)
print(f'Total citas: {len(data)}')
print(f'Primera cita: {json.dumps(data[0], indent=2, ensure_ascii=False)[:300]}')
"
```

### Salida Esperada

```
$ wc -l data/raw/books_static.csv
1001 data/raw/books_static.csv

$ head -5 data/raw/books_static.csv
title,price,rating,availability
A Light in the Attic,51.77,3,In stock
Tipping the Velvet,53.74,1,In stock
Soumission,50.1,1,In stock
Sharp Objects,47.82,4,In stock
```

```
Total citas: 100
Primera cita: {
  "quote": "The world as we have created it is a process of our thinking. It cannot be changed without changing our thinking.",
  "author": "Albert Einstein",
  "tags": ["change", "deep-thoughts", "thinking", "world"]
}
```

### Verificación

```bash
python -c "
import pandas as pd
import json

# Verificar CSV
df = pd.read_csv('data/raw/books_static.csv')
assert len(df) == 1000, f'Se esperaban 1000 libros, se obtuvieron {len(df)}'
assert list(df.columns) == ['title', 'price', 'rating', 'availability']
assert df['price'].dtype == float
assert df['rating'].between(1, 5).all()
print(f'✓ books_static.csv: {len(df)} libros, estructura correcta')

# Verificar JSON
with open('data/raw/quotes_dynamic.json') as f:
    quotes = json.load(f)
assert len(quotes) == 100, f'Se esperaban 100 citas, se obtuvieron {len(quotes)}'
assert all(k in quotes[0] for k in ['quote', 'author', 'tags'])
print(f'✓ quotes_dynamic.json: {len(quotes)} citas, estructura correcta')

print('\n✓ TODOS LOS DATOS EXTRAÍDOS CORRECTAMENTE')
"
```

---

## Paso 7: Verificar el Log y las Buenas Prácticas

**Objetivo:** Confirmar que el log registra correctamente la ejecución y que las buenas prácticas se implementaron.

### Instrucciones

1. Revise el archivo de log generado:

```bash
cat logs/lab05_scraping.log | head -30
```

2. Verifique que se registró la verificación de robots.txt:

```bash
grep "robots.txt" logs/lab05_scraping.log
```

3. Confirme que los delays se están aplicando (el tiempo total debe ser > 50 segundos para 50 páginas):

```bash
grep "Tiempo total" logs/lab05_scraping.log
```

4. Verifique la rotación de User-Agent inspeccionando el código:

```bash
grep -n "User-Agent" src/lab05_web_scraping.py | head -10
```

### Salida Esperada

```
$ grep "robots.txt" logs/lab05_scraping.log
2024-XX-XX [INFO] Verificando robots.txt en: https://books.toscrape.com/robots.txt
2024-XX-XX [INFO] ✓ Scraping PERMITIDO para https://books.toscrape.com/
2024-XX-XX [INFO] Verificando robots.txt en: https://quotes.toscrape.com/robots.txt
2024-XX-XX [INFO] ✓ Scraping PERMITIDO para https://quotes.toscrape.com/scroll
```

```
$ grep "Tiempo total" logs/lab05_scraping.log
2024-XX-XX [INFO] Tiempo total de ejecución: 185.3 segundos
```

### Verificación

El tiempo total debe ser superior a 50 segundos (evidencia de delays implementados). Ambos sitios deben mostrar "PERMITIDO" en la verificación de robots.txt.

---

## Validación y Pruebas

Ejecute el siguiente script de validación integral para confirmar que todos los componentes del laboratorio funcionan correctamente:

```bash
python -c "
import os
import json
import pandas as pd

print('=' * 60)
print('VALIDACIÓN INTEGRAL - LAB 05')
print('=' * 60)

errors = []

# 1. Verificar existencia del script
script_path = 'src/lab05_web_scraping.py'
if os.path.exists(script_path):
    print('✓ Script lab05_web_scraping.py existe')
else:
    errors.append('Script no encontrado')

# 2. Verificar CSV de libros
csv_path = 'data/raw/books_static.csv'
if os.path.exists(csv_path):
    df = pd.read_csv(csv_path)
    if len(df) == 1000:
        print(f'✓ books_static.csv: 1000 registros')
    else:
        errors.append(f'CSV tiene {len(df)} registros (esperados: 1000)')
    
    expected_cols = ['title', 'price', 'rating', 'availability']
    if list(df.columns) == expected_cols:
        print(f'✓ Columnas correctas: {expected_cols}')
    else:
        errors.append(f'Columnas incorrectas: {list(df.columns)}')
    
    if df['price'].min() > 0:
        print(f'✓ Precios válidos (min: £{df[\"price\"].min():.2f}, max: £{df[\"price\"].max():.2f})')
    else:
        errors.append('Precios inválidos encontrados')
    
    if df['rating'].between(1, 5).all():
        print(f'✓ Ratings válidos (1-5)')
    else:
        errors.append('Ratings fuera de rango')
else:
    errors.append('books_static.csv no encontrado')

# 3. Verificar JSON de citas
json_path = 'data/raw/quotes_dynamic.json'
if os.path.exists(json_path):
    with open(json_path, 'r', encoding='utf-8') as f:
        quotes = json.load(f)
    
    if len(quotes) == 100:
        print(f'✓ quotes_dynamic.json: 100 citas')
    else:
        errors.append(f'JSON tiene {len(quotes)} citas (esperadas: 100)')
    
    required_keys = {'quote', 'author', 'tags'}
    if quotes and required_keys.issubset(quotes[0].keys()):
        print(f'✓ Estructura JSON correcta: {required_keys}')
    else:
        errors.append('Estructura JSON incorrecta')
    
    authors = set(q['author'] for q in quotes)
    print(f'✓ Autores únicos: {len(authors)}')
else:
    errors.append('quotes_dynamic.json no encontrado')

# 4. Verificar log
log_path = 'logs/lab05_scraping.log'
if os.path.exists(log_path):
    with open(log_path) as f:
        log_content = f.read()
    if 'robots.txt' in log_content:
        print('✓ Verificación de robots.txt registrada en log')
    else:
        errors.append('No se encontró verificación de robots.txt en log')
    
    if 'FINALIZADO' in log_content:
        print('✓ Ejecución completada según log')
    else:
        errors.append('Ejecución no completada según log')
else:
    errors.append('Archivo de log no encontrado')

# Resultado final
print()
print('=' * 60)
if not errors:
    print('✓ TODAS LAS VALIDACIONES PASARON EXITOSAMENTE')
else:
    print(f'✗ {len(errors)} ERRORES ENCONTRADOS:')
    for e in errors:
        print(f'  - {e}')
print('=' * 60)
"
```

### Resultado Esperado

```
============================================================
VALIDACIÓN INTEGRAL - LAB 05
============================================================
✓ Script lab05_web_scraping.py existe
✓ books_static.csv: 1000 registros
✓ Columnas correctas: ['title', 'price', 'rating', 'availability']
✓ Precios válidos (min: £10.00, max: £59.99)
✓ Ratings válidos (1-5)
✓ quotes_dynamic.json: 100 citas
✓ Estructura JSON correcta: {'quote', 'author', 'tags'}
✓ Autores únicos: 50
✓ Verificación de robots.txt registrada en log
✓ Ejecución completada según log

============================================================
✓ TODAS LAS VALIDACIONES PASARON EXITOSAMENTE
============================================================
```

---

## Solución de Problemas

### Problema 1: Selenium no puede iniciar Chrome

**Síntomas:**
```
selenium.common.exceptions.WebDriverException: Message: unknown error: cannot find Chrome binary
```
o
```
SessionNotCreatedException: Message: session not created: This version of ChromeDriver only supports Chrome version XXX
```

**Causa:** Google Chrome no está instalado, no está en el PATH del sistema, o hay una incompatibilidad de versiones entre Chrome y ChromeDriver. Selenium Manager intenta descargar ChromeDriver automáticamente, pero necesita que Chrome esté accesible.

**Solución:**

```bash
# Verificar que Chrome está instalado
google-chrome --version  # Linux
# Si no está instalado (Ubuntu/Debian):
wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | sudo apt-key add -
echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" | sudo tee /etc/apt/sources.list.d/google-chrome.list
sudo apt update && sudo apt install google-chrome-stable

# Si hay conflicto de versiones, Selenium Manager lo resuelve automáticamente
# en selenium 4.18.1. Asegúrese de tener la versión correcta:
pip show selenium | grep Version
# Si es inferior a 4.6, actualice:
pip install selenium==4.18.1
```

En macOS, instale Chrome desde https://www.google.com/chrome/ o con:
```bash
brew install --cask google-chrome
```

---

### Problema 2: El CSV tiene menos de 1000 libros o contiene datos corruptos

**Síntomas:**
- El archivo `books_static.csv` tiene menos de 1001 líneas
- Algunos precios aparecen como `0.0` o títulos como "Sin título"
- Errores en el log como: `Error HTTP en página X: ConnectionError`

**Causa:** Conexión inestable a Internet que causa timeouts o respuestas incompletas, o el sitio books.toscrape.com está temporalmente lento. También puede ocurrir si el parser no encuentra los selectores CSS esperados debido a cambios en la estructura del sitio.

**Solución:**

```bash
# 1. Verificar conectividad al sitio
curl -I https://books.toscrape.com/

# 2. Si hay errores intermitentes, agregue reintentos al script.
# Modifique la solicitud en scrape_books_static() para incluir reintentos:
python -c "
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

session = requests.Session()
retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
session.mount('https://', HTTPAdapter(max_retries=retries))

resp = session.get('https://books.toscrape.com/')
print(f'Status: {resp.status_code}, Length: {len(resp.text)}')
"

# 3. Si el problema persiste, ejecute solo las páginas faltantes
# revisando el log para identificar qué páginas fallaron:
grep "Error HTTP" logs/lab05_scraping.log

# 4. Re-ejecute el script completo con mejor conectividad:
python src/lab05_web_scraping.py
```

Si el sitio ha cambiado su estructura HTML, inspeccione manualmente con:
```bash
python -c "
import requests
from bs4 import BeautifulSoup
r = requests.get('https://books.toscrape.com/')
soup = BeautifulSoup(r.text, 'lxml')
article = soup.select_one('article.product_pod')
print(article.prettify() if article else 'No se encontró article.product_pod')
"
```

---

## Limpieza

Si desea liberar espacio o reiniciar el laboratorio:

```bash
# Eliminar archivos de datos generados (mantiene la estructura)
rm -f data/raw/books_static.csv
rm -f data/raw/quotes_dynamic.json

# Limpiar logs del laboratorio
rm -f logs/lab05_scraping.log

# (Opcional) Si necesita desinstalar selenium:
# pip uninstall selenium -y

# El caché de ChromeDriver gestionado por Selenium Manager está en:
# ~/.cache/selenium/ (Linux/macOS) o %LOCALAPPDATA%\selenium\ (Windows)
# Para limpiarlo:
# rm -rf ~/.cache/selenium/
```

> **Nota:** No elimine el entorno virtual ni la estructura de directorios, ya que son compartidos con los demás laboratorios del curso.

---

## Resumen

En este laboratorio has implementado un pipeline completo de web scraping que demuestra las dos aproximaciones fundamentales para la extracción de datos web:

| Aspecto | Scraping Estático | Scraping Dinámico |
|---------|-------------------|-------------------|
| **Sitio** | books.toscrape.com | quotes.toscrape.com/scroll |
| **Herramienta** | requests + BeautifulSoup | Selenium + ChromeDriver |
| **Técnica** | Paginación por URL | Scroll infinito + JS executor |
| **Datos extraídos** | 1000 libros (4 campos) | 100 citas (3 campos) |
| **Formato de salida** | CSV | JSON |
| **Tiempo aprox.** | 2-4 min (con delays) | 30-60 seg |

**Buenas prácticas implementadas:**
- ✅ Verificación de `robots.txt` antes de cada extracción
- ✅ Delays aleatorios (1-3 segundos) entre solicitudes
- ✅ Rotación de User-Agent headers
- ✅ Manejo de excepciones granular (HTTP, parsing, elementos no encontrados)
- ✅ Logging detallado con progreso y resumen
- ✅ Modo headless para Selenium (sin interfaz gráfica)

### Recursos Adicionales

- [Documentación oficial de BeautifulSoup 4](https://www.crummy.com/software/BeautifulSoup/bs4/doc/)
- [Documentación oficial de Selenium para Python](https://selenium-python.readthedocs.io/)
- [books.toscrape.com - Sitio de práctica para scraping](https://books.toscrape.com/)
- [quotes.toscrape.com - Sitio de práctica con JavaScript](https://quotes.toscrape.com/)
- [Web Scraping Best Practices - Scrapy docs](https://docs.scrapy.org/en/latest/topics/practices.html)
