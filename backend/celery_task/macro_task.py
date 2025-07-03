import os
import logging
import traceback
import requests
from urllib.parse import urljoin
from tenacity import retry, stop_after_attempt, wait_exponential, RetryError
from celery import shared_task

# ✅ Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ✅ Configuratie
API_BASE_URL = os.getenv("API_BASE_URL", "http://market_dashboard-api:5002/api")
TIMEOUT = 10
HEADERS = {"Content-Type": "application/json"}

# ✅ Robuuste API-aanroep met retries
@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=5, max=20), reraise=True)
def safe_request(url, method="POST", payload=None):
    try:
        response = requests.request(method, url, json=payload, headers=HEADERS, timeout=TIMEOUT)
        response.raise_for_status()
        logger.info(f"✅ API-call succesvol: {url}")
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ RequestException bij {url}: {e}")
        logger.error(traceback.format_exc())
        raise
    except Exception as e:
        logger.error(f"⚠️ Onverwachte fout bij {url}: {e}")
        logger.error(traceback.format_exc())
        raise

# ✅ Celery taak: Macrodata ophalen en opslaan
@shared_task(name="celery_task.macro_task.fetch_macro_data")
def fetch_macro_data():
    logger.info("📊 Macrodata ophalen gestart...")
    try:
        data = safe_request(urljoin(API_BASE_URL, "/save_macro_data"))
        logger.info(f"✅ Macrodata succesvol opgeslagen: {data}")
    except RetryError:
        logger.error("❌ Alle retries mislukt voor fetch_macro_data!")
        logger.error(traceback.format_exc())
