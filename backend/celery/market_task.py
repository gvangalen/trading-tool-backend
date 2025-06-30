from celery import Celery
import os
import logging
import traceback
import requests
from urllib.parse import urljoin
from tenacity import retry, stop_after_attempt, wait_exponential, RetryError

# ✅ Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ✅ Celery-configuratie
celery = Celery(__name__)
celery.conf.broker_url = os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0")
celery.conf.result_backend = os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/0")

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

# ✅ Celery taak: Marktdata ophalen en opslaan
@celery.task(name="market.fetch_market_data")
def fetch_market_data():
    logger.info("🌍 Marktdata ophalen gestart...")
    try:
        data = safe_request(urljoin(API_BASE_URL, "/save_market_data"))
        logger.info(f"✅ Marktdata succesvol opgeslagen: {data}")
    except RetryError:
        logger.error("❌ Alle retries mislukt voor fetch_market_data!")
        logger.error(traceback.format_exc())
