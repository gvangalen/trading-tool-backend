import logging
import os
import traceback
import json
import requests
from urllib.parse import urljoin
from tenacity import retry, stop_after_attempt, wait_exponential, RetryError
from celery import shared_task

# ✅ Logging instellen
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ✅ Basisconfiguratie
API_BASE_URL = os.getenv("API_BASE_URL", "http://market_dashboard-api:5002/api")
TIMEOUT = 10
HEADERS = {"Content-Type": "application/json"}

# ✅ Retry wrapper
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

# ✅ Celery taak: Opslaan technische data via POST /technical_data
@shared_task(name="celery_task.technical_task.save_technical_data")
def save_technical_data(symbol, rsi, volume, ma_200, timeframe="1D"):
    payload = {
        "symbol": symbol,
        "rsi": rsi,
        "volume": volume,
        "ma_200": ma_200,
        "timeframe": timeframe
    }
    try:
        url = urljoin(API_BASE_URL, "/technical_data")
        data = safe_request(url, method="POST", payload=payload)
        logger.info(f"✅ Technische data opgeslagen: {data}")
    except RetryError:
        logger.error("❌ Alle retries mislukt voor save_technical_data!")
        logger.error(traceback.format_exc())
