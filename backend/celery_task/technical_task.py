import os
import logging
import traceback
from urllib.parse import urljoin
from celery import shared_task
from tenacity import retry, stop_after_attempt, wait_exponential, RetryError
import requests

# ✅ Logging instellen
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ✅ Configuratie
API_BASE_URL = os.getenv("API_BASE_URL", "http://market_dashboard-api:5002/api")
TIMEOUT = 10
HEADERS = {"Content-Type": "application/json"}

# ✅ Robuuste API-call met retries
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

# ✅ Celery taak: Technische data opslaan via API
@shared_task(name="celery_task.technical_task.save_technical_data_task")
def save_technical_data_task(symbol, rsi, volume, ma_200, timeframe="1D"):
    payload = {
        "symbol": symbol,
        "rsi": rsi,
        "volume": volume,
        "ma_200": ma_200,
        "timeframe": timeframe
    }
    try:
        url = urljoin(API_BASE_URL, "/technical_data")
        response = safe_request(url, method="POST", payload=payload)
        logger.info(f"✅ Technische data succesvol opgeslagen: {response}")
    except RetryError:
        logger.error("❌ Alle retries mislukt voor save_technical_data_task!")
        logger.error(traceback.format_exc())
    except Exception as e:
        logger.error(f"❌ Onverwachte fout bij opslaan technische data: {e}")
        logger.error(traceback.format_exc())
