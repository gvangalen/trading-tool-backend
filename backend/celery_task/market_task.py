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
        try:
            return response.json()
        except Exception:
            logger.warning("⚠️ API-call gaf geen geldige JSON terug.")
            return {"message": "Non-JSON response"}
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ RequestException bij {url}: {e}")
        logger.error(traceback.format_exc())
        raise
    except Exception as e:
        logger.error(f"⚠️ Onverwachte fout bij {url}: {e}")
        logger.error(traceback.format_exc())
        raise

# ✅ Celery taak: Marktdata ophalen en opslaan
@shared_task(name="celery_task.market_task.fetch_market_data")
def fetch_market_data():
    logger.info("🌍 Marktdata ophalen gestart...")
    try:
        url = urljoin(API_BASE_URL, "/save_market_data")
        response = safe_request(url)
        logger.info(f"✅ Marktdata succesvol opgeslagen: {response}")
    except RetryError:
        logger.error("❌ Alle retries mislukt voor fetch_market_data!")
        logger.error(traceback.format_exc())
    except Exception as e:
        logger.error(f"❌ Onverwachte fout bij fetch_market_data: {e}")
        logger.error(traceback.format_exc())
