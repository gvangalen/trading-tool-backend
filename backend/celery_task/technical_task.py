import logging
import os
import traceback
import requests
from urllib.parse import urljoin
from tenacity import retry, stop_after_attempt, wait_exponential, RetryError
from celery import shared_task

# ✅ Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

API_BASE_URL = os.getenv("API_BASE_URL", "http://market_dashboard-api:5002/api")
TIMEOUT = 10
HEADERS = {"Content-Type": "application/json"}

# ✅ Veilige API-call met retry
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

# ✅ Celery taak: Technische data ophalen en opslaan
@shared_task(name="celery_task.technical_task.fetch_technical_data")
def fetch_technical_data():
    logger.info("📈 Technische data ophalen gestart...")
    try:
        data = safe_request(urljoin(API_BASE_URL, "/save_technical_data"))
        logger.info(f"✅ Technische data succesvol opgeslagen: {data}")
    except RetryError:
        logger.error("❌ Alle retries mislukt voor fetch_technical_data!")
        logger.error(traceback.format_exc())
