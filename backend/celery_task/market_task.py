import os
import logging
import traceback
import requests
from urllib.parse import urljoin
from tenacity import retry, stop_after_attempt, wait_exponential, RetryError
from celery import shared_task

# ✅ .env forceren laden
from dotenv import load_dotenv
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", "..", ".env"))

# ✅ Logging instellen
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ✅ API-config
API_BASE_URL = os.getenv("API_BASE_URL", "http://127.0.0.1:5002/api")
TIMEOUT = 10
HEADERS = {"Content-Type": "application/json"}
logger.info(f"🌍 API_BASE_URL geladen als: {API_BASE_URL}")

# ✅ Robuuste request-functie
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=5, max=20), reraise=True)
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
        raise
    except Exception as e:
        logger.error(f"⚠️ Onverwachte fout bij {url}: {e}")
        raise

# ✅ Task: Live BTC marktdata opslaan
@shared_task(name="celery_task.market_task.fetch_market_data")
def fetch_market_data():
    logger.info("📈 Taak gestart: live BTC-marktdata ophalen...")
    try:
        url = urljoin(API_BASE_URL, "/market_data/save")
        response = safe_request(url)
        logger.info(f"✅ Live marktdata opgeslagen: {response}")
    except RetryError:
        logger.error("❌ Alle retries mislukt voor fetch_market_data.")
        logger.error(traceback.format_exc())
    except Exception as e:
        logger.error(f"❌ Onverwachte fout tijdens fetch_market_data: {e}")
        logger.error(traceback.format_exc())

# ✅ ✅ ✅ Task: Nieuwe route voor 7-daagse BTC data gebruiken!
@shared_task(name="celery_task.market_task.save_market_data_7d")
def save_market_data_7d():
    logger.info("📊 Taak gestart: BTC 7-daagse marktdata vullen...")
    try:
        url = urljoin(API_BASE_URL, "/market_data/btc/7d/fill")  # ✅ nieuwe route!
        response = safe_request(url)
        logger.info(f"✅ 7-daagse BTC marktdata gevuld: {response}")
    except RetryError:
        logger.error("❌ Alle retries mislukt voor save_market_data_7d.")
        logger.error(traceback.format_exc())
    except Exception as e:
        logger.error(f"❌ Onverwachte fout tijdens save_market_data_7d: {e}")
        logger.error(traceback.format_exc())

# ✅ Task: Forward returns berekenen
@shared_task(name="celery_task.market_task.save_forward_returns")
def save_forward_returns():
    logger.info("📈 Taak gestart: forward returns berekenen...")
    try:
        url = urljoin(API_BASE_URL, "/market_data/forward/save")
        response = safe_request(url)
        logger.info(f"✅ Forward returns opgeslagen: {response}")
    except RetryError:
        logger.error("❌ Alle retries mislukt voor save_forward_returns.")
        logger.error(traceback.format_exc())
    except Exception as e:
        logger.error(f"❌ Onverwachte fout tijdens save_forward_returns: {e}")
        logger.error(traceback.format_exc())
