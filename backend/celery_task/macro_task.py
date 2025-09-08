import os
import logging
import traceback
import requests
from tenacity import retry, stop_after_attempt, wait_exponential, RetryError
from celery import shared_task

# ✅ Logging instellen
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ✅ Configuratie
API_BASE_URL = os.getenv("API_BASE_URL", "http://127.0.0.1:5002/api")
TIMEOUT = 10
HEADERS = {"Content-Type": "application/json"}


# ✅ Robuuste API-aanroep met retry-logica
@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=5, max=20), reraise=True)
def safe_request(url, method="POST", payload=None):
    try:
        response = requests.request(method, url, json=payload, headers=HEADERS, timeout=TIMEOUT)
        response.raise_for_status()
        logger.info(f"✅ API-call succesvol: {url}")
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Fout bij verzoek naar {url}: {e}")
        logger.error(traceback.format_exc())
        raise
    except Exception as e:
        logger.error(f"⚠️ Onverwachte fout bij {url}: {e}")
        logger.error(traceback.format_exc())
        raise


# ✅ Celery taak: Macro-indicator doorsturen naar macro API (alleen naam!)
@shared_task(name="backend.celery_task.macro_task.enqueue_macro_indicator")
def enqueue_macro_indicator(indicator_name: str):
    url = f"{API_BASE_URL}/macro_data"
    payload = {"name": indicator_name}
    try:
        response = safe_request(url, method="POST", payload=payload)
        logger.info(f"✅ Macro-indicator '{indicator_name}' verwerkt: {response}")
    except RetryError:
        logger.error(f"❌ Alle retries mislukt voor '{indicator_name}'")
    except Exception as e:
        logger.error(f"❌ Fout bij verwerken '{indicator_name}': {e}")
        logger.error(traceback.format_exc())


# ✅ Verzameltaak: alle relevante macro-indicatoren aanroepen via config-gebaseerde API
@shared_task(name="backend.celery_task.macro_task.fetch_macro_data")
def fetch_macro_data():
    logger.info("🌍 Start ophalen macrodata...")

    # Deze lijst moet matchen met je macro_indicators_config.json
    indicator_namen = ["Fear & Greed Index", "S&P500", "DXY"]

    for name in indicator_namen:
        enqueue_macro_indicator.delay(name)
