import os
import logging
import traceback
import requests
from urllib.parse import urljoin
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

# ✅ Celery taak: Macrodata opslaan via API
@shared_task(name="celery_task.macro_task.save_macro_data_task")
def save_macro_data_task(indicator, value, trend=None, interpretation=None, action=None, score=None):
    payload = {
        "indicator": indicator,
        "value": value,
        "trend": trend,
        "interpretation": interpretation,
        "action": action,
        "score": score
    }
    try:
        url = urljoin(API_BASE_URL, "/macro_data")
        response = safe_request(url, method="POST", payload=payload)
        logger.info(f"✅ Macrodata succesvol opgeslagen: {response}")
    except RetryError:
        logger.error("❌ Alle retries mislukt voor save_macro_data_task!")
        logger.error(traceback.format_exc())
    except Exception as e:
        logger.error(f"❌ Onverwachte fout bij opslaan macrodata: {e}")
        logger.error(traceback.format_exc())
