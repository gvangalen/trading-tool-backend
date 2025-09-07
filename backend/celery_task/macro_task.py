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

# ✅ Celery taak: Macrodata opslaan via API
@shared_task(name="backend.celery_task.macro_task.save_macro_data_task")
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
        url = f"{API_BASE_URL}/macro_data"
        response = safe_request(url, method="POST", payload=payload)
        logger.info(f"✅ Macrodata succesvol opgeslagen: {response}")
    except RetryError:
        logger.error("❌ Alle retries mislukt voor save_macro_data_task!")
        logger.error(traceback.format_exc())
    except Exception as e:
        logger.error(f"❌ Onverwachte fout bij opslaan macrodata: {e}")
        logger.error(traceback.format_exc())

# ✅ Celery taak: Ophalen van macrodata (live API's)
@shared_task(name="backend.celery_task.macro_task.fetch_macro_data")
def fetch_macro_data():
    logger.info("🌍 Start ophalen macrodata...")

    # ✅ 1. Fear & Greed Index
    try:
        response = requests.get("https://api.alternative.me/fng/")
        data = response.json()
        value = int(data["data"][0]["value"])
        save_macro_data_task.delay(
            indicator="Fear & Greed Index",
            value=value
        )
        logger.info(f"✅ Fear & Greed opgeslagen: {value}")
    except Exception as e:
        logger.warning(f"❌ Fout bij Fear & Greed ophalen: {e}")

    # ✅ 2. S&P500 (Yahoo Finance)
    try:
        sp_response = requests.get("https://query1.finance.yahoo.com/v8/finance/chart/^GSPC?interval=1d&range=1d")
        sp_data = sp_response.json()
        price = sp_data["chart"]["result"][0]["meta"]["regularMarketPrice"]
        save_macro_data_task.delay(
            indicator="S&P500",
            value=round(price, 2)
        )
        logger.info(f"✅ S&P500 opgeslagen: {price}")
    except Exception as e:
        logger.warning(f"❌ Fout bij S&P500 ophalen: {e}")

    # ✅ 3. DXY (Yahoo Finance)
    try:
        dxy_response = requests.get("https://query1.finance.yahoo.com/v8/finance/chart/DX-Y.NYB?interval=1d&range=1d")
        dxy_data = dxy_response.json()
        price = dxy_data["chart"]["result"][0]["meta"]["regularMarketPrice"]
        save_macro_data_task.delay(
            indicator="DXY",
            value=round(price, 2)
        )
        logger.info(f"✅ DXY opgeslagen: {price}")
    except Exception as e:
        logger.warning(f"❌ Fout bij DXY ophalen: {e}")
