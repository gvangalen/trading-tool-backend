import os
import logging
import traceback
import requests
from datetime import datetime
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
        if response.status_code != 200:
            logger.error(f"❌ Foutstatus {response.status_code} van {url}: {response.text}")
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
        "score": score,
        "timestamp": datetime.utcnow().isoformat(),  # ✅ Toegevoegd
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
        fng_response = requests.get("https://api.alternative.me/fng/", timeout=TIMEOUT)
        if fng_response.status_code != 200:
            raise Exception(f"FNG statuscode {fng_response.status_code}")
        data = fng_response.json()
        value = int(data.get("data", [{}])[0].get("value", 0))
        save_macro_data_task.delay(indicator="Fear & Greed Index", value=value)
        logger.info(f"✅ Fear & Greed opgeslagen: {value}")
    except Exception as e:
        logger.warning(f"❌ Fout bij ophalen Fear & Greed: {e}")
        logger.warning(traceback.format_exc())

    # ✅ 2. S&P500 (Yahoo Finance)
    try:
        sp_response = requests.get("https://query1.finance.yahoo.com/v8/finance/chart/^GSPC?interval=1d&range=1d", timeout=TIMEOUT)
        if sp_response.status_code != 200:
            raise Exception(f"S&P500 statuscode {sp_response.status_code}")
        sp_data = sp_response.json()
        price = sp_data.get("chart", {}).get("result", [{}])[0].get("meta", {}).get("regularMarketPrice")
        if price is not None:
            save_macro_data_task.delay(indicator="S&P500", value=round(price, 2))
            logger.info(f"✅ S&P500 opgeslagen: {price}")
        else:
            raise ValueError("S&P500 prijs niet gevonden in response.")
    except Exception as e:
        logger.warning(f"❌ Fout bij ophalen S&P500: {e}")
        logger.warning(traceback.format_exc())

    # ✅ 3. DXY (Yahoo Finance)
    try:
        dxy_response = requests.get("https://query1.finance.yahoo.com/v8/finance/chart/DX-Y.NYB?interval=1d&range=1d", timeout=TIMEOUT)
        if dxy_response.status_code != 200:
            raise Exception(f"DXY statuscode {dxy_response.status_code}")
        dxy_data = dxy_response.json()
        price = dxy_data.get("chart", {}).get("result", [{}])[0].get("meta", {}).get("regularMarketPrice")
        if price is not None:
            save_macro_data_task.delay(indicator="DXY", value=round(price, 2))
            logger.info(f"✅ DXY opgeslagen: {price}")
        else:
            raise ValueError("DXY prijs niet gevonden in response.")
    except Exception as e:
        logger.warning(f"❌ Fout bij ophalen DXY: {e}")
        logger.warning(traceback.format_exc())
