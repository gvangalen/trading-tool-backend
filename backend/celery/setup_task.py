# 📁 backend/celery/setup_task.py

from celery import Celery
import os
import logging
import traceback
import requests
from urllib.parse import urljoin
from tenacity import retry, stop_after_attempt, wait_exponential, RetryError
from ai_strategy_generator import generate_strategy, generate_strategy_from_setup

# ✅ Logging instellen
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ✅ Celery setup
celery = Celery(__name__)
celery.conf.broker_url = os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0")
celery.conf.result_backend = os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/0")

API_BASE_URL = os.getenv("API_BASE_URL", "http://market_dashboard-api:5002/api")
HEADERS = {"Content-Type": "application/json"}
TIMEOUT = 10

# ✅ Veilige request
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

# ✅ Strategie genereren voor alle setups
@celery.task(name="setup.generate_strategieën_automatisch")
def generate_strategieën_automatisch():
    logger.info("🤖 Start AI-strategie generatie voor alle setups")
    try:
        setups = safe_request(urljoin(API_BASE_URL, "/setups"), method="GET")
        if not setups:
            logger.warning("⚠️ Geen setups gevonden")
            return

        for setup in setups:
            if setup.get("strategy_generated"):
                continue
            if not setup.get("name") or not setup.get("symbol"):
                logger.warning(f"⚠️ Setup incompleet: {setup}")
                continue

            strategie = generate_strategy(setup)
            if not strategie:
                logger.warning(f"⚠️ AI kon geen strategie genereren voor {setup['name']}")
                continue

            payload = {
                "setup_name": setup["name"],
                "type": setup.get("strategy_type", "Auto gegenereerd"),
                "asset": setup.get("symbol", "BTC"),
                "timeframe": setup.get("timeframe", "1D"),
                "score": setup.get("score", 0),
                "entry": strategie["entry"],
                "targets": strategie["targets"],
                "stop_loss": strategie["stop_loss"],
                "risk_reward": strategie["risk_reward"],
                "explanation": strategie["explanation"]
            }

            try:
                result = safe_request(urljoin(API_BASE_URL, "/strategieën"), method="POST", payload=payload)
                logger.info(f"✅ Strategie opgeslagen: {setup['name']} → {result}")
            except Exception as e:
                logger.error(f"❌ Fout bij opslaan strategie: {e}")

    except Exception as e:
        logger.error(f"❌ Fout in generate_strategieën_automatisch: {e}")
        logger.error(traceback.format_exc())

# ✅ Strategie genereren voor specifieke setup
@celery.task(name="setup.generate_strategie_voor_setup")
def generate_strategie_voor_setup(setup_id, overwrite=True):
    logger.info(f"🔍 Setup ophalen: {setup_id}")
    try:
        res = requests.get(urljoin(API_BASE_URL, f"/setups/{setup_id}"))
        if res.status_code != 200:
            logger.error(f"❌ Setup niet gevonden: {setup_id}")
            return {"error": "Setup niet gevonden"}

        setup = res.json()
        strategie = generate_strategy_from_setup(setup)
        if not strategie:
            return {"error": "Strategie-generatie mislukt"}

        payload = {
            "setup_name": setup.get("name"),
            "asset": setup.get("symbol"),
            "timeframe": setup.get("timeframe"),
            "score": setup.get("score"),
            "entry": strategie.get("entry"),
            "targets": strategie.get("targets"),
            "stop_loss": strategie.get("stop_loss"),
            "risk_reward": strategie.get("risk_reward"),
            "explanation": strategie.get("explanation"),
            "type": "Auto gegenereerd"
        }

        if overwrite:
            res = requests.put(urljoin(API_BASE_URL, f"/strategieën/van_setup/{setup_id}"), json=payload)
        else:
            res = requests.post(urljoin(API_BASE_URL, "/strategieën"), json=payload)

        if res.status_code not in [200, 201]:
            return {"error": res.text}

        logger.info("✅ Strategie succesvol opgeslagen")
        return {"success": True, "strategie": payload}

    except Exception as e:
        logger.error(f"❌ Fout bij strategie generatie voor setup: {e}")
        return {"error": str(e)}
