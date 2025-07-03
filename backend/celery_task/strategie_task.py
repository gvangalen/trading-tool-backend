import logging
import os
import traceback
import requests
from urllib.parse import urljoin
from tenacity import retry, stop_after_attempt, wait_exponential, RetryError
from celery import shared_task
from ai_strategy_generator import generate_strategy, generate_strategy_from_setup

# ✅ Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

API_BASE_URL = os.getenv("API_BASE_URL", "http://market_dashboard-api:5002/api")
HEADERS = {"Content-Type": "application/json"}

# ✅ Safe API call
@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=5, max=20), reraise=True)
def safe_request(url, method="POST", payload=None):
    try:
        response = requests.request(method, url, json=payload, headers=HEADERS, timeout=10)
        response.raise_for_status()
        logger.info(f"✅ API-call succesvol: {url}")
        return response.json()
    except Exception as e:
        logger.error(f"❌ Fout bij API-call: {url}\n{e}")
        logger.error(traceback.format_exc())
        raise

# ✅ Taak: Genereer strategieën automatisch voor alle setups
@shared_task(name="celery_task.strategie_task.generate_all")
def generate_strategieën_automatisch():
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

# ✅ Taak: Genereer strategie voor specifieke setup
@shared_task(name="strategie.generate_for_setup")
def generate_strategie_voor_setup(setup_id, overwrite=True):
    try:
        logger.info(f"🔍 Setup ophalen: {setup_id}")
        setup_res = requests.get(urljoin(API_BASE_URL, f"/setups/{setup_id}"))
        if setup_res.status_code != 200:
            logger.error(f"❌ Setup niet gevonden: {setup_id}")
            return {"error": "Setup niet gevonden"}

        setup = setup_res.json()
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
