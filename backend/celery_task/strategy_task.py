import os
import logging
import traceback
import json
import requests
from tenacity import retry, stop_after_attempt, wait_exponential
from celery import shared_task
from backend.ai_agents.strategy_ai_agent import generate_strategy_from_setup

# ✅ Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

API_BASE_URL = os.getenv("API_BASE_URL", "http://127.0.0.1:5002/api")
HEADERS = {"Content-Type": "application/json"}
TIMEOUT = 10

# ✅ Robuuste fetch-functie met retries
@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=5, max=20), reraise=True)
def safe_request(url, method="POST", payload=None):
    try:
        response = requests.request(method, url, json=payload, headers=HEADERS, timeout=TIMEOUT)
        response.raise_for_status()
        logger.info(f"✅ API-call succesvol: {url}")
        return response.json()
    except Exception as e:
        logger.error(f"❌ Fout bij API-call: {url}\n{e}")
        logger.error(traceback.format_exc())
        raise

# ✅ Taak 1: Genereer strategieën voor alle setups
@shared_task(name="backend.celery_task.strategie_task.generate_all")
def generate_strategieën_automatisch():
    try:
        logger.info("🚀 Start automatische strategie-generatie voor alle setups...")
        setups = safe_request(f"{API_BASE_URL}/setups", method="GET")

        if not isinstance(setups, list):
            logger.error(f"❌ /setups response is geen lijst: {setups}")
            return

        for setup in setups:
            if setup.get("strategy_id"):
                logger.info(f"⏭️ Strategie al aanwezig voor setup '{setup.get('name')}' → overslaan.")
                continue

            if not setup.get("name") or not setup.get("symbol"):
                logger.warning(f"⚠️ Setup incompleet: {setup}")
                continue

            strategie = generate_strategy_from_setup(setup)
            if not strategie:
                logger.warning(f"⚠️ AI kon geen strategie genereren voor '{setup['name']}'")
                continue

            strategy_type = setup.get("strategy_type", "manual").lower()

            payload = {
                "setup_name": setup["name"],
                "strategy_type": strategy_type,
                "symbol": setup.get("symbol", "BTC"),
                "timeframe": setup.get("timeframe", "1D"),
                "score": setup.get("score", 0),
                "explanation": strategie.get("explanation"),
                "risk_reward": strategie.get("risk_reward"),
            }

            if strategy_type == "dca":
                payload.update({
                    "amount": strategie.get("amount", 0),
                    "frequency": strategie.get("frequency", "weekly"),
                })
            else:
                payload.update({
                    "entry": strategie.get("entry"),
                    "targets": strategie.get("targets"),
                    "stop_loss": strategie.get("stop_loss"),
                })

            logger.info(f"📦 Strategie-payload:\n{json.dumps(payload, indent=2)}")

            try:
                result = safe_request(f"{API_BASE_URL}/strategies", method="POST", payload=payload)
                logger.info(f"✅ Strategie opgeslagen voor {setup['name']}: {result}")
            except Exception as e:
                logger.error(f"❌ Fout bij opslaan strategie: {e}")
                logger.error(traceback.format_exc())

    except Exception as e:
        logger.error(f"❌ Fout in generate_strategieën_automatisch: {e}")
        logger.error(traceback.format_exc())

# ✅ Taak 2: Genereer strategie voor specifieke setup
@shared_task(name="backend.celery_task.strategie_task.generate_for_setup")
def generate_strategie_voor_setup(setup_id, overwrite=True):
    try:
        logger.info(f"🔍 Setup ophalen via ID: {setup_id}")
        res = requests.get(f"{API_BASE_URL}/setups/{setup_id}", timeout=TIMEOUT)

        if not res.ok:
            logger.error(f"❌ Setup ophalen mislukt: {res.status_code} - {res.text}")
            return {"error": "Setup niet gevonden"}

        try:
            setup = res.json()
        except Exception:
            logger.error("❌ Setup JSON kon niet worden ingelezen")
            return {"error": "Ongeldige JSON"}

        strategie = generate_strategy_from_setup(setup)
        if not strategie:
            return {"error": "Strategie-generatie mislukt"}

        strategy_type = setup.get("strategy_type", "manual").lower()

        payload = {
            "setup_name": setup.get("name"),
            "strategy_type": strategy_type,
            "symbol": setup.get("symbol"),
            "timeframe": setup.get("timeframe"),
            "score": setup.get("score"),
            "explanation": strategie.get("explanation"),
            "risk_reward": strategie.get("risk_reward"),
        }

        if strategy_type == "dca":
            payload.update({
                "amount": strategie.get("amount", 0),
                "frequency": strategie.get("frequency", "weekly"),
            })
        else:
            payload.update({
                "entry": strategie.get("entry"),
                "targets": strategie.get("targets"),
                "stop_loss": strategie.get("stop_loss"),
            })

        logger.info(f"📦 Strategie-payload:\n{json.dumps(payload, indent=2)}")

        if overwrite:
            res = requests.put(f"{API_BASE_URL}/strategies/van_setup/{setup_id}", json=payload, timeout=TIMEOUT)
        else:
            res = requests.post(f"{API_BASE_URL}/strategies", json=payload, timeout=TIMEOUT)

        if res.status_code not in [200, 201]:
            logger.error(f"❌ Strategie opslaan faalde: {res.status_code} - {res.text}")
            return {"error": res.text}

        logger.info("✅ Strategie succesvol opgeslagen.")
        return {"success": True, "strategie": payload}

    except Exception as e:
        logger.error(f"❌ Fout bij strategie generatie voor setup: {e}")
        logger.error(traceback.format_exc())
        return {"error": str(e)}
