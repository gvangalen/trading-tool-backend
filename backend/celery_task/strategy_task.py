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
@shared_task(name="backend.celery_task.strategy_task.generate_all")
def generate_strategieën_automatisch():
    try:
        logger.info("🚀 Start automatische strategie-generatie voor alle setups...")
        setups = safe_request(f"{API_BASE_URL}/setups", method="GET")

        if not isinstance(setups, list):
            logger.error(f"❌ /setups response is geen lijst: {setups}")
            return

        for setup in setups:
            setup_id = setup.get("id")
            if not setup_id:
                logger.warning(f"⚠️ Setup zonder id: {setup}")
                continue

            # Als je hier wilt overslaan als er al een strategy is, moet je eerst strategieën query'en.
            # Voor nu: altijd proberen 1 strategie per setup te maken.
            logger.info(f"🧩 Genereer strategie voor setup {setup_id} – {setup.get('name')}")

            strategie = generate_strategy_from_setup(setup)
            if not strategie:
                logger.warning(f"⚠️ AI kon geen strategie genereren voor '{setup.get('name')}'")
                continue

            strategy_type = setup.get("strategy_type", "manual").lower()

            payload = {
                "setup_id": setup_id,
                "setup_name": setup.get("name"),
                "strategy_type": strategy_type,
                "symbol": setup.get("symbol", "BTC"),
                "timeframe": setup.get("timeframe", "1D"),
                "score": setup.get("score", 0),
                "explanation": strategie.get("explanation"),
                "risk_reward": strategie.get("risk_reward"),
                "entry": strategie.get("entry"),
                "targets": strategie.get("targets"),
                "stop_loss": strategie.get("stop_loss"),
            }

            logger.info(f"📦 Strategie-payload (auto):\n{json.dumps(payload, indent=2, ensure_ascii=False)}")

            try:
                result = safe_request(f"{API_BASE_URL}/strategies", method="POST", payload=payload)
                logger.info(f"✅ Strategie opgeslagen voor {setup.get('name')}: {result}")
            except Exception as e:
                logger.error(f"❌ Fout bij opslaan strategie: {e}")
                logger.error(traceback.format_exc())

    except Exception as e:
        logger.error(f"❌ Fout in generate_strategieën_automatisch: {e}")
        logger.error(traceback.format_exc())


# ✅ Taak 2: Genereer strategie voor specifieke setup (knop in frontend)
@shared_task(name="backend.celery_task.strategy_task.generate_for_setup")
def generate_strategie_voor_setup(setup_id, overwrite=True):
    """
    Wordt aangeroepen via:
      POST /api/strategies/generate/{setup_id}
    vanaf de knop "Genereer strategie (AI)" in de frontend.
    """
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
            return {"error": "Ongeldige JSON voor setup"}

        logger.info(f"📄 Setup geladen voor strategie: {setup}")

        strategie = generate_strategy_from_setup(setup)
        if not strategie:
            logger.error("❌ Strategy Agent gaf geen resultaat terug.")
            return {"error": "Strategie-generatie mislukt"}

        strategy_type = (setup.get("strategy_type") or "manual").lower()

        payload = {
            "setup_id": setup_id,
            "setup_name": setup.get("name"),
            "strategy_type": strategy_type,
            "symbol": setup.get("symbol", "BTC"),
            "timeframe": setup.get("timeframe", "1D"),
            "score": setup.get("score", 0),
            "explanation": strategie.get("explanation"),
            "risk_reward": strategie.get("risk_reward"),
            "entry": strategie.get("entry"),
            "targets": strategie.get("targets"),
            "stop_loss": strategie.get("stop_loss"),
        }

        logger.info(f"📦 Strategie-payload (single):\n{json.dumps(payload, indent=2, ensure_ascii=False)}")

        # Voor nu: altijd via POST /strategies.
        # overwrite-flag kun je later gebruiken om een bestaande strategy op te zoeken en via PUT te updaten.
        res = requests.post(f"{API_BASE_URL}/strategies", json=payload, timeout=TIMEOUT)

        if res.status_code in (200, 201):
            logger.info("✅ Strategie succesvol opgeslagen (single).")
            return {"success": True, "strategie": payload}

        if res.status_code == 409:
            # Strategie bestaat al voor deze setup + type
            logger.warning(f"⚠️ Strategie bestaat al voor setup_id={setup_id} en type={strategy_type}: {res.text}")
            return {"warning": "Strategie bestaat al voor deze setup en type."}

        logger.error(f"❌ Strategie opslaan faalde: {res.status_code} - {res.text}")
        return {"error": res.text}

    except Exception as e:
        logger.error(f"❌ Fout bij strategie generatie voor setup: {e}")
        logger.error(traceback.format_exc())
        return {"error": str(e)}
