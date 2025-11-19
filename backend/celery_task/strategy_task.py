import os
import logging
import traceback
import json
import requests
from tenacity import retry, stop_after_attempt, wait_exponential
from celery import shared_task

from backend.ai_agents.strategy_ai_agent import generate_strategy_from_setup

# ---------------------------------------------------------
# 🔧 Config + logging
# ---------------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

API_BASE_URL = os.getenv("API_BASE_URL", "http://127.0.0.1:5002/api")
HEADERS = {"Content-Type": "application/json"}
TIMEOUT = 10


# ---------------------------------------------------------
# 🔁 Safe request helper
# ---------------------------------------------------------
@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=5, max=20), reraise=True)
def safe_request(url, method="GET", payload=None):
    try:
        response = requests.request(
            method, url,
            json=payload,
            headers=HEADERS,
            timeout=TIMEOUT
        )
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"❌ API-call fout ({url}): {e}")
        logger.error(traceback.format_exc())
        raise


# ---------------------------------------------------------
# 🧩 Helper: check of strategy bestaat
# ---------------------------------------------------------
def find_existing_strategy(setup_id, strategy_type):
    try:
        url = f"{API_BASE_URL}/strategies/by_setup/{setup_id}?type={strategy_type}"
        res = requests.get(url, timeout=TIMEOUT)

        if res.status_code == 404:
            return None

        return res.json()
    except Exception:
        return None


# ---------------------------------------------------------
# ✨ Payload builder (AI → DB)
# ---------------------------------------------------------
def build_payload(setup, strategie):
    """
    Zorgt ervoor dat we NIET jouw handmatige explanation overschrijven.
    AI explanation gaat in ai_explanation.
    """
    return {
        "setup_id": setup["id"],
        "setup_name": setup.get("name"),
        "strategy_type": (setup.get("strategy_type") or "manual").lower(),
        "symbol": setup.get("symbol", "BTC"),
        "timeframe": setup.get("timeframe", "1D"),
        "score": setup.get("score", 0),

        # 🔥 LET OP: AI explanation gaat in ai_explanation
        # jouw handmatige explanation blijft bestaan
        "ai_explanation": strategie.get("explanation"),

        "risk_reward": strategie.get("risk_reward"),
        "entry": strategie.get("entry"),
        "targets": strategie.get("targets"),
        "stop_loss": strategie.get("stop_loss"),
    }


# ---------------------------------------------------------
# 🚀 AI Strategy genereren voor 1 setup
# ---------------------------------------------------------
@shared_task(name="backend.celery_task.strategy_task.generate_for_setup")
def generate_strategie_voor_setup(setup_id, overwrite=True):
    try:
        logger.info(f"🔍 Setup ophalen via /setups/{setup_id}")
        res = requests.get(f"{API_BASE_URL}/setups/{setup_id}", timeout=TIMEOUT)

        if not res.ok:
            logger.error(f"❌ Setup ophalen mislukt: {res.status_code} - {res.text}")
            return {"error": "Setup niet gevonden"}

        setup = res.json()
        logger.info(f"📄 Setup geladen: {setup}")

        # -----------------------------------------------------
        # 🔍 AI strategie genereren
        # -----------------------------------------------------
        strategie = generate_strategy_from_setup(setup)
        if not strategie:
            return {"error": "AI kon geen strategie genereren"}

        payload = build_payload(setup, strategie)
        strategy_type = payload["strategy_type"]

        # -----------------------------------------------------
        # 🔍 Check of strategy bestaat
        # -----------------------------------------------------
        existing = find_existing_strategy(setup_id, strategy_type)

        if existing and not overwrite:
            return {"warning": "Strategie bestaat al en overwrite=False"}

        # -----------------------------------------------------
        # ✏ UPDATE BESTAANDE STRATEGIE
        # -----------------------------------------------------
        if existing and overwrite:
            strategy_id = existing["id"]
            logger.info(f"✏ Updaten van bestaande strategie ID={strategy_id}")

            res = requests.put(
                f"{API_BASE_URL}/strategies/{strategy_id}",
                json=payload,
                timeout=TIMEOUT
            )

            if res.status_code in (200, 201):
                logger.info("✅ Strategie bijgewerkt")
                return {"success": True, "updated": True, "strategy": payload}

            logger.error(f"❌ Update mislukt: {res.status_code} {res.text}")
            return {"error": "Bijwerken mislukt"}

        # -----------------------------------------------------
        # ➕ CREATE Nieuwe strategie
        # -----------------------------------------------------
        logger.info("➕ Nieuwe strategie aanmaken")
        res = requests.post(
            f"{API_BASE_URL}/strategies",
            json=payload,
            timeout=TIMEOUT
        )

        if res.status_code in (200, 201):
            logger.info("✅ Nieuwe strategie aangemaakt")
            return {"success": True, "created": True, "strategy": payload}

        logger.error(f"❌ Aanmaken mislukt: {res.status_code} {res.text}")
        return {"error": "Aanmaken mislukt"}

    except Exception as e:
        logger.error(f"❌ Fout in generate_strategie_voor_setup: {e}")
        logger.error(traceback.format_exc())
        return {"error": str(e)}


# ---------------------------------------------------------
# 🔄 Automatisch strategieën genereren voor alle setups
# ---------------------------------------------------------
@shared_task(name="backend.celery_task.strategy_task.generate_all")
def generate_strategieën_automatisch():
    try:
        logger.info("🚀 Automatische strategie-generatie gestart")

        setups = safe_request(f"{API_BASE_URL}/setups", method="GET")
        if not isinstance(setups, list):
            return {"error": "Ongeldige response van /setups"}

        for setup in setups:
            setup_id = setup["id"]
            logger.info(f"🔁 Strategy genereren voor setup {setup_id}")

            generate_strategie_voor_setup(setup_id, overwrite=True)

        return {"success": True}

    except Exception as e:
        logger.error(f"❌ Fout generate_strategieën_automatisch: {e}")
        logger.error(traceback.format_exc())
        return {"error": str(e)}
