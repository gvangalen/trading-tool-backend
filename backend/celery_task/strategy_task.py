import os
import logging
import traceback
import json
import requests
from tenacity import retry, stop_after_attempt, wait_exponential
from celery import shared_task

# 👉 Nieuwe correcte import (oude bestaat niet meer)
from backend.ai_agents.strategy_ai_agent import analyze_strategies_for_setup

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
@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=3, max=12), reraise=True)
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
    except Exception as e:
        logger.error("❌ Failed to check existing strategy")
        return None


# ---------------------------------------------------------
# ✨ Payload builder (AI → DB)
# ---------------------------------------------------------
def build_payload(setup, strategie):
    return {
        "setup_id": setup["id"],
        "setup_name": setup.get("name"),
        "strategy_type": (setup.get("strategy_type") or "manual").lower(),
        "symbol": setup.get("symbol", "BTC"),
        "timeframe": setup.get("timeframe", "1D"),
        "score": setup.get("score", 0),

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
        # -----------------------------------------------------
        # 1. SETUP OPHALEN
        # -----------------------------------------------------
        logger.info(f"🔍 Setup ophalen via /setups/{setup_id}")
        res = requests.get(f"{API_BASE_URL}/setups/{setup_id}", timeout=TIMEOUT)

        if not res.ok:
            return {
                "state": "FAILURE",
                "success": False,
                "error": f"Setup niet gevonden ({res.status_code})"
            }

        setup = res.json()
        logger.info(f"📄 Setup geladen: {setup}")

        # -----------------------------------------------------
        # 2. AI STRATEGIE ANALYSE (GEEN nieuwe strategie genereren!)
        # -----------------------------------------------------
        strategie = analyze_strategies_for_setup(setup)
        if not strategie:
            return {
                "state": "FAILURE",
                "success": False,
                "error": "AI kon geen strategie-analyse maken"
            }

        payload = build_payload(setup, strategie)
        strategy_type = payload["strategy_type"]

        # -----------------------------------------------------
        # 3. CHECK BESTAANDE STRATEGIE
        # -----------------------------------------------------
        existing = find_existing_strategy(setup_id, strategy_type)
        existing_id = None

        if existing and existing.get("exists") and existing.get("strategy"):
            existing_id = existing["strategy"].get("id")

        # -----------------------------------------------------
        # 4. UPDATE BESTAANDE STRATEGIE
        # -----------------------------------------------------
        if existing_id and overwrite:
            logger.info(f"✏ Update bestaande strategy ID={existing_id}")

            result = safe_request(
                f"{API_BASE_URL}/strategies/{existing_id}",
                method="PUT",
                payload=payload
            )

            return {
                "state": "SUCCESS",
                "success": True,
                "updated": True,
                "created": False,
                "strategy": result
            }

        # -----------------------------------------------------
        # 5. CREATE NIEUWE STRATEGIE
        # -----------------------------------------------------
        logger.info("➕ Nieuwe strategie aanmaken")

        result = safe_request(
            f"{API_BASE_URL}/strategies",
            method="POST",
            payload=payload
        )

        return {
            "state": "SUCCESS",
            "success": True,
            "updated": False,
            "created": True,
            "strategy": result
        }

    except Exception as e:
        logger.error(f"❌ Fout in generate_strategie_voor_setup: {e}")
        logger.error(traceback.format_exc())

        return {
            "state": "FAILURE",
            "success": False,
            "error": str(e)
        }


# ---------------------------------------------------------
# 🔄 Automatisch strategieën genereren voor alle setups
# ---------------------------------------------------------
@shared_task(name="backend.celery_task.strategy_task.generate_all")
def generate_strategieën_automatisch():
    try:
        logger.info("🚀 Automatische strategie-generatie gestart")

        setups = safe_request(f"{API_BASE_URL}/setups", method="GET")
        if not isinstance(setups, list):
            return {"state": "FAILURE", "success": False, "error": "Ongeldige setup respons"}

        results = []

        for setup in setups:
            setup_id = setup["id"]
            logger.info(f"🔁 Strategie genereren voor setup {setup_id}")

            result = generate_strategie_voor_setup(setup_id, overwrite=True)
            results.append(result)

        return {
            "state": "SUCCESS",
            "success": True,
            "results": results
        }

    except Exception as e:
        logger.error(f"❌ Fout generate_strategieën_automatisch: {e}")
        return {
            "state": "FAILURE",
            "success": False,
            "error": str(e)
        }
