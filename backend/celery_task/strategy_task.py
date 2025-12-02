import os
import logging
import traceback
import json
import requests
from tenacity import retry, stop_after_attempt, wait_exponential
from celery import shared_task


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
# 📦 Bestaat strategy al?
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
# 📦 Payload builder
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
# 🚀 Strategy genereren voor één setup (AI-knop functie)
# ---------------------------------------------------------
@shared_task(name="backend.celery_task.strategy_task.generate_for_setup")
def generate_strategie_voor_setup(setup_id, overwrite=True):
    try:
        # 1. Setup ophalen
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

        # ⚠️ 2. GEEN AI strategie kanaal meer
        # -> strategie wordt niet automatisch gemaakt
        return {
            "state": "FAILURE",
            "success": False,
            "error": "AI strategie-generatie is uitgeschakeld"
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
# 🔄 Alles genereren (voor later)
# ---------------------------------------------------------
@shared_task(name="backend.celery_task.strategy_task.generate_all")
def generate_strategieën_automatisch():
    return {
        "state": "FAILURE",
        "success": False,
        "error": "Automatische strategie-generatie is uitgeschakeld"
    }
