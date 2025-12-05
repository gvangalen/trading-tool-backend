import os
import logging
import traceback
import json
import requests
from tenacity import retry, stop_after_attempt, wait_exponential
from celery import shared_task

# ---------------------------------------------------------
# 🔧 Config + Logging
# ---------------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

API_BASE_URL = os.getenv("API_BASE_URL", "http://127.0.0.1:5002/api")
HEADERS = {"Content-Type": "application/json"}
TIMEOUT = 10


# ---------------------------------------------------------
# 🔁 Safe Request Helper (GET/POST/PUT)
# ---------------------------------------------------------
@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=3, max=12), reraise=True)
def safe_request(url, method="GET", payload=None):
    try:
        response = requests.request(
            method=method,
            url=url,
            json=payload,
            headers=HEADERS,
            timeout=TIMEOUT
        )
        response.raise_for_status()
        return response.json()

    except Exception as e:
        logger.error(f"❌ API-call fout ({method} {url}): {e}")
        logger.error(traceback.format_exc())
        raise


# ---------------------------------------------------------
# 📦 Bestaat strategy al (per user)?
# ---------------------------------------------------------
def find_existing_strategy(user_id, setup_id, strategy_type):
    url = f"{API_BASE_URL}/{user_id}/strategies/by_setup/{setup_id}?type={strategy_type}"

    try:
        res = requests.get(url, timeout=TIMEOUT)

        if res.status_code == 404:
            return None

        return res.json()

    except Exception as e:
        logger.error(f"❌ Failed to check existing strategy: {e}")
        return None


# ---------------------------------------------------------
# 📦 Payload builder voor opslaan
# ---------------------------------------------------------
def build_payload(setup, strategie):
    """
    Bouwt de JSON om op te slaan in /strategies.
    De AI genereert dit, maar jouw versie heeft AI uit.
    """
    return {
        "setup_id": setup["id"],
        "setup_name": setup.get("name"),
        "strategy_type": (setup.get("strategy_type") or "manual").lower(),
        "symbol": setup.get("symbol", "BTC"),
        "timeframe": setup.get("timeframe", "1D"),
        "score": setup.get("score", 0),

        # AI (nu leeg)
        "ai_explanation": strategie.get("explanation"),
        "risk_reward": strategie.get("risk_reward"),
        "entry": strategie.get("entry"),
        "targets": strategie.get("targets"),
        "stop_loss": strategie.get("stop_loss"),
    }


# ---------------------------------------------------------
# 🚀 Strategy genereren voor één setup (met user_id)
# ---------------------------------------------------------
@shared_task(name="backend.celery_task.strategy_task.generate_for_setup")
def generate_for_setup(user_id, setup_id, overwrite=True):
    try:
        logger.info(f"🔍 Setup ophalen voor user={user_id}, setup_id={setup_id}")

        # 1. Setup ophalen via user-route
        setup_url = f"{API_BASE_URL}/{user_id}/setups/{setup_id}"
        setup_res = requests.get(setup_url, timeout=TIMEOUT)

        if not setup_res.ok:
            return {
                "state": "FAILURE",
                "success": False,
                "error": f"Setup niet gevonden ({setup_res.status_code})"
            }

        setup = setup_res.json()
        logger.info(f"📄 Setup geladen: {setup}")

        # -------------------------------------------------
        # ❌ AI-strategie generatie is uitgeschakeld (jouw keuze)
        # -------------------------------------------------
        return {
            "state": "FAILURE",
            "success": False,
            "error": "AI strategie-generatie is momenteel uitgeschakeld"
        }

    except Exception as e:
        logger.error(f"❌ Fout in generate_for_setup(): {e}")
        logger.error(traceback.format_exc())

        return {
            "state": "FAILURE",
            "success": False,
            "error": str(e)
        }


# ---------------------------------------------------------
# 🔄 Automatisch strategieën genereren (user-specific)
# ---------------------------------------------------------
@shared_task(name="backend.celery_task.strategy_task.generate_all")
def generate_all(user_id):
    """
    Automatische daily strategie-generatie.
    Momenteel bewust uitgeschakeld.
    """
    return {
        "state": "FAILURE",
        "success": False,
        "error": "Automatische strategie-generatie is momenteel uitgeschakeld"
    }
