import os
import logging
import traceback
import requests
from tenacity import retry, stop_after_attempt, wait_exponential
from celery import shared_task

from backend.ai_agents.strategy_ai_agent import generate_strategy_from_setup

# ---------------------------------------------------------
# 🔧 Config + Logging
# ---------------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

API_BASE_URL = os.getenv("API_BASE_URL", "http://127.0.0.1:5002/api")
HEADERS = {"Content-Type": "application/json"}
TIMEOUT = 10


# ---------------------------------------------------------
# 🔁 Safe Request Helper
# ---------------------------------------------------------
@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=3, max=12), reraise=True)
def safe_request(url, method="GET", payload=None):
    response = requests.request(
        method=method,
        url=url,
        json=payload,
        headers=HEADERS,
        timeout=TIMEOUT,
    )
    response.raise_for_status()
    return response.json()


# ---------------------------------------------------------
# 📦 Payload builder voor /strategies
# ---------------------------------------------------------
def build_payload(setup, strategy):
    return {
        "setup_id": setup["id"],
        "setup_name": setup.get("name"),
        "strategy_type": (setup.get("strategy_type") or "ai").lower(),
        "symbol": setup.get("symbol", "BTC"),
        "timeframe": setup.get("timeframe", "1D"),
        "score": setup.get("score", 0),

        "ai_explanation": strategy.get("explanation"),
        "risk_reward": strategy.get("risk_reward"),
        "entry": strategy.get("entry"),
        "targets": strategy.get("targets"),
        "stop_loss": strategy.get("stop_loss"),
    }


# ---------------------------------------------------------
# 🚀 AI STRATEGY GENERATION (DEZE IS DE BELANGRIJKE)
# ---------------------------------------------------------
@shared_task(name="backend.celery_task.strategy_task.generate_for_setup")
def generate_for_setup(user_id: int, setup_id: int, overwrite: bool = True):
    try:
        logger.info(f"🚀 AI strategie genereren | user={user_id} setup={setup_id}")

        # 1️⃣ Setup ophalen
        setup_url = f"{API_BASE_URL}/{user_id}/setups/{setup_id}"
        setup = safe_request(setup_url, "GET")

        # 2️⃣ AI strategie genereren
        logger.info("🧠 AI strategie agent starten...")
        strategy = generate_strategy_from_setup(setup, user_id=user_id)

        if not strategy:
            raise ValueError("AI gaf geen strategie terug")

        # 3️⃣ Opslaan via API
        payload = build_payload(setup, strategy)
        save_url = f"{API_BASE_URL}/{user_id}/strategies"

        result = safe_request(save_url, "POST", payload)

        logger.info("✅ AI strategie succesvol opgeslagen")

        return {
            "state": "SUCCESS",
            "success": True,
            "strategy": result,
        }

    except Exception as e:
        logger.error("❌ Fout in generate_for_setup")
        logger.error(traceback.format_exc())

        return {
            "state": "FAILURE",
            "success": False,
            "error": str(e),
        }


# ---------------------------------------------------------
# 🔄 (OPTIONEEL) bulk generatie — voorlopig uit
# ---------------------------------------------------------
@shared_task(name="backend.celery_task.strategy_task.generate_all")
def generate_all(user_id: int):
    return {
        "state": "FAILURE",
        "success": False,
        "error": "Bulk AI strategie-generatie nog niet geactiveerd",
    }
