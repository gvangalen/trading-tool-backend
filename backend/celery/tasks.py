from celery import Celery
import requests
import logging
import os
import traceback
import json
from urllib.parse import urljoin
from tenacity import retry, stop_after_attempt, wait_exponential, RetryError
from ai_strategy_generator import generate_strategy, generate_strategy_from_setup

# ✅ Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ✅ Celery-configuratie
celery = Celery(
    "celery_worker",
    broker=os.getenv("CELERY_BROKER_URL", "redis://redis:6379/0"),
    backend=os.getenv("CELERY_RESULT_BACKEND", "redis://redis:6379/0"),
)
celery.conf.timezone = "UTC"
celery.conf.enable_utc = True

# ✅ API-config
API_BASE_URL = os.getenv("API_BASE_URL", "http://market_dashboard-api:5002/api")
CONFIG_PATH = os.getenv("MACRO_CONFIG_PATH", "macro_indicators_config.json")
TIMEOUT = 10
HEADERS = {"Content-Type": "application/json"}

# ✅ Safe API-request
@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=5, max=20), reraise=True)
def safe_request(url, method="POST", payload=None):
    try:
        response = requests.request(method, url, json=payload, headers=HEADERS, timeout=TIMEOUT)
        response.raise_for_status()
        logging.info(f"✅ API-call succesvol: {url}")
        return response.json()
    except Exception as e:
        logging.error(f"❌ Fout bij API-call naar {url}: {e}")
        logging.error(traceback.format_exc())
        raise

# === 📈 Marktdata ophalen ===
@celery.task(name="celery_worker.fetch_market_data")
def fetch_market_data():
    try:
        data = safe_request(urljoin(API_BASE_URL, "/save_market_data"))
        logging.info(f"✅ Marktdata opgeslagen: {data}")
    except RetryError:
        logging.error("❌ Alle retries mislukt voor fetch_market_data")

# === 🌍 Macrodata ophalen op basis van config ===
@celery.task(name="celery_worker.fetch_macro_data")
def fetch_macro_data():
    if not os.path.exists(CONFIG_PATH):
        logging.error(f"❌ Configbestand niet gevonden: {CONFIG_PATH}")
        return

    try:
        with open(CONFIG_PATH) as f:
            config = json.load(f)
    except Exception as e:
        logging.error(f"❌ Config laden mislukt: {e}")
        return

    for name in config:
        try:
            result = safe_request(urljoin(API_BASE_URL, "/macro_data"), payload={"name": name})
            logging.info(f"✅ Macrodata opgeslagen: {name}")
        except RetryError:
            logging.error(f"❌ Mislukt voor macrodata '{name}'")

# === 📊 Technische data ophalen ===
@celery.task(name="celery_worker.fetch_technical_data")
def fetch_technical_data():
    try:
        result = safe_request(urljoin(API_BASE_URL, "/save_technical_data"), payload={"symbol": "BTC"})
        logging.info(f"✅ Technische data opgeslagen: {result}")
    except RetryError:
        logging.error("❌ Alle retries mislukt voor fetch_technical_data")

# === 🤖 Automatisch strategieën genereren ===
@celery.task(name="celery_worker.generate_strategieën_automatisch")
def generate_strategieën_automatisch():
    try:
        setups = safe_request(urljoin(API_BASE_URL, "/setups"), method="GET")
        if not setups:
            logging.warning("⚠️ Geen setups gevonden")
            return

        for setup in setups:
            if setup.get("strategy_generated") or not setup.get("name") or not setup.get("symbol"):
                continue

            strategie = generate_strategy(setup)
            if not strategie:
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
                result = safe_request(urljoin(API_BASE_URL, "/strategieën"), payload=payload)
                logging.info(f"✅ Strategie opgeslagen: {setup['name']}")
            except Exception as e:
                logging.error(f"❌ Opslaan strategie mislukt: {e}")

    except Exception as e:
        logging.error(f"❌ Fout in generate_strategieën_automatisch: {e}")
        logging.error(traceback.format_exc())

# === 🧠 Genereer strategie voor één specifieke setup ===
@celery.task(name="celery_worker.generate_strategie_voor_setup")
def generate_strategie_voor_setup(setup_id, overwrite=True):
    try:
        res = requests.get(urljoin(API_BASE_URL, f"/setups/{setup_id}"))
        if res.status_code != 200:
            return {"error": "Setup niet gevonden"}

        setup = res.json()
        strategie = generate_strategy_from_setup(setup)
        if not strategie:
            return {"error": "Generatie mislukt"}

        payload = {
            "setup_name": setup["name"],
            "asset": setup["symbol"],
            "timeframe": setup["timeframe"],
            "score": setup.get("score", 0),
            "entry": strategie["entry"],
            "targets": strategie["targets"],
            "stop_loss": strategie["stop_loss"],
            "risk_reward": strategie["risk_reward"],
            "explanation": strategie["explanation"],
            "type": "Auto gegenereerd"
        }

        if overwrite:
            result = requests.put(urljoin(API_BASE_URL, f"/strategieën/van_setup/{setup_id}"), json=payload)
        else:
            result = requests.post(urljoin(API_BASE_URL, "/strategieën"), json=payload)

        if result.status_code not in [200, 201]:
            return {"error": result.text}

        logging.info(f"✅ Strategie opgeslagen voor setup {setup_id}")
        return {"success": True}

    except Exception as e:
        logging.error(f"❌ Fout bij strategie-generatie voor setup: {e}")
        return {"error": str(e)}

# ✅ Debug-run
if __name__ == "__main__":
    logging.info("🚀 Celery taken handmatig gestart")
