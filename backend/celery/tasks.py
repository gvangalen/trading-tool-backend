from celery import Celery
import requests
import logging
import os
import traceback
import json
from urllib.parse import urljoin
from tenacity import retry, stop_after_attempt, wait_exponential, RetryError
from ai_strategy_generator import generate_strategy, generate_strategy_from_setup  # ✅ AI generator

# ✅ Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ✅ Celery
celery = Celery(
    "celery_worker",
    broker=os.getenv("CELERY_BROKER_URL", "redis://redis:6379/0"),
    backend=os.getenv("CELERY_RESULT_BACKEND", "redis://redis:6379/0"),
)
celery.conf.timezone = "UTC"
celery.conf.enable_utc = True

# ✅ Config
API_BASE_URL = os.getenv("API_BASE_URL", "http://market_dashboard-api:5002/api")
CONFIG_PATH = os.getenv("MACRO_CONFIG_PATH", "macro_indicators_config.json")
TIMEOUT = 10
HEADERS = {"Content-Type": "application/json"}

# ✅ Safe API call
@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=5, max=20), reraise=True)
def safe_request(url, method="POST", payload=None):
    try:
        response = requests.request(method, url, json=payload, headers=HEADERS, timeout=TIMEOUT)
        response.raise_for_status()
        logging.info(f"✅ API-call succesvol: {url}")
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"❌ RequestException bij {url}: {e}")
        logging.error(traceback.format_exc())
        raise
    except Exception as e:
        logging.error(f"⚠️ Onverwachte fout bij {url}: {e}")
        logging.error(traceback.format_exc())
        raise

# ✅ Marktdata
@celery.task(name="celery_worker.fetch_market_data")
def fetch_market_data():
    try:
        data = safe_request(urljoin(API_BASE_URL, "/save_market_data"))
        logging.info(f"✅ Marktdata succesvol opgeslagen: {data}")
    except RetryError:
        logging.error("❌ Alle retries mislukt voor fetch_market_data!")
        logging.error(traceback.format_exc())

# ✅ Macrodata
@celery.task(name="celery_worker.fetch_macro_data")
def fetch_macro_data():
    logging.info("📡 Start ophalen macrodata")

    if not os.path.exists(CONFIG_PATH):
        logging.error(f"❌ Configbestand niet gevonden: {CONFIG_PATH}")
        return

    try:
        with open(CONFIG_PATH) as f:
            config = json.load(f)
    except Exception as e:
        logging.error(f"❌ Fout bij laden config: {e}")
        return

    for name in config.keys():
        try:
            response = safe_request(urljoin(API_BASE_URL, "/macro_data"), method="POST", payload={"name": name})
            logging.info(f"✅ Macrodata '{name}' opgeslagen: {response}")
        except RetryError:
            logging.error(f"❌ Alle retries mislukt voor macrodata '{name}'")
        except Exception as e:
            logging.error(f"❌ Fout bij macrodata '{name}': {e}")

# ✅ Technische data
@celery.task(name="celery_worker.fetch_technical_data")
def fetch_technical_data():
    try:
        payload = {"symbol": "BTC"}
        data = safe_request(urljoin(API_BASE_URL, "/save_technical_data"), method="POST", payload=payload)
        logging.info(f"✅ Technische data succesvol opgeslagen: {data}")
    except RetryError:
        logging.error("❌ Alle retries mislukt voor fetch_technical_data!")
        logging.error(traceback.format_exc())

# ✅ Strategieën automatisch genereren
@celery.task(name="celery_worker.generate_strategieën_automatisch")
def generate_strategieën_automatisch():
    try:
        setups = safe_request(urljoin(API_BASE_URL, "/setups"), method="GET")
        if not setups:
            logging.warning("⚠️ Geen setups gevonden")
            return

        for setup in setups:
            if setup.get("strategy_generated"):
                continue
            if not setup.get("name") or not setup.get("symbol"):
                logging.warning(f"⚠️ Setup incompleet: {setup}")
                continue

            strategie = generate_strategy(setup)
            if not strategie:
                logging.warning(f"⚠️ AI kon geen strategie genereren voor {setup['name']}")
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
                logging.info(f"✅ Strategie opgeslagen: {setup['name']} → {result}")
            except Exception as e:
                logging.error(f"❌ Fout bij opslaan strategie: {e}")

    except Exception as e:
        logging.error(f"❌ Fout in generate_strategieën_automatisch: {e}")
        logging.error(traceback.format_exc())

# ✅ Strategie genereren voor specifieke setup
@celery.task(name="celery_worker.generate_strategie_voor_setup")
def generate_strategie_voor_setup(setup_id, overwrite=True):
    try:
        logging.info(f"🔍 Setup ophalen: {setup_id}")
        setup_res = requests.get(urljoin(API_BASE_URL, f"/setups/{setup_id}"))
        if setup_res.status_code != 200:
            logging.error(f"❌ Setup niet gevonden: {setup_id}")
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

        logging.info("✅ Strategie succesvol opgeslagen")
        return {"success": True, "strategie": payload}

    except Exception as e:
        logging.error(f"❌ Fout bij strategie generatie voor setup: {e}")
        return {"error": str(e)}

# ✅ Debug start
if __name__ == "__main__":
    logging.info("🚀 Celery Task-module gestart!")
