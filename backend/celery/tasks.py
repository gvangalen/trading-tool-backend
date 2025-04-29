from celery import Celery
import requests
import logging
import os
import traceback
import json
from tenacity import retry, stop_after_attempt, wait_exponential, RetryError
from ai_strategy_generator import generate_strategy, generate_strategy_from_setup  # ✅ AI generator importeren

# ✅ Logging instellen
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ✅ Celery instellen
celery = Celery(
    "celery_worker",
    broker=os.getenv("CELERY_BROKER_URL", "redis://redis:6379/0"),
    backend=os.getenv("CELERY_RESULT_BACKEND", "redis://redis:6379/0"),
)

celery.conf.timezone = "UTC"
celery.conf.enable_utc = True

# ✅ API Base URL en config path
API_BASE_URL = os.getenv("API_BASE_URL", "http://market_dashboard-api:5002/api")
CONFIG_PATH = "macro_indicators_config.json"
TIMEOUT = 10

# ✅ Retry wrapper voor veilige API-calls
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=5, min=5, max=20),
    reraise=True,
)
def safe_request(url, method="POST", payload=None):
    headers = {"Content-Type": "application/json"}
    try:
        response = requests.request(method, url, json=payload, headers=headers, timeout=TIMEOUT)
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

# ✅ Marktdata ophalen
@celery.task(name="celery_worker.fetch_market_data")
def fetch_market_data():
    try:
        data = safe_request(f"{API_BASE_URL}/save_market_data")
        logging.info(f"✅ Marktdata succesvol opgeslagen: {data}")
    except RetryError:
        logging.error("❌ Alle retries mislukt voor fetch_market_data!")
        logging.error(traceback.format_exc())

# ✅ Macrodata ophalen (via config)
@celery.task(name="celery_worker.fetch_macro_data")
def fetch_macro_data():
    logging.info("📡 Start ophalen macrodata")

    if not os.path.exists(CONFIG_PATH):
        logging.error("❌ macro_indicators_config.json niet gevonden")
        return

    try:
        with open(CONFIG_PATH) as f:
            config = json.load(f)
    except Exception as e:
        logging.error(f"❌ Config laden mislukt: {e}")
        return

    for name in config.keys():
        try:
            response = safe_request(f"{API_BASE_URL}/macro_data", method="POST", payload={"name": name})
            logging.info(f"✅ Macrodata '{name}' opgeslagen: {response}")
        except RetryError:
            logging.error(f"❌ Alle retries mislukt voor macrodata '{name}'")
            logging.error(traceback.format_exc())
        except Exception as e:
            logging.error(f"❌ Fout bij macrodata '{name}': {e}")
            logging.error(traceback.format_exc())

# ✅ Technische data ophalen (optioneel)
@celery.task(name="celery_worker.fetch_technical_data")
def fetch_technical_data():
    try:
        payload = {"symbol": "BTC"}
        data = safe_request(f"{API_BASE_URL}/save_technical_data", method="POST", payload=payload)
        logging.info(f"✅ Technische data succesvol opgeslagen: {data}")
    except RetryError:
        logging.error("❌ Alle retries mislukt voor fetch_technical_data!")
        logging.error(traceback.format_exc())

# ✅ Strategieën automatisch genereren
@celery.task(name="celery_worker.generate_strategieën_automatisch")
def generate_strategieën_automatisch():
    try:
        setups = safe_request(f"{API_BASE_URL}/setups", method="GET")
        if not setups:
            logging.warning("⚠️ Geen setups gevonden om strategieën voor te genereren")
            return

        for setup in setups:
            if setup.get("strategy_generated"):
                continue  # Skip als al strategie aanwezig is

            strategie = generate_strategy(setup)
            if not strategie:
                logging.warning(f"⚠️ Geen strategie gegenereerd voor setup {setup.get('name')}")
                continue

            payload = {
                "setup_name": setup["name"],
                "type": setup.get("strategy_type", "Algemeen"),
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
                result = safe_request(f"{API_BASE_URL}/strategieën", method="POST", payload=payload)
                logging.info(f"✅ Strategie opgeslagen voor setup {setup['name']}: {result}")
            except Exception as e:
                logging.error(f"❌ Fout bij opslaan strategie: {e}")

    except Exception as e:
        logging.error(f"❌ Fout in generate_strategieën_automatisch: {e}")
        logging.error(traceback.format_exc())

# ✅ Strategie genereren per specifieke setup (met overwrite-optie)
@celery.task(name="celery_worker.generate_strategie_voor_setup")
def generate_strategie_voor_setup(setup_id, overwrite=True):
    try:
        logging.info(f"🔍 Setup ophalen voor ID {setup_id}...")
        setup_res = requests.get(f"{API_BASE_URL}/setups/{setup_id}")
        if setup_res.status_code != 200:
            logging.error(f"❌ Setup niet gevonden: {setup_id}")
            return {"error": "Setup niet gevonden"}

        setup = setup_res.json()
        strategie = generate_strategy_from_setup(setup)
        if not strategie:
            logging.error("❌ Strategie generatie mislukt")
            return {"error": "AI-strategie kon niet gegenereerd worden"}

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
            logging.info("✏️ Bestaande strategie overschrijven...")
            put_res = requests.put(f"{API_BASE_URL}/strategieën/van_setup/{setup_id}", json=payload)
            if put_res.status_code != 200:
                logging.error(f"❌ Mislukt bij overschrijven strategie: {put_res.text}")
                return {"error": put_res.text}
        else:
            logging.info("➕ Nieuwe strategie aanmaken...")
            post_res = requests.post(f"{API_BASE_URL}/strategieën", json=payload)
            if post_res.status_code != 201:
                logging.error(f"❌ Mislukt bij toevoegen strategie: {post_res.text}")
                return {"error": post_res.text}

        logging.info("✅ Strategie succesvol gegenereerd")
        return {"success": True, "strategie": payload}

    except Exception as e:
        logging.error(f"❌ Fout in generate_strategie_voor_setup: {e}")
        logging.error(traceback.format_exc())
        return {"error": str(e)}

# ✅ Startbericht voor debug
if __name__ == "__main__":
    logging.info("🚀 Celery Task-module gestart!")


