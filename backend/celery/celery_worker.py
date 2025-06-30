from celery import Celery
import logging
import requests
import os
import traceback
import json
import psycopg2
from tenacity import retry, stop_after_attempt, wait_exponential, RetryError
from db import get_db_connection
from ai_setup_validator import validate_setups
from ai_trading_advice import generate_strategy_advice
from utils.pdf_generator import generate_pdf_from_advice  # ⬅️ Zorg dat dit bestand bestaat

# === 🛠️ Config & Logging ===
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

API_BASE_URL = os.getenv("API_BASE_URL", "http://market_dashboard-market_data_api:5002/api")
CONFIG_PATH = "macro_indicators_config.json"
TIMEOUT = 10

# === 🚀 Celery Setup ===
celery = Celery(
    "celery_worker",
    broker=os.getenv("CELERY_BROKER_URL", "redis://market_dashboard-redis:6379/0"),
    backend=os.getenv("CELERY_RESULT_BACKEND", "redis://market_dashboard-redis:6379/0"),
)
celery.conf.timezone = "UTC"
celery.conf.enable_utc = True

# === 🔁 Retry wrapper voor API-calls ===
@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=5, max=60), reraise=True)
def safe_request(url, method="POST", payload=None):
    headers = {"Content-Type": "application/json"}
    try:
        logger.debug(f"🌐 {method} request naar {url} payload={payload}")
        response = requests.request(method, url, json=payload, headers=headers, timeout=TIMEOUT)
        response.raise_for_status()
        logger.info(f"✅ API-response: {response.status_code}")
        return response.json()
    except requests.RequestException as e:
        logger.error(f"❌ API-fout bij {url}: {e}")
        logger.error(traceback.format_exc())
        return None

# === 📊 Marktdata ophalen ===
@celery.task(name="celery_worker.fetch_market_data")
def fetch_market_data():
    try:
        logger.info("📈 Ophalen marktdata gestart")
        result = safe_request(f"{API_BASE_URL}/save_market_data")
        if result:
            logger.info(f"✅ Marktdata opgeslagen: {result}")
    except RetryError:
        logger.error("❌ Marktdata: alle retries mislukt")
    except Exception as e:
        logger.error(f"❌ Marktdata error: {e}")
        logger.error(traceback.format_exc())

# === 📉 Macrodata ophalen via config ===
@celery.task(name="celery_worker.fetch_macro_data")
def fetch_macro_data():
    logger.info("🌍 Ophalen macrodata gestart")

    if not os.path.exists(CONFIG_PATH):
        logger.error("❌ macro_indicators_config.json niet gevonden")
        return

    try:
        with open(CONFIG_PATH) as f:
            config = json.load(f)
    except Exception as e:
        logger.error(f"❌ Laden van config mislukt: {e}")
        return

    for name in config.keys():
        try:
            result = safe_request(f"{API_BASE_URL}/macro_data/add", method="POST", payload={"name": name})
            if result:
                logger.info(f"✅ Macrodata '{name}' opgeslagen")
        except RetryError:
            logger.error(f"❌ Macrodata '{name}': retries mislukt")
        except Exception as e:
            logger.error(f"❌ Macrodata '{name}': {e}")
            logger.error(traceback.format_exc())

# === ✅ Setup-validatie ===
@celery.task(name="celery_worker.validate_setups_task")
def validate_setups_task():
    logger.info("🧠 Start setup-validatie")
    try:
        results = validate_setups()
        logger.info(f"🟢 {len(results)} setups gevalideerd")
        with open("validated_setups.json", "w") as f:
            json.dump(results, f, indent=2)
    except Exception as e:
        logger.error(f"❌ Fout in validate_setups_task: {e}")
        logger.error(traceback.format_exc())

# === 📈 Tradingadvies genereren ===
@celery.task(name="celery_worker.generate_trading_advice_task")
def generate_trading_advice_task():
    logger.info("📊 Start tradingadvies generatie")
    try:
        setups = validate_setups()
        macro_score = calculate_avg_score(setups, "macro")
        technical_score = calculate_avg_score(setups, "technical")

        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT price, change_24h FROM market_data WHERE symbol = 'BTC' ORDER BY timestamp DESC LIMIT 1")
            row = cur.fetchone()
        conn.close()

        if not row:
            logger.warning("⚠️ Geen marktdata beschikbaar voor BTC")
            return

        market_data = {"symbol": "BTC", "price": float(row[0]), "change_24h": float(row[1])}
        advice = generate_strategy_advice(setups, macro_score, technical_score, market_data)

        with open("trading_advice.json", "w") as f:
            json.dump(advice, f, indent=2)

        logger.info("✅ Tradingadvies succesvol gegenereerd")

    except Exception as e:
        logger.error(f"❌ Fout in generate_trading_advice_task: {e}")
        logger.error(traceback.format_exc())

# === 📄 PDF-rapport genereren ===
@celery.task(name="celery_worker.generate_daily_report_pdf")
def generate_daily_report_pdf():
    logger.info("📝 Start PDF-generatie van dagrapport")
    try:
        if not os.path.exists("trading_advice.json"):
            logger.warning("⚠️ Geen trading_advice.json gevonden")
            return

        with open("trading_advice.json") as f:
            advice = json.load(f)

        output_path = f"pdfs/report_{advice['date']}.pdf" if "date" in advice else "pdfs/report_latest.pdf"
        generate_pdf_from_advice(advice, output_path)

        logger.info(f"✅ PDF gegenereerd: {output_path}")
    except Exception as e:
        logger.error(f"❌ Fout in generate_daily_report_pdf: {e}")
        logger.error(traceback.format_exc())

# === 🧮 Scorehulp ===
def calculate_avg_score(setups, category):
    scores = []
    for setup in setups:
        part = setup.get("score_breakdown", {}).get(category, {})
        if part.get("total", 0) > 0:
            scores.append(part["score"])
    return round(sum(scores) / len(scores), 2) if scores else 0

# === 🧪 Handmatige test-run ===
if __name__ == "__main__":
    logger.info("🚀 Celery worker handmatig gestart (test)")
