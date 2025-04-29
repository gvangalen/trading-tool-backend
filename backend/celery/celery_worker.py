from celery import Celery
from celery.schedules import crontab
import requests
import logging
import os
import traceback
import psycopg2
import json
from tenacity import retry, stop_after_attempt, wait_exponential, RetryError
from db import get_db_connection
from ai_setup_validator import validate_setups
from ai_trading_advice import generate_strategy_advice

# ✅ Logging instellen
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ✅ Celery configuratie
celery = Celery(
    "celery_worker",
    broker=os.getenv("CELERY_BROKER_URL", "redis://market_dashboard-redis:6379/0"),
    backend=os.getenv("CELERY_RESULT_BACKEND", "redis://market_dashboard-redis:6379/0"),
)
celery.conf.timezone = "UTC"
celery.conf.enable_utc = True

# ✅ Celery Beat planning
celery.conf.beat_schedule = {
    "fetch_market_data": {
        "task": "celery_worker.fetch_market_data",
        "schedule": crontab(minute="*/5"),
    },
    "fetch_macro_data": {
        "task": "celery_worker.fetch_macro_data",
        "schedule": crontab(minute="*/10"),
    },
    "validate_setups": {
        "task": "celery_worker.validate_setups_task",
        "schedule": crontab(minute=0, hour="*/6"),
    },
    "generate_trading_advice_task": {
        "task": "celery_worker.generate_trading_advice_task",
        "schedule": crontab(minute=5, hour="*/6"),
    },
}

# ✅ API Base URL (Docker internal)
API_BASE_URL = os.getenv("API_BASE_URL", "http://market_dashboard-market_data_api:5002/api")

# ✅ Retry wrapper
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=5, min=5, max=60), reraise=True)
def safe_request(url, method="POST", payload=None):
    headers = {"Content-Type": "application/json"}
    try:
        logger.debug(f"Sending {method} request to {url} with payload: {payload}")
        response = requests.request(method, url, json=payload, headers=headers, timeout=10)
        response.raise_for_status()
        logger.info(f"Response: {response.status_code} - {response.text}")
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ API-fout bij {url}: {e}")
        logger.error(traceback.format_exc())
        return None

# ✅ Marktdata ophalen
@celery.task(name="celery_worker.fetch_market_data")
def fetch_market_data():
    try:
        logger.info("📱 Start ophalen marktdata")
        data = safe_request(f"{API_BASE_URL}/save_market_data")
        if data:
            logger.info(f"✅ Marktdata opgeslagen: {data}")
        else:
            logger.warning("⚠️ Geen marktdata response")
    except RetryError:
        logger.error("❌ Alle retries mislukt voor fetch_market_data")
    except Exception as e:
        logger.error(f"❌ Fout bij fetch_market_data: {e}")
        logger.error(traceback.format_exc())

# ✅ Macrodata ophalen
@celery.task(name="celery_worker.fetch_macro_data")
def fetch_macro_data():
    logger.info("📱 Start ophalen macrodata")
    config_path = "macro_indicators_config.json"
    if not os.path.exists(config_path):
        logger.error("❌ macro_indicators_config.json niet gevonden")
        return
    try:
        with open(config_path) as f:
            config = json.load(f)
    except Exception as e:
        logger.error(f"❌ Fout bij laden van config: {e}")
        return

    for name in config.keys():
        try:
            response = safe_request(f"{API_BASE_URL}/macro_data/add", method="POST", payload={"name": name})
            if response:
                logger.info(f"✅ Macrodata '{name}' opgeslagen: {response}")
            else:
                logger.warning(f"⚠️ Geen response voor macrodata '{name}'")
        except RetryError:
            logger.error(f"❌ Alle retries mislukt voor macrodata '{name}'")
        except Exception as e:
            logger.error(f"❌ Fout bij macrodata '{name}': {e}")
            logger.error(traceback.format_exc())

# ✅ Setup-validatie
@celery.task(name="celery_worker.validate_setups_task")
def validate_setups_task():
    logger.info("🤖 Start setup-validatie via Celery")
    try:
        results = validate_setups()
        logger.info(f"✅ {len(results)} setups gevalideerd")
        for r in results:
            logger.info(f"🟢 {r['name']} - Active: {r['active']} | Score: {r['score']}")
        with open("validated_setups.json", "w") as f:
            json.dump(results, f, indent=2)
    except Exception as e:
        logger.error(f"❌ Fout bij validate_setups_task: {e}")
        logger.error(traceback.format_exc())

# ✅ Tradingadvies genereren
@celery.task(name="celery_worker.generate_trading_advice_task")
def generate_trading_advice_task():
    logger.info("📈 Start genereren tradingadvies via Celery")
    try:
        setup_results = validate_setups()
        macro_score = calculate_avg_score(setup_results, "macro")
        technical_score = calculate_avg_score(setup_results, "technical")

        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT price, change_24h FROM market_data WHERE symbol = 'BTC' ORDER BY timestamp DESC LIMIT 1")
            row = cur.fetchone()
            market_data = {"symbol": "BTC", "price": float(row[0]), "change_24h": float(row[1])}
        conn.close()

        advice = generate_strategy_advice(setup_results, macro_score, technical_score, market_data)

        with open("trading_advice.json", "w") as f:
            json.dump(advice, f, indent=2)

        logger.info(f"✅ Tradingadvies gegenereerd: {advice}")

    except Exception as e:
        logger.error(f"❌ Fout bij tradingadvies: {e}")
        logger.error(traceback.format_exc())

# ✅ Helpers voor scoreberekening
def calculate_avg_score(setup_results, category):
    scores = []
    for setup in setup_results:
        breakdown = setup.get("score_breakdown", {})
        if category in breakdown and breakdown[category]["total"] > 0:
            scores.append(breakdown[category]["score"])
    if not scores:
        return 0
    return round(sum(scores) / len(scores), 2)

# ✅ Webhook verwerking
@celery.task(name="celery_worker.process_tradingview_webhook")
def process_tradingview_webhook(symbol, rsi, volume, ma_200):
    try:
        logger.info(f"📱 Webhook voor {symbol}: RSI={rsi}, Volume={volume}, MA200={ma_200}")
        success = save_technical_data(symbol, rsi, volume, ma_200)
        if success:
            logger.info(f"✅ Technische data opgeslagen voor {symbol}")
        else:
            logger.error(f"❌ Opslaan technische data mislukt voor {symbol}")
    except Exception as e:
        logger.error(f"❌ Fout bij webhook-verwerking: {e}")
        logger.error(traceback.format_exc())

# ✅ Technische data opslaan via taak
@celery.task(name="celery_worker.save_technical_data_task")
def save_technical_data_task(symbol, rsi, volume, ma_200):
    logger.info(f"📱 Celery taak voor {symbol}: RSI={rsi}, Volume={volume}, MA200={ma_200}")
    success = save_technical_data(symbol, rsi, volume, ma_200)
    if success:
        logger.info(f"✅ Data opgeslagen via taak voor {symbol}")
    else:
        logger.error(f"❌ Celery taak mislukt voor {symbol}")

# ✅ DB-opslag
def save_technical_data(symbol, rsi, volume, ma_200):
    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen databaseverbinding")
        return False
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO technical_data (symbol, rsi, volume, ma_200, is_updated, timestamp)
                VALUES (%s, %s, %s, %s, TRUE, NOW())
            """, (symbol, rsi, volume, ma_200))
            conn.commit()
        return True
    except psycopg2.Error as e:
        logger.error(f"❌ Databasefout: {e}")
        return False
    finally:
        conn.close()

# ✅ Handmatige test
if __name__ == "__main__":
    logger.info("🚀 Celery Worker handmatig gestart (debug/test)")
