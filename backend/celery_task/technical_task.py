import os
import logging
import traceback
from datetime import datetime
from celery import shared_task
from tenacity import retry, stop_after_attempt, wait_exponential
import requests

# ✅ Eigen utils
from backend.utils.db import get_db_connection
from backend.utils.scoring_utils import load_config, generate_scores

# ✅ Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ✅ Config
BINANCE_BASE_URL = "https://api.binance.com"
TIMEOUT = 10
HEADERS = {"Content-Type": "application/json"}

# ✅ Safe API-call
@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=5, max=20), reraise=True)
def safe_request(url, method="GET", payload=None, headers=None):
    try:
        response = requests.request(
            method, url,
            json=payload if method.upper() == "POST" else None,
            params=payload if method.upper() == "GET" else None,
            headers=headers or HEADERS,
            timeout=TIMEOUT
        )
        response.raise_for_status()
        logger.info(f"✅ API-call succesvol: {url}")
        return response.json()
    except Exception as e:
        logger.error(f"❌ API-fout bij {url}: {e}")
        raise

# ✅ RSI berekening
def calculate_rsi(closes, period=14):
    if len(closes) < period + 1:
        return None
    gains, losses = [], []
    for i in range(1, period + 1):
        delta = closes[-i] - closes[-i - 1]
        gains.append(max(delta, 0))
        losses.append(max(-delta, 0))
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return round(100 - (100 / (1 + rs)), 2)

# ✅ Opslaan score in DB
def store_technical_score_db(symbol, indicator, value, score, trend, interpretation, action, timestamp):
    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding")
        return

    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO technical_indicators (symbol, indicator, value, score, advies, uitleg, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (symbol, indicator, value, score, action, interpretation, timestamp))
        conn.commit()
        logger.info(f"✅ Score opgeslagen voor {indicator}")
    except Exception as e:
        logger.error(f"❌ Fout bij DB-opslag {indicator}: {e}")
        logger.error(traceback.format_exc())
    finally:
        conn.close()

# ✅ Hoofd-functie: data ophalen en scoren
def fetch_and_post_daily(symbol="BTCUSDT", our_symbol="BTC", interval="1d", limit=300):
    try:
        logger.info("🚀 Ophalen technische data...")
        url = f"{BINANCE_BASE_URL}/api/v3/klines"
        params = {"symbol": symbol, "interval": interval, "limit": limit}
        data = safe_request(url, payload=params)

        closes = [float(item[4]) for item in data]
        volumes = [float(item[5]) for item in data]

        if len(closes) < 200:
            logger.warning("⚠️ Onvoldoende candles voor 200MA")
            return

        rsi = calculate_rsi(closes)
        volume = round(sum(volumes), 2)
        ma_200 = round(sum(closes[-200:]) / 200, 2)
        current_price = closes[-1]
        ma_200_ratio = round(current_price / ma_200, 3)

        raw_data = {
            "rsi": rsi,
            "volume": volume,
            "ma_200": ma_200_ratio
        }

        config = load_config("config/technical_indicators_config.json")
        indicator_config = config.get("indicators", {})
        scores = generate_scores(raw_data, indicator_config)
        utc_now = datetime.utcnow().replace(microsecond=0)

        for indicator, details in scores["scores"].items():
            store_technical_score_db(
                symbol=our_symbol,
                indicator=indicator,
                value=details["value"],
                score=details["score"],
                trend=details.get("trend", ""),
                interpretation=details.get("interpretation", ""),
                action=details.get("action", ""),
                timestamp=utc_now,
            )

    except Exception as e:
        logger.error("❌ Verwerkingsfout:")
        logger.error(traceback.format_exc())

# ✅ Celery-taak
@shared_task(name="backend.celery_task.technical_task.fetch_technical_data_day")
def fetch_technical_data_day():
    fetch_and_post_daily()
