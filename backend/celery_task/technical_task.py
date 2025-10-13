import os
import logging
import traceback
from datetime import datetime
from celery import shared_task
from tenacity import retry, stop_after_attempt, wait_exponential, RetryError
import requests

# ✅ Eigen utils
from backend.utils.db import get_db_connection
from backend.utils.technical_interpreter import process_all_technical

# ✅ Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ✅ Config
API_BASE_URL = os.getenv("API_BASE_URL", "http://127.0.0.1:5002/api")
BINANCE_BASE_URL = "https://api.binance.com"
TIMEOUT = 10
HEADERS = {"Content-Type": "application/json"}

# ✅ Safe API-call
@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=5, max=20), reraise=True)
def safe_request(url, method="GET", payload=None, headers=None):
    try:
        if method.upper() == "POST":
            response = requests.request(method, url, json=payload, headers=headers or HEADERS, timeout=TIMEOUT)
        else:
            response = requests.request(method, url, params=payload, headers=headers or HEADERS, timeout=TIMEOUT)

        response.raise_for_status()
        logger.info(f"✅ API-call succesvol: {url}")
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ RequestException bij {url}: {e}")
        raise
    except Exception as e:
        logger.error(f"⚠️ Onverwachte fout bij {url}: {e}")
        raise


# ✅ RSI berekenen
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


# ✅ API POST wrapper
def post_technical_data(payload: dict):
    try:
        url = f"{API_BASE_URL}/technical_data"
        logger.info(f"📡 POST technische data: {payload}")
        response = safe_request(url, method="POST", payload=payload, headers=HEADERS)
        logger.info(f"✅ Technische data opgeslagen of bijgewerkt: {response}")
    except RetryError:
        logger.error("❌ Alle retries mislukt voor technische data")
        logger.error(traceback.format_exc())
    except Exception as e:
        logger.error(f"❌ Fout bij opslaan technische data: {e}")
        logger.error(traceback.format_exc())


# ✅ Nieuwe functie: technische score direct in database opslaan
def store_technical_score_db(symbol, indicator, value, score, advies, uitleg, timestamp):
    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen databaseverbinding bij opslaan technische score.")
        return

    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO technical_indicators (symbol, indicator, value, score, advies, uitleg, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (symbol, indicator, value, score, advies, uitleg, timestamp))
        conn.commit()
        logger.info(f"🗃️ Technische score opgeslagen in DB voor {indicator} ({symbol})")
    except Exception as e:
        logger.error(f"❌ Fout bij opslaan technische score ({indicator}): {e}")
        logger.error(traceback.format_exc())
    finally:
        conn.close()


# ✅ Technische data ophalen, berekenen en direct opslaan
def fetch_and_post_daily(symbol="BTCUSDT", our_symbol="BTC", interval="1d", limit=300):
    try:
        logger.info(f"🚀 Start ophalen technische dagdata ({symbol})...")
        url = f"{BINANCE_BASE_URL}/api/v3/klines"
        params = {"symbol": symbol, "interval": interval, "limit": limit}
        data = safe_request(url, payload=params)
        logger.info("✅ Binance candles ontvangen")

        closes = [float(item[4]) for item in data]
        volumes = [float(item[5]) for item in data]

        if len(closes) < 200:
            logger.warning("⚠️ Niet genoeg candles voor 200MA (dagdata)")
            return

        # Berekeningen
        rsi = calculate_rsi(closes)
        volume = round(sum(volumes), 2)
        ma_200 = round(sum(closes[-200:]) / 200, 2)
        current_price = closes[-1]
        ma_200_ratio = round(current_price / ma_200, 3)

        logger.info(f"📊 Berekende waarden - RSI: {rsi}, Volume: {volume}, MA-ratio: {ma_200_ratio}")

        # Interpretatie + scoreberekening
        result = process_all_technical({
            "rsi": rsi,
            "volume": volume,
            "ma_200": ma_200_ratio
        })

        utc_now = datetime.utcnow().replace(microsecond=0)

        # ✅ Opslaan via API én direct in database
        for indicator, data in result.items():
            payload = {
                "symbol": our_symbol,
                "indicator": indicator,
                "value": data.get("value"),
                "score": data.get("score"),
                "advies": data.get("action"),
                "uitleg": data.get("explanation"),
                "timestamp": utc_now.isoformat(),
            }

            # 🔁 API-call (voor frontend sync)
            post_technical_data(payload)

            # 🗃️ Direct opslaan in database
            store_technical_score_db(
                symbol=our_symbol,
                indicator=indicator,
                value=data.get("value"),
                score=data.get("score"),
                advies=data.get("action"),
                uitleg=data.get("explanation"),
                timestamp=utc_now,
            )

    except Exception as e:
        logger.error("❌ Fout bij ophalen/verwerken technische data")
        logger.error(traceback.format_exc())


# ✅ Dagelijkse Celery taak
@shared_task(name="backend.celery_task.technical_task.fetch_technical_data_day")
def fetch_technical_data_day():
    fetch_and_post_daily()
