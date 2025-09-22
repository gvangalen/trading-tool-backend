import os
import logging
import traceback
from urllib.parse import urljoin
from datetime import datetime
from celery import shared_task
from tenacity import retry, stop_after_attempt, wait_exponential, RetryError
import requests

# ✅ Eigen utils
from backend.utils.technical_score import process_all_technical

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

# ✅ POST wrapper (zonder timeframe!)
def post_technical_data(payload: dict):
    try:
        url = f"{API_BASE_URL}/technical_data"
        logger.info(f"📡 POST technische data: {payload}")
        response = safe_request(url, method="POST", payload=payload, headers=HEADERS)
        logger.info(f"✅ Technische data opgeslagen: {response}")
    except RetryError:
        logger.error("❌ Alle retries mislukt voor technische data")
        logger.error(traceback.format_exc())
    except Exception as e:
        logger.error(f"❌ Fout bij opslaan technische data: {e}")
        logger.error(traceback.format_exc())

# ✅ Technische data ophalen en posten
def fetch_and_post(symbol="BTCUSDT", our_symbol="BTC", interval="1d", limit=300):
    try:
        url = f"{BINANCE_BASE_URL}/api/v3/klines"
        params = {"symbol": symbol, "interval": interval, "limit": limit}
        data = safe_request(url, payload=params)
        closes = [float(item[4]) for item in data]
        volumes = [float(item[5]) for item in data]

        if len(closes) < 200:
            logger.warning("⚠️ Niet genoeg candles voor 200MA")
            return

        rsi = calculate_rsi(closes)
        volume = round(volumes[-1], 2)
        ma_200 = round(sum(closes[-200:]) / 200, 2)
        current_price = closes[-1]
        ma_200_ratio = round(current_price / ma_200, 3)

        # ✅ Score-engine gebruiken
        score_result = process_all_technical({
            "rsi": rsi,
            "volume": volume,
            "ma_200": ma_200_ratio
        })

        # ✅ Payload bouwen
        payload = {
            "symbol": our_symbol,
            "rsi": rsi,
            "volume": volume,
            "ma_200": ma_200_ratio,
            "rsi_score": score_result.get("rsi", {}).get("score"),
            "volume_score": score_result.get("volume", {}).get("score"),
            "ma_200_score": score_result.get("ma_200", {}).get("score"),
            "timestamp": datetime.utcnow().isoformat()  # ✅ timestamp toevoegen
        }

        logger.info(f"📊 Technische Scores: {payload}")
        post_technical_data(payload)

    except Exception as e:
        logger.error("❌ Fout bij ophalen/verwerken technische data")
        logger.error(traceback.format_exc())

# ✅ Dagelijkse task
@shared_task(name="backend.celery_task.technical_task.fetch_technical_data")
def fetch_technical_data():
    fetch_and_post()
