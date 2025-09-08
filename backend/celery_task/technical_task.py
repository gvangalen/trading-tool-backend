import os
import logging
import traceback
from urllib.parse import urljoin
from datetime import datetime
from celery import shared_task
from tenacity import retry, stop_after_attempt, wait_exponential, RetryError
import requests

# ✅ Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ✅ Configuratie
API_BASE_URL = os.getenv("API_BASE_URL", "http://127.0.0.1:5002/api")
BINANCE_BASE_URL = "https://api.binance.com"
TIMEOUT = 10
HEADERS = {"Content-Type": "application/json"}

# ✅ Robuuste API-call met retries
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

# ✅ Huidige prijs ophalen voor vergelijking met MA200
def get_current_price(symbol="BTCUSDT"):
    try:
        url = f"{BINANCE_BASE_URL}/api/v3/ticker/price"
        params = {"symbol": symbol}
        data = safe_request(url, payload=params)
        return float(data["price"])
    except Exception as e:
        logger.warning(f"⚠️ Fout bij ophalen laatste prijs {symbol}: {e}")
        return 0

# ✅ Technische data POSTen naar backend
@shared_task(name="backend.celery_task.technical_task.save_technical_data_task")
def save_technical_data_task(symbol, rsi, volume, ma_200_position, timeframe="1D"):
    payload = {
        "symbol": symbol,
        "rsi": rsi,
        "volume": volume,
        "ma_200": ma_200_position,
        "timeframe": timeframe,
    }

    try:
        url = f"{API_BASE_URL}/technical_data"
        logger.info(f"📡 POST technische data: {payload}")
        response = safe_request(url, method="POST", payload=payload, headers=HEADERS)
        logger.info(f"✅ Technische data succesvol opgeslagen: {response}")
    except RetryError:
        logger.error("❌ Alle retries mislukt voor save_technical_data_task!")
        logger.error(traceback.format_exc())
    except Exception as e:
        logger.error(f"❌ Fout bij opslaan technische data: {e}")
        logger.error(traceback.format_exc())

# ✅ Technische data ophalen van Binance en verwerken
@shared_task(name="backend.celery_task.technical_task.fetch_technical_data")
def fetch_technical_data():
    try:
        symbol = "BTCUSDT"
        timeframe = "1d"
        our_symbol = "BTC"
        limit = 300

        # ✅ Binance candles ophalen
        url = f"{BINANCE_BASE_URL}/api/v3/klines"
        params = {
            "symbol": symbol,
            "interval": timeframe,
            "limit": limit,
        }

        data = safe_request(url, payload=params)
        closes = [float(item[4]) for item in data]  # sluitprijzen
        volumes = [float(item[5]) for item in data]

        if len(closes) < 200:
            logger.warning("⚠️ Niet genoeg candles voor 200MA")
            return

        # ✅ Berekeningen
        rsi = calculate_rsi(closes)
        volume = round(volumes[-1], 2)
        ma_200 = round(sum(closes[-200:]) / 200, 2)
        current_price = closes[-1]
        ma_200_position = "above" if current_price > ma_200 else "below"

        logger.info(f"📊 RSI: {rsi}, MA200: {ma_200}, Positie: {ma_200_position}, Volume: {volume}")

        # ✅ Naar backend sturen
        save_technical_data_task.delay(our_symbol, rsi, volume, ma_200_position, "1D")

    except Exception as e:
        logger.error(f"❌ Fout bij ophalen technische data: {e}")
        logger.error(traceback.format_exc())
