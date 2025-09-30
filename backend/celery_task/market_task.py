import os
import json
import logging
import traceback
import requests
from datetime import datetime, timedelta
from pathlib import Path
from tenacity import retry, stop_after_attempt, wait_exponential, RetryError
from celery import shared_task
from dotenv import load_dotenv

# ✅ .env laden
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(dotenv_path=BASE_DIR / ".env")

# ✅ Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ✅ Config
API_BASE_URL = os.getenv("API_BASE_URL", "http://127.0.0.1:5002/api")
ASSETS_JSON = os.getenv("ASSETS_JSON", '{"BTC": "bitcoin"}')

try:
    ASSETS = json.loads(ASSETS_JSON)
except json.JSONDecodeError:
    logger.error("❌ Ongeldige JSON in ASSETS_JSON.")
    ASSETS = {"BTC": "bitcoin"}

TIMEOUT = 10
HEADERS = {"Content-Type": "application/json"}
CACHE_FILE = "/tmp/last_market_data_fetch.txt"
MIN_INTERVAL_MINUTES = 15

def recently_fetched():
    try:
        mtime = datetime.fromtimestamp(Path(CACHE_FILE).stat().st_mtime)
        if datetime.now() - mtime < timedelta(minutes=MIN_INTERVAL_MINUTES):
            logger.info(f"♻️ Marktdata is < {MIN_INTERVAL_MINUTES} minuten oud. Skip.")
            return True
    except FileNotFoundError:
        return False
    return False

@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=5, max=30), reraise=True)
def safe_request(url, method="POST", payload=None):
    try:
        response = requests.request(method, url, json=payload, headers=HEADERS, timeout=TIMEOUT)
        if response.status_code == 429:
            logger.error(f"🚫 CoinGecko rate limit bereikt (429).")
            raise Exception("Rate limit")
        if response.status_code != 200:
            logger.error(f"❌ API-foutstatus {response.status_code}: {response.text}")
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"❌ API-call fout: {e}")
        raise

# ✅ 1. Live marktdata ophalen (voor dashboard - elke 15 min)
@shared_task(name="backend.celery_task.market_task.fetch_market_data")
def fetch_market_data():
    logger.info("📈 Start live marktdata ophalen...")

    if recently_fetched():
        return

    try:
        coingecko_id = ASSETS.get("BTC", "bitcoin")
        url = f"https://api.coingecko.com/api/v3/coins/{coingecko_id}"
        response = requests.get(url, params={"localization": "false"}, timeout=TIMEOUT)

        if response.status_code == 429:
            logger.warning("🚫 CoinGecko API rate limit (429). Sla opvraag over.")
            return
        if response.status_code != 200:
            logger.error(f"❌ CoinGecko foutstatus {response.status_code}: {response.text}")
            return

        data = response.json()
        market_data = data.get("market_data", {})

        price = market_data.get("current_price", {}).get("usd")
        volume = market_data.get("total_volume", {}).get("usd")
        change = market_data.get("price_change_percentage_24h", 0)

        if price is None or volume is None:
            logger.warning("⚠️ Onvolledige CoinGecko data.")
            return

        payload = {
            "symbol": "BTC",
            "price": price,
            "volume": volume,
            "change": change,
            "timestamp": datetime.utcnow().isoformat(),
            "source": "coingecko"
        }

        logger.info(f"📡 Versturen marktdata naar backend: {payload}")
        save_url = f"{API_BASE_URL}/market_data"
        safe_request(save_url, method="POST", payload=payload)

        Path(CACHE_FILE).touch()
        logger.info("✅ Marktdata succesvol opgeslagen en cache-tijd bijgewerkt.")

    except RetryError:
        logger.error("❌ Retries mislukt voor fetch_market_data.")
        logger.error(traceback.format_exc())

# ✅ 2. Dagelijkse snapshot opslaan in market_data (rond 00:00)
@shared_task(name="backend.celery_task.market_task.save_market_data_daily")
def save_market_data_daily():
    logger.info("📅 Dagelijkse BTC-close data ophalen...")

    try:
        coingecko_id = ASSETS.get("BTC", "bitcoin")
        url = f"https://api.coingecko.com/api/v3/coins/{coingecko_id}"
        response = requests.get(url, params={"localization": "false"}, timeout=TIMEOUT)

        if response.status_code != 200:
            logger.error(f"❌ CoinGecko foutstatus {response.status_code}: {response.text}")
            return

        data = response.json()
        market_data = data.get("market_data", {})
        price = market_data.get("current_price", {}).get("usd")
        volume = market_data.get("total_volume", {}).get("usd")
        change = market_data.get("price_change_percentage_24h", 0)

        if price is None or volume is None:
            logger.warning("⚠️ Onvolledige CoinGecko data.")
            return

        payload = {
            "symbol": "BTC",
            "price": price,
            "volume": volume,
            "change": change,
            "timestamp": datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0).isoformat(),
            "source": "daily_close"
        }

        save_url = f"{API_BASE_URL}/market_data"
        logger.info(f"🕛 Versturen dagelijkse snapshot: {payload}")
        safe_request(save_url, method="POST", payload=payload)
        logger.info("✅ Dagelijkse market_data opgeslagen.")
    except Exception as e:
        logger.error("❌ Fout bij dagelijkse data-opslag.")
        logger.error(traceback.format_exc())

# ✅ 3. 7-daagse BTC-data genereren uit eigen database
@shared_task(name="backend.celery_task.market_task.save_market_data_7d")
def save_market_data_7d():
    logger.info("📊 Start genereren van 7-daagse BTC-data uit opgeslagen market_data...")

    try:
        save_url = f"{API_BASE_URL}/market_data/btc/7d/fill"
        response = safe_request(save_url, method="POST")
        logger.info(f"✅ Marktdata 7 dagen opgebouwd uit eigen DB: {response}")
    except RetryError:
        logger.error("❌ Retries mislukt voor save_market_data_7d.")
        logger.error(traceback.format_exc())
    except Exception as e:
        logger.error("❌ Algemene fout in save_market_data_7d.")
        logger.error(traceback.format_exc())

# ✅ 4. Forward returns opslaan
@shared_task(name="backend.celery_task.market_task.save_forward_returns")
def save_forward_returns():
    logger.info("📈 Start berekenen forward returns...")
    try:
        url = f"{API_BASE_URL}/market_data/forward/save"
        response = safe_request(url)
        logger.info(f"✅ Forward returns opgeslagen: {response}")
    except RetryError:
        logger.error("❌ Retries mislukt voor save_forward_returns.")
        logger.error(traceback.format_exc())

# ✅ 5. Historische BTC-prijs ophalen via CoinGecko (max 365 dagen i.v.m. gratis API)
@shared_task(name="backend.celery_task.market_task.fetch_btc_price_history")
def fetch_btc_price_history():
    logger.info("⏳ Start ophalen BTC-prijsgeschiedenis (365 dagen)...")
    try:
        coingecko_id = ASSETS.get("BTC", "bitcoin")
        url = f"https://api.coingecko.com/api/v3/coins/{coingecko_id}/market_chart"
        params = {"vs_currency": "usd", "days": "365", "interval": "daily"}  # <= aangepast

        response = requests.get(url, params=params, timeout=TIMEOUT)
        if response.status_code != 200:
            logger.error(f"❌ CoinGecko foutstatus {response.status_code}: {response.text}")
            return

        data = response.json()
        prices = data.get("prices", [])
        if not prices:
            logger.warning("⚠️ Geen data van CoinGecko.")
            return

        save_url = f"{API_BASE_URL}/market_data/history/save"
        payload = [
            {
                "date": datetime.utcfromtimestamp(ts / 1000).date().isoformat(),
                "price": round(price, 2),
            }
            for ts, price in prices
        ]
        logger.info(f"📝 Versturen {len(payload)} entries naar history endpoint...")
        response = safe_request(save_url, method="POST", payload=payload)
        logger.info(f"✅ Geschiedenis opgeslagen: {response}")
    except Exception as e:
        logger.error("❌ Fout tijdens ophalen historische BTC-data.")
        logger.error(traceback.format_exc())

@shared_task(name="backend.celery_task.market_task.calculate_and_save_forward_returns")
def calculate_and_save_forward_returns():
    logger.info("📈 Start berekening van forward returns vanuit btc_price_history...")

    try:
        from backend.utils.db import get_db_connection  # import hier binnen de task
        from datetime import date

        conn = get_db_connection()
        cur = conn.cursor()

        # 📅 Data ophalen en sorteren op datum
        cur.execute("SELECT date, price FROM btc_price_history ORDER BY date ASC")
        rows = cur.fetchall()
        data = [{"date": row[0], "price": float(row[1])} for row in rows]

        payload = []
        # Je kunt hier je periodes naar wens aanpassen
        periods = [7, 30, 90]  # 1w, 1m, 1q

        for i, row in enumerate(data):
            for days in periods:
                if i + days >= len(data):
                    continue
                start = row
                end = data[i + days]

                change = ((end["price"] - start["price"]) / start["price"]) * 100
                avg_daily = change / days

                payload.append({
                    "symbol": "BTC",
                    "period": f"{days}d",
                    "start_date": start["date"],  # wordt zo meteen geconverteerd
                    "end_date": end["date"],      # idem
                    "change": round(change, 2),
                    "avg_daily": round(avg_daily, 3)
                })

        if not payload:
            logger.warning("⚠️ Geen forward return data om op te slaan.")
            return

        # 🔧 Fix: converteer alle date-velden naar ISO strings voor JSON
        for item in payload:
            if isinstance(item.get("start_date"), date):
                item["start_date"] = item["start_date"].isoformat()
            if isinstance(item.get("end_date"), date):
                item["end_date"] = item["end_date"].isoformat()

        save_url = f"{API_BASE_URL}/market_data/forward/save"
        logger.info(f"🧮 Versturen {len(payload)} forward returns...")
        safe_request(save_url, method="POST", payload=payload)
        logger.info(f"✅ Forward returns berekend en opgeslagen ({len(payload)} rijen).")

    except Exception as e:
        logger.error("❌ Fout tijdens berekening van forward returns.")
        logger.error(traceback.format_exc())
