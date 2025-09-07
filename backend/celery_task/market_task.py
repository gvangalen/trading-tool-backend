import os
import json
import logging
import traceback
import requests
from urllib.parse import urljoin
from datetime import datetime
from pathlib import Path
from tenacity import retry, stop_after_attempt, wait_exponential, RetryError
from celery import shared_task
from dotenv import load_dotenv

# ✅ .env laden (dynamisch pad)
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(dotenv_path=BASE_DIR / ".env")

# ✅ Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ✅ Config
API_BASE_URL = os.getenv("API_BASE_URL", "http://127.0.0.1:5002/api")
COINGECKO_URL = os.getenv("COINGECKO_URL")
VOLUME_URL = os.getenv("VOLUME_URL")
ASSETS_JSON = os.getenv("ASSETS_JSON", '{"BTC": "bitcoin"}')

try:
    ASSETS = json.loads(ASSETS_JSON)  # ✅ Veilig omzetten naar dict
except json.JSONDecodeError:
    logger.error("❌ Ongeldige JSON in ASSETS_JSON.")
    ASSETS = {"BTC": "bitcoin"}

TIMEOUT = 10
HEADERS = {"Content-Type": "application/json"}

# ✅ Robuuste request
@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=5, max=30), reraise=True)
def safe_request(url, method="POST", payload=None):
    try:
        response = requests.request(method, url, json=payload, headers=HEADERS, timeout=TIMEOUT)
        if response.status_code != 200:
            logger.error(f"❌ Foutstatus {response.status_code} van {url}: {response.text}")
        response.raise_for_status()
        logger.info(f"✅ API-call succesvol: {url}")
        try:
            return response.json()
        except Exception:
            logger.warning("⚠️ Non-JSON response")
            return {}
    except Exception as e:
        logger.error(f"❌ API-call fout: {e}")
        raise


# ✅ 1. Live BTC marktdata ophalen
@shared_task(name="backend.celery_task.market_task.fetch_market_data")
def fetch_market_data():
    logger.info("📈 Start live marktdata ophalen...")
    try:
        url = urljoin(API_BASE_URL, "/market_data/save")
        response = safe_request(url)
        logger.info(f"✅ Marktdata opgeslagen: {response}")
    except RetryError:
        logger.error("❌ Alle retries mislukt voor fetch_market_data.")
        logger.error(traceback.format_exc())


# ✅ 2. 7-daagse BTC-data vullen
@shared_task(name="backend.celery_task.market_task.save_market_data_7d")
def save_market_data_7d():
    logger.info("📊 Start vullen 7-daagse BTC-data...")
    try:
        url = urljoin(API_BASE_URL, "/market_data/btc/7d/fill")
        response = safe_request(url)
        logger.info(f"✅ 7d-data gevuld: {response}")
    except RetryError:
        logger.error("❌ Retries mislukt voor save_market_data_7d.")
        logger.error(traceback.format_exc())


# ✅ 3. Forward returns opslaan
@shared_task(name="backend.celery_task.market_task.save_forward_returns")
def save_forward_returns():
    logger.info("📈 Start berekenen forward returns...")
    try:
        url = urljoin(API_BASE_URL, "/market_data/forward/save")
        response = safe_request(url)
        logger.info(f"✅ Forward returns opgeslagen: {response}")
    except RetryError:
        logger.error("❌ Retries mislukt voor save_forward_returns.")
        logger.error(traceback.format_exc())


# ✅ 4. Historische BTC-prijs ophalen via CoinGecko
@shared_task(name="backend.celery_task.market_task.fetch_btc_price_history")
def fetch_btc_price_history():
    logger.info("⏳ Start ophalen BTC-prijsgeschiedenis...")
    try:
        coingecko_id = ASSETS.get("BTC", "bitcoin")
        url = f"https://api.coingecko.com/api/v3/coins/{coingecko_id}/market_chart"
        params = {"vs_currency": "usd", "days": "max"}

        response = requests.get(url, params=params, timeout=TIMEOUT)
        if response.status_code != 200:
            logger.error(f"❌ CoinGecko foutstatus {response.status_code}: {response.text}")
            return

        data = response.json()
        prices = data.get("prices", [])
        if not prices:
            logger.warning("⚠️ Geen data van CoinGecko.")
            return

        # ⏬ API-call naar eigen backend voor opslaan
        save_url = urljoin(API_BASE_URL, "/market_data/history/save")
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
