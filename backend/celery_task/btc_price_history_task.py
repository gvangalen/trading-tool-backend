import os
import logging
import traceback
import requests
from celery import shared_task
from backend.utils.db import get_db_connection
from datetime import datetime

logger = logging.getLogger(__name__)

COINGECKO_URL = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart"
PARAMS = {"vs_currency": "usd", "days": "max"}

# 📈 Volledige historiek ophalen
@shared_task(name="backend.celery_task.btc_price_history_task.fetch_btc_history")
def fetch_btc_history():
    logger.info("📊 Start ophalen historische BTC-prijzen...")
    logger.info(f"📡 Request: {COINGECKO_URL} met {PARAMS}")
    try:
        response = requests.get(COINGECKO_URL, params=PARAMS, timeout=10)
        if response.status_code != 200:
            logger.error(f"❌ Foutcode {response.status_code} van CoinGecko: {response.text}")
            return

        data = response.json()
        prices = data.get("prices", [])
        logger.info(f"📈 Ontvangen {len(prices)} datapunten.")

        if not prices:
            logger.warning("⚠️ Geen prijsdata ontvangen van CoinGecko.")
            return

        conn = get_db_connection()
        if not conn:
            logger.error("❌ Geen databaseverbinding.")
            return

        with conn.cursor() as cur:
            inserted = 0
            for timestamp, price in prices:
                date = datetime.utcfromtimestamp(timestamp / 1000).date()
                try:
                    cur.execute(
                        "INSERT INTO btc_price_history (date, price) VALUES (%s, %s) ON CONFLICT (date) DO NOTHING",
                        (date, round(price, 2)),
                    )
                    inserted += 1
                except Exception as e:
                    logger.warning(f"⚠️ Skipped {date}: {e}")

            conn.commit()
            logger.info(f"✅ {inserted} BTC-prijsrecords toegevoegd aan btc_price_history.")
    except Exception as e:
        logger.error("❌ Fout bij ophalen en opslaan van BTC-prijzen:")
        logger.error(traceback.format_exc())

# 📆 Dagelijkse prijs ophalen (laatste datapunt)
@shared_task(name="backend.celery_task.btc_price_history_task.fetch_btc_history_daily")
def fetch_btc_history_daily():
    logger.info("📆 Start ophalen BTC-prijs van vandaag...")
    try:
        response = requests.get(COINGECKO_URL, params={"vs_currency": "usd", "days": "1"}, timeout=10)
        if response.status_code != 200:
            logger.error(f"❌ Foutcode {response.status_code} van CoinGecko: {response.text}")
            return

        data = response.json()
        prices = data.get("prices", [])
        if not prices:
            logger.warning("⚠️ Geen prijsdata voor vandaag ontvangen.")
            return

        latest_entry = prices[-1]
        timestamp, price = latest_entry
        date = datetime.utcfromtimestamp(timestamp / 1000).date()

        conn = get_db_connection()
        if not conn:
            logger.error("❌ Geen databaseverbinding.")
            return

        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO btc_price_history (date, price) VALUES (%s, %s) ON CONFLICT (date) DO NOTHING",
                (date, round(price, 2)),
            )
            conn.commit()
            logger.info(f"✅ BTC-prijs van {date} opgeslagen: {round(price, 2)} USD")
    except Exception:
        logger.error("❌ Fout bij ophalen dagelijkse BTC-prijs:")
        logger.error(traceback.format_exc())
