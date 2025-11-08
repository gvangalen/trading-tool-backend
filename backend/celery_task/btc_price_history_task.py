import logging
import traceback
import requests
from datetime import datetime, timedelta
from celery import shared_task
from backend.utils.db import get_db_connection
from tenacity import retry, stop_after_attempt, wait_exponential

# === Config
COINGECKO_ID = "bitcoin"
COINGECKO_URL = f"https://api.coingecko.com/api/v3/coins/{COINGECKO_ID}/market_chart/range"
TIMEOUT = 15
HEADERS = {"Content-Type": "application/json"}

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# =====================================================
# 🔁 Safe request met retry
# =====================================================
@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=5, max=20), reraise=True)
def safe_get(url, params=None):
    resp = requests.get(url, headers=HEADERS, params=params, timeout=TIMEOUT)
    resp.raise_for_status()
    return resp.json()


# =====================================================
# 🧠 Laatste datum in DB ophalen
# =====================================================
def get_last_date_in_db():
    conn = get_db_connection()
    if not conn:
        return None
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(date) FROM btc_price_history;")
            row = cur.fetchone()
            return row[0] if row and row[0] else None
    except Exception as e:
        logger.error(f"❌ Fout bij ophalen laatste datum: {e}")
        return None
    finally:
        conn.close()


# =====================================================
# 💾 Opslaan in DB
# =====================================================
def insert_price_rows(rows):
    conn = get_db_connection()
    if not conn:
        return
    try:
        with conn.cursor() as cur:
            for date_str, price in rows:
                cur.execute("""
                    INSERT INTO btc_price_history (date, price)
                    VALUES (%s, %s)
                    ON CONFLICT (date) DO UPDATE SET price = EXCLUDED.price
                """, (date_str, round(price, 2)))
        conn.commit()
        logger.info(f"✅ {len(rows)} records opgeslagen in btc_price_history.")
    except Exception as e:
        logger.error(f"❌ Fout bij DB-insert: {e}")
        logger.error(traceback.format_exc())
    finally:
        conn.close()


# =====================================================
# 📅 Hoofdtaak: BTC-prijs bijwerken tot vandaag
# =====================================================
@shared_task(name="backend.celery_task.btc_price_history_task.update_btc_history")
def update_btc_history():
    """
    Update de BTC-prijsgeschiedenis in de database.
    Haalt alleen ontbrekende dagen op sinds de laatste bekende datum.
    """
    logger.info("📈 Start update_btc_history...")

    last_date = get_last_date_in_db()
    today = datetime.utcnow().date()
    if not last_date:
        logger.warning("⚠️ Geen bestaande data gevonden, haal laatste 365 dagen op.")
        start_date = today - timedelta(days=365)
    else:
        start_date = last_date + timedelta(days=1)

    if start_date >= today:
        logger.info("✅ BTC-prijsdata al up-to-date tot vandaag.")
        return

    # CoinGecko verwacht UNIX timestamps (seconden)
    from_ts = int(datetime.combine(start_date, datetime.min.time()).timestamp())
    to_ts = int(datetime.combine(today + timedelta(days=1), datetime.min.time()).timestamp())

    logger.info(f"📡 Ophalen van {start_date} t/m {today} ({(today - start_date).days} dagen)")
    try:
        data = safe_get(COINGECKO_URL, params={"vs_currency": "usd", "from": from_ts, "to": to_ts})
        prices = data.get("prices", [])
        if not prices:
            logger.warning("⚠️ Geen prijsdata ontvangen van CoinGecko.")
            return

        rows = [
            (datetime.utcfromtimestamp(ts / 1000).date().isoformat(), float(price))
            for ts, price in prices
        ]
        insert_price_rows(rows)

        logger.info(f"✅ BTC-prijsdata bijgewerkt t/m {today} (totaal {len(rows)} dagen).")

    except Exception:
        logger.error("❌ Fout bij ophalen en opslaan van BTC-prijzen:")
        logger.error(traceback.format_exc())
