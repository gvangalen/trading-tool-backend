import logging
import traceback
import requests
from datetime import datetime
from pathlib import Path
from tenacity import retry, stop_after_attempt, wait_exponential
from celery import shared_task

# Eigen imports
from backend.utils.db import get_db_connection
from backend.utils.scoring_utils import generate_scores_db
from backend.celery_task.btc_price_history_task import update_btc_history

# === Config ===
TIMEOUT = 10
HEADERS = {"Content-Type": "application/json"}
CACHE_FILE = "/tmp/last_market_data_fetch.txt"
ASSETS = {"BTC": "bitcoin"}

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# =====================================================
# 🔁 Safe HTTP-get met retry
# =====================================================
@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=5, max=20), reraise=True)
def safe_get(url, params=None):
    resp = requests.get(url, headers=HEADERS, params=params, timeout=TIMEOUT)
    resp.raise_for_status()
    return resp.json()


# =====================================================
# 🌐 CoinGecko fetch helper
# =====================================================
def fetch_coingecko_market(symbol_id="bitcoin"):
    """Haalt marktdata op van CoinGecko."""
    try:
        url = f"https://api.coingecko.com/api/v3/coins/{symbol_id}"
        resp = requests.get(url, params={"localization": "false"}, timeout=TIMEOUT)
        resp.raise_for_status()

        data = resp.json()
        md = data.get("market_data", {}) or {}

        price = float(md.get("current_price", {}).get("usd", 0))
        volume = float(md.get("total_volume", {}).get("usd", 0))
        change_24h = float(md.get("price_change_percentage_24h", 0) or 0)

        logger.info(f"📊 Fetched market data: price={price}, vol={volume}, 24h={change_24h}")
        return {"price": price, "volume": volume, "change_24h": change_24h}

    except Exception:
        logger.error("❌ Fout bij ophalen CoinGecko market:")
        logger.error(traceback.format_exc())
        return None


# =====================================================
# 💾 Opslaan in market_data (correcte kolommen)
# =====================================================
def store_market_data_db(symbol, price, volume, change_24h):
    """Slaat de live marktdata op in de juiste tabelkolommen."""
    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding bij market-opslag.")
        return

    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO market_data (symbol, price, volume, change_24h, timestamp)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                symbol,
                price,
                volume,
                change_24h,
                datetime.utcnow().replace(microsecond=0)
            ))
        conn.commit()
        logger.info(f"✅ Marktdata opgeslagen: {symbol} prijs={price}, volume={volume}, 24h={change_24h}")
    except Exception:
        logger.error("❌ Fout bij opslaan market_data:")
        logger.error(traceback.format_exc())
    finally:
        conn.close()


# =====================================================
# 📊 Marktdata ophalen en opslaan
# =====================================================
def process_market_now():
    """Haalt huidige BTC-marketdata op en slaat op."""
    cg_id = ASSETS.get("BTC", "bitcoin")
    live = fetch_coingecko_market(cg_id)
    if not live:
        logger.warning("⚠️ Geen live marketdata ontvangen.")
        return

    store_market_data_db("BTC", live["price"], live["volume"], live["change_24h"])


# =====================================================
# 🚀 Celery-taken
# =====================================================

# 1️⃣ Live marketdata (elke ±15 min)
@shared_task(name="backend.celery_task.market_task.fetch_market_data")
def fetch_market_data_task():
    """Haalt live BTC-marktdata op en slaat deze op."""
    logger.info("📈 Start live marktdata taak...")
    try:
        process_market_now()
        Path(CACHE_FILE).touch()
        logger.info("✅ Live marktdata verwerkt.")
    except Exception:
        logger.error("❌ Fout in fetch_market_data_task()")
        logger.error(traceback.format_exc())


# 2️⃣ Dagelijkse snapshot (voor rapporten)
@shared_task(name="backend.celery_task.market_task.save_market_data_daily")
def save_market_data_daily():
    """Dagelijkse snapshot van de BTC-marktdata (zelfde als fetch_market_data_task)."""
    logger.info("🕛 Dagelijkse market snapshot...")
    try:
        process_market_now()
        logger.info("✅ Dagelijkse snapshot opgeslagen.")
    except Exception:
        logger.error("❌ Fout in save_market_data_daily()")
        logger.error(traceback.format_exc())


# 3️⃣ BTC-prijs bijwerken (range-based update)
@shared_task(name="backend.celery_task.market_task.sync_price_history_and_returns")
def sync_price_history_and_returns():
    """Update de BTC-prijsdata en berekent daarna forward returns."""
    logger.info("🔄 Start synchronisatie BTC-historie + forward returns...")
    try:
        update_btc_history()
        calculate_and_save_forward_returns.apply_async(countdown=5)
        logger.info("✅ Forward returns berekening ingepland.")
    except Exception:
        logger.error("❌ Fout in sync_price_history_and_returns()")
        logger.error(traceback.format_exc())


# 4️⃣ Forward returns berekenen en opslaan
@shared_task(name="backend.celery_task.market_task.calculate_and_save_forward_returns")
def calculate_and_save_forward_returns():
    """Bereken en sla forward returns op uit btc_price_history."""
    logger.info("📈 Forward returns berekenen...")
    try:
        conn = get_db_connection()
        if not conn:
            logger.error("❌ Geen DB-verbinding.")
            return

        with conn.cursor() as cur:
            cur.execute("SELECT date, price FROM btc_price_history ORDER BY date ASC")
            rows = cur.fetchall()
        if not rows:
            logger.warning("⚠️ Geen data in btc_price_history.")
            return

        data = [{"date": r[0], "price": float(r[1])} for r in rows]
        periods = [7, 30, 90]
        results = []

        for i, start in enumerate(data):
            for d in periods:
                j = i + d
                if j >= len(data):
                    continue
                end = data[j]
                change = ((end["price"] - start["price"]) / start["price"]) * 100
                avg_daily = change / d
                results.append((start["date"], end["date"], d, change, avg_daily))

        if not results:
            logger.warning("⚠️ Geen returns berekend.")
            return

        with conn:
            with conn.cursor() as cur:
                for s_date, e_date, days, change, avg_daily in results:
                    cur.execute("""
                        INSERT INTO market_forward_returns
                            (symbol, period, start_date, end_date, change, avg_daily)
                        VALUES ('BTC', %s, %s, %s, %s, %s)
                        ON CONFLICT (symbol, period, start_date)
                        DO UPDATE SET
                            end_date = EXCLUDED.end_date,
                            change = EXCLUDED.change,
                            avg_daily = EXCLUDED.avg_daily
                    """, (f"{days}d", s_date, e_date, round(change, 2), round(avg_daily, 3)))
        conn.commit()
        logger.info(f"✅ Forward returns opgeslagen ({len(results)} rijen).")

    except Exception:
        logger.error("❌ Fout in calculate_and_save_forward_returns()")
        logger.error(traceback.format_exc())
    finally:
        if 'conn' in locals() and conn:
            conn.close()


# 5️⃣ 7-daags aggregaat vullen uit eigen DB
@shared_task(name="backend.celery_task.market_task.save_market_data_7d")
def save_market_data_7d():
    """Bouw 7-daagse aggregaten op uit market_data."""
    logger.info("📊 Opbouwen 7-daagse marktdata (DB-native)...")
    try:
        conn = get_db_connection()
        if not conn:
            logger.error("❌ Geen DB-verbinding.")
            return

        with conn.cursor() as cur:
            cur.execute("""
                WITH per_day AS (
                    SELECT
                        DATE(timestamp) AS dag,
                        AVG(price) AS avg_price,
                        AVG(volume) AS avg_volume,
                        AVG(change_24h) AS avg_change
                    FROM market_data
                    WHERE symbol = 'BTC'
                    GROUP BY DATE(timestamp)
                )
                SELECT dag, avg_price, avg_volume, avg_change
                FROM per_day
                ORDER BY dag DESC
                LIMIT 7;
            """)
            rows = cur.fetchall()

        logger.info(f"ℹ️ Laatste 7 dagen samengevoegd ({len(rows)} rijen).")
    except Exception:
        logger.error("❌ Fout in save_market_data_7d()")
        logger.error(traceback.format_exc())
