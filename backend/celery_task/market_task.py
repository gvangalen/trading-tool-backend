import logging
import traceback
import requests
from datetime import datetime, timedelta
from pathlib import Path
from tenacity import retry, stop_after_attempt, wait_exponential
from celery import shared_task

# ✅ Eigen utils
from backend.utils.db import get_db_connection
from backend.utils.scoring_utils import generate_scores_db
from backend.celery_task.btc_price_history_task import update_btc_history  # nieuwe versie gebruiken

# === Config
TIMEOUT = 10
HEADERS = {"Content-Type": "application/json"}
CACHE_FILE = "/tmp/last_market_data_fetch.txt"
MIN_INTERVAL_MINUTES = 15
ASSETS = {"BTC": "bitcoin"}

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
# 🌐 CoinGecko fetch helper
# =====================================================
def fetch_coingecko_market(symbol_id="bitcoin"):
    try:
        url = f"https://api.coingecko.com/api/v3/coins/{symbol_id}"
        resp = requests.get(url, params={"localization": "false"}, timeout=TIMEOUT)
        if resp.status_code != 200:
            logger.error(f"❌ CoinGecko foutstatus {resp.status_code}: {resp.text}")
            return None

        data = resp.json()
        md = data.get("market_data", {}) or {}

        return {
            "price": float(md.get("current_price", {}).get("usd")),
            "volume": float(md.get("total_volume", {}).get("usd")),
            "change_24h": float(md.get("price_change_percentage_24h", 0) or 0),
        }
    except Exception:
        logger.error("❌ Fout bij ophalen CoinGecko market:")
        logger.error(traceback.format_exc())
        return None


# =====================================================
# 💾 Opslaan in market_data
# =====================================================
def store_market_indicator_db(symbol, indicator, value, score, trend, interpretation, action, source, ts):
    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding bij market-opslag.")
        return
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO market_data
                    (symbol, indicator, value, score, trend, interpretation, action, source, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                symbol,
                indicator,
                value,
                score,
                trend or "–",
                interpretation or "–",
                action or "–",
                source,
                ts or datetime.utcnow().replace(microsecond=0)
            ))
        conn.commit()
    except Exception:
        logger.error("❌ Fout bij opslaan market_data:")
        logger.error(traceback.format_exc())
    finally:
        conn.close()


# =====================================================
# 📊 Marktdata ophalen en scoren (via DB-regels)
# =====================================================
def process_market_now():
    cg_id = ASSETS.get("BTC", "bitcoin")
    live = fetch_coingecko_market(cg_id)
    if not live:
        logger.warning("⚠️ Geen live marketdata ontvangen.")
        return

    input_values = {
        "price": live["price"],
        "volume": live["volume"],
        "change_24h": live["change_24h"],
    }

    scored = generate_scores_db("technical", input_values)
    scores = (scored or {}).get("scores", {})
    ts = datetime.utcnow().replace(microsecond=0)

    for ind, result in scores.items():
        store_market_indicator_db(
            "BTC",
            ind,
            input_values.get(ind, 0),
            int(result.get("score", 10)),
            result.get("trend", "–"),
            result.get("interpretation", "–"),
            result.get("action", "–"),
            "coingecko",
            ts
        )


# =====================================================
# 🚀 Celery-taken
# =====================================================

# 1️⃣ Live marketdata (elke ±15 min)
@shared_task(name="backend.celery_task.market_task.fetch_market_data")
def fetch_market_data_task():
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
    """
    Update de BTC-prijsdata en berekent daarna forward returns.
    """
    logger.info("🔄 Start synchronisatie BTC-historie + forward returns...")
    try:
        update_btc_history()  # haalt alleen ontbrekende dagen op
        calculate_and_save_forward_returns.apply_async(countdown=5)
        logger.info("✅ Forward returns berekening ingepland.")
    except Exception:
        logger.error("❌ Fout in sync_price_history_and_returns()")
        logger.error(traceback.format_exc())


# 4️⃣ Forward returns berekenen en opslaan
@shared_task(name="backend.celery_task.market_task.calculate_and_save_forward_returns")
def calculate_and_save_forward_returns():
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
