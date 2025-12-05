import logging
import traceback
import requests
from datetime import datetime
from pathlib import Path
from tenacity import retry, stop_after_attempt, wait_exponential
from celery import shared_task
from collections import defaultdict

# DB utils
from backend.utils.db import get_db_connection

# === Config ===
TIMEOUT = 10
HEADERS = {"Content-Type": "application/json"}
CACHE_FILE = "/tmp/last_market_data_fetch.txt"

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
# 🌐 RAW MARKET ENDPOINTS (GEEN USER-ID)
# =====================================================
def load_market_raw_endpoints():
    """
    Market raw endpoints zijn globaal — niet per user.
    """
    conn = get_db_connection()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT name, link
            FROM indicators
            WHERE category = 'market_raw'
        """)
        endpoints = {r[0]: r[1] for r in cur.fetchall()}
    conn.close()
    return endpoints


# =====================================================
# 🌐 RAW MARKET DATA
# =====================================================
def fetch_raw_market_data():
    """
    Haalt actuele BTC prijs, volume en 24h-change op.
    """
    endpoints = load_market_raw_endpoints()

    price_url = endpoints.get("btc_price")
    change_url = endpoints.get("btc_change_24h")
    volume_url = endpoints.get("btc_volume")

    with requests.Session() as s:
        # Prijs + change 24h
        r = s.get(change_url, timeout=10).json()
        if "bitcoin" in r:
            price = float(r["bitcoin"]["usd"])
            change_24h = float(r["bitcoin"]["usd_24h_change"])
        else:
            md = r.get("market_data", {})
            price = float(md.get("current_price", {}).get("usd"))
            change_24h = float(md.get("price_change_percentage_24h"))

        # Volume ophalen
        r2 = s.get(volume_url, timeout=10).json()
        if "total_volumes" in r2:
            volume = float(r2["total_volumes"][-1][1])
        else:
            volume = None

    return {"price": price, "volume": volume, "change_24h": change_24h}


# =====================================================
# 💾 RAW opslaan (GEEN USER-ID)
# =====================================================
def store_market_data_db(symbol, price, volume, change_24h):
    conn = get_db_connection()
    ts = datetime.utcnow()

    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO market_data (symbol, price, volume, change_24h, timestamp)
                VALUES (%s, %s, %s, %s, %s)
            """, (symbol, price, volume, change_24h, ts))
        conn.commit()
    except Exception:
        logger.error("❌ Fout opslaan market_data")
        logger.error(traceback.format_exc())
    finally:
        conn.close()


# =====================================================
# 📊 MARKET SCORING (HIER KOMT user_id!)
# =====================================================
def apply_market_scoring(user_id: int):
    conn = get_db_connection()

    try:
        with conn.cursor() as cur:

            # RAW ophalen
            cur.execute("""
                SELECT price, volume, change_24h
                FROM market_data
                WHERE symbol='BTC'
                ORDER BY timestamp DESC
                LIMIT 1
            """)
            raw = cur.fetchone()

            if not raw:
                logger.warning("⚠️ Geen RAW data beschikbaar voor scoring.")
                return

            price, volume, change_24h = raw

            # Vandaag wissen voor deze gebruiker
            cur.execute("""
                DELETE FROM market_data_indicators 
                WHERE DATE(timestamp) = CURRENT_DATE
                AND user_id = %s
            """, (user_id,))

            # Gelabelde indicatoren ophalen
            cur.execute("""
                SELECT name
                FROM indicators
                WHERE category='market'
                AND active = TRUE
                AND user_id = %s
            """, (user_id,))

            indicators = [r[0] for r in cur.fetchall()]

            for ind in indicators:

                # juiste raw waarde
                if ind == "btc_change_24h":
                    value = change_24h
                elif ind == "volume_strength":
                    value = volume
                elif ind == "price_trend":
                    value = price
                elif ind == "volatility":
                    value = abs(change_24h)
                else:
                    continue

                # scoreregels ophalen
                cur.execute("""
                    SELECT range_min, range_max, score, trend, interpretation, action
                    FROM market_indicator_rules
                    WHERE indicator=%s AND user_id=%s
                    ORDER BY range_min ASC
                """, (ind, user_id))

                rules = cur.fetchall()
                rule = next((r for r in rules if r[0] <= value < r[1]), None)
                if not rule:
                    continue

                range_min, range_max, score, trend, interp, action = rule

                cur.execute("""
                    INSERT INTO market_data_indicators
                        (user_id, name, value, trend, interpretation, action, score, timestamp)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                """, (
                    user_id,
                    ind,
                    value,
                    trend,
                    interp,
                    action,
                    score
                ))

        conn.commit()
        logger.info(f"📊 Market scoring uitgevoerd voor user_id={user_id}")

    except Exception:
        logger.error("❌ Fout bij market scoring")
        logger.error(traceback.format_exc())

    finally:
        conn.close()


# =====================================================
# 🔁 RAW → SCORE (USER-ID)
# =====================================================
def process_market_now(user_id: int):
    raw = fetch_raw_market_data()
    if not raw:
        logger.warning("⚠️ Geen raw market data ontvangen.")
        return

    store_market_data_db("BTC", raw["price"], raw["volume"], raw["change_24h"])
    apply_market_scoring(user_id=user_id)


# =====================================================
# 🚀 LIVE MARKET TASK (15m)
# =====================================================
@shared_task(name="backend.celery_task.market_task.fetch_market_data")
def fetch_market_data_task(user_id: int):
    logger.info(f"📈 Start market task voor user_id={user_id}")
    try:
        process_market_now(user_id=user_id)
        Path(CACHE_FILE).touch()
        logger.info("✅ Market data verwerkt.")
    except Exception:
        logger.error("❌ Fout in fetch_market_data_task")
        logger.error(traceback.format_exc())


# =====================================================
# 🕛 DAGELIJKSE SNAPSHOT (GEEN USER-ID)
# =====================================================
@shared_task(name="backend.celery_task.market_task.save_market_data_daily")
def save_market_data_daily():
    """
    Dagelijkse snapshot voor grafieken.
    Niet user-specifiek.
    """
    logger.info("🕛 Dagelijkse market snapshot gestart...")
    try:
        process_market_now(user_id=1)  # scoring voor admin zodat dataset compleet blijft

        # Binance OHLC voor vandaag
        url = "https://api.binance.com/api/v3/klines"
        params = {"symbol": "BTCUSDT", "interval": "1d", "limit": 1}
        candles = safe_get(url, params=params)

        if candles:
            k = candles[-1]
            open_p = float(k[1])
            high_p = float(k[2])
            low_p = float(k[3])
            close_p = float(k[4])
            quote_vol_usd = float(k[7])
            change = round(((close_p - open_p) / open_p) * 100, 2)

            conn = get_db_connection()
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO market_data_7d
                        (symbol, date, open, high, low, close, change, volume, created_at)
                    VALUES ('BTC', CURRENT_DATE, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (symbol, date)
                    DO UPDATE SET
                        open = EXCLUDED.open,
                        high = EXCLUDED.high,
                        low = EXCLUDED.low,
                        close = EXCLUDED.close,
                        change = EXCLUDED.change,
                        volume = EXCLUDED.volume,
                        created_at = NOW();
                """, (open_p, high_p, low_p, close_p, change, quote_vol_usd))

            conn.commit()
            conn.close()

    except Exception:
        logger.error("❌ Fout in save_market_data_daily")
        logger.error(traceback.format_exc())
