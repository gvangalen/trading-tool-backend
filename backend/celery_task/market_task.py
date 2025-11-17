import logging
import traceback
import requests
from datetime import datetime
from pathlib import Path
from tenacity import retry, stop_after_attempt, wait_exponential
from celery import shared_task
from collections import defaultdict

# Eigen imports
from backend.utils.db import get_db_connection
from backend.celery_task.btc_price_history_task import update_btc_history  # blijft beschikbaar

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
# 🔍 DB Helper — Market RAW endpoints
# =====================================================
def load_market_raw_endpoints():
    """Laadt alle actieve market_raw endpoints uit de DB (btc_price, btc_volume, btc_change_24h)."""
    conn = get_db_connection()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT name, link
            FROM indicators
            WHERE category = 'market_raw' AND active = TRUE
        """)
        endpoints = {r[0]: r[1] for r in cur.fetchall()}
    conn.close()
    return endpoints


# =====================================================
# 🌐 Market RAW ophalen via database-endpoints
# =====================================================
def fetch_raw_market_data():
    """
    Haalt BTC price, volume en change_24h op via de market_raw endpoints in de DB.
    Verwacht indicatoren:
      - btc_price
      - btc_change_24h
      - btc_volume
    """
    endpoints = load_market_raw_endpoints()

    price_url = endpoints.get(
        "btc_price",
        "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd"
    )
    change_url = endpoints.get(
        "btc_change_24h",
        "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd&include_24hr_change=true"
    )
    volume_url = endpoints.get(
        "btc_volume",
        "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart?vs_currency=usd&days=1"
    )

    with requests.Session() as s:
        # Price + Change ophalen
        r = s.get(change_url, timeout=10).json()

        if "bitcoin" in r:
            price = r["bitcoin"]["usd"]
            change_24h = r["bitcoin"]["usd_24h_change"]
        else:
            md = r["market_data"]
            price = md["current_price"]["usd"]
            change_24h = md["price_change_percentage_24h"]

        # Volume ophalen
        r2 = s.get(volume_url, timeout=10).json()
        if "total_volumes" in r2:
            volume = r2["total_volumes"][-1][1]
        else:
            volume = None

    return {
        "price": float(price),
        "volume": float(volume),
        "change_24h": float(change_24h)
    }


# =====================================================
# 💾 RAW Market Data opslaan
# =====================================================
def store_market_data_db(symbol, price, volume, change_24h):
    """Slaat raw market data zonder upsert op (jouw table ondersteunt upsert niet)."""
    conn = get_db_connection()
    ts = datetime.utcnow()

    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO market_data (symbol, price, volume, change_24h, timestamp)
                VALUES (%s, %s, %s, %s, %s)
            """, (symbol, price, volume, change_24h, ts))
        conn.commit()
        logger.info(f"💾 RAW market data opgeslagen voor {symbol}")

    except Exception:
        logger.error("❌ Fout opslaan market_data")
        logger.error(traceback.format_exc())

    finally:
        conn.close()


# =====================================================
# 📊 Market Scoring (market → market_indicator_rules → market_data_indicators)
# =====================================================
def apply_market_scoring():
    """Zet raw market_data om naar gescoorde indicatoren in market_data_indicators."""
    conn = get_db_connection()

    try:
        with conn.cursor() as cur:
            # 1️⃣ Haal laatste RAW market_data op
            cur.execute("""
                SELECT price, volume, change_24h
                FROM market_data
                WHERE symbol='BTC'
                ORDER BY timestamp DESC
                LIMIT 1
            """)
            raw = cur.fetchone()

            if not raw:
                logger.warning("⚠️ Geen raw market data beschikbaar voor scoring.")
                return

            price, volume, change_24h = raw

            # 2️⃣ Haal alle scorebare indicatoren (category='market')
            cur.execute("""
                SELECT DISTINCT indicator FROM market_indicator_rules
            """)
            indicators = [r[0] for r in cur.fetchall()]

            # 3️⃣ Per indicator → bepaal raw value → selecteer bijpassende regel → opslaan
            for ind in indicators:

                # juiste raw waarde kiezen
                if ind == "btc_change_24h":
                    value = change_24h
                elif ind == "volume_strength":
                    value = volume
                elif ind == "price_trend":
                    value = price
                elif ind == "volatility":
                    value = abs(change_24h)  # voorlopig: vol = absolute change
                else:
                    continue

                # regels ophalen
                cur.execute("""
                    SELECT range_min, range_max, score, trend, interpretation, action
                    FROM market_indicator_rules
                    WHERE indicator=%s
                    ORDER BY range_min ASC
                """, (ind,))
                rules = cur.fetchall()

                rule = next((r for r in rules if r[0] <= value < r[1]), None)
                if not rule:
                    continue

                range_min, range_max, score, trend, interp, action = rule

                cur.execute("""
                    INSERT INTO market_data_indicators
                        (name, value, trend, interpretation, action, score, timestamp)
                    VALUES (%s, %s, %s, %s, %s, %s, NOW())
                """, (ind, value, trend, interp, action, score))

        conn.commit()
        logger.info("📊 Market scoring uitgevoerd en opgeslagen.")

    except Exception:
        logger.error("❌ Fout bij market scoring:")
        logger.error(traceback.format_exc())

    finally:
        conn.close()


# =====================================================
# 🔁 Combined processing
# =====================================================
def process_market_now():
    """Complete pipeline: RAW ophalen → opslaan → scoring."""
    raw = fetch_raw_market_data()
    if not raw:
        logger.warning("⚠️ Geen raw market data ontvangen.")
        return

    store_market_data_db("BTC", raw["price"], raw["volume"], raw["change_24h"])
    apply_market_scoring()


# =====================================================
# 🚀 Celery Task: Elke ±15 min RAW + Scoring
# =====================================================
@shared_task(name="backend.celery_task.market_task.fetch_market_data")
def fetch_market_data_task():
    logger.info("📈 Start live market data taak...")
    try:
        process_market_now()
        Path(CACHE_FILE).touch()
        logger.info("✅ Live market data verwerkt.")
    except Exception:
        logger.error("❌ Fout in fetch_market_data_task()")
        logger.error(traceback.format_exc())


# =====================================================
# 🕛 Dagelijks snapshot (Binance 1d OHLC)
# =====================================================
@shared_task(name="backend.celery_task.market_task.save_market_data_daily")
def save_market_data_daily():
    """
    Dagelijkse snapshot:
    - RAW marketdata (via process_market_now)
    - Binance 1d candle
    - 7d update
    """
    logger.info("🕛 Dagelijkse market snapshot gestart...")

    try:
        # 1️⃣ Raw snapshot + scoring
        process_market_now()
        logger.info("✅ Raw snapshot + scoring gedaan.")

        # 2️⃣ Binance candle ophalen
        logger.info("📊 Ophalen Binance OHLC candle (1d) ...")
        url = "https://api.binance.com/api/v3/klines"
        params = {"symbol": "BTCUSDT", "interval": "1d", "limit": 1}
        candles = safe_get(url, params=params)

        if candles:
            k = candles[-1]
            open_p = float(k[1])
            high_p = float(k[2])
            low_p = float(k[3])
            close_p = float(k[4])
            base_vol_btc = float(k[5])
            quote_vol_usd = float(k[7])
            change = round(((close_p - open_p) / open_p) * 100, 2)

            conn = get_db_connection()
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO market_data_7d (symbol, date, open, high, low, close, change, volume, created_at)
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

            logger.info(f"✅ Dagrecord bijgewerkt | Δ{change:+.2f}% | VolumeUSD:{quote_vol_usd}")

        # 3️⃣ 7-daagse update
        fetch_market_data_7d()
        logger.info("🔁 7-daagse update klaar.")

    except Exception:
        logger.error("❌ Fout in save_market_data_daily")
        logger.error(traceback.format_exc())


# =====================================================
# 📆 7-daagse Binance + CoinGecko volume
# =====================================================
@shared_task(name="backend.celery_task.market_task.fetch_market_data_7d")
def fetch_market_data_7d():
    logger.info("📆 Start fetch_market_data_7d...")
    conn = get_db_connection()

    if not conn:
        logger.error("❌ Geen DB-verbinding.")
        return

    try:
        # Binance 7 days OHLC
        binance_url = "https://api.binance.com/api/v3/klines"
        params = {"symbol": "BTCUSDT", "interval": "1d", "limit": 7}
        candles = safe_get(binance_url, params=params)

        # CoinGecko 7 days volume
        cg_url = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart"
        vol_data = safe_get(cg_url, params={"vs_currency": "usd", "days": "7"})
        volume_points = vol_data.get("total_volumes", [])

        volume_by_date = defaultdict(list)
        for ts, vol in volume_points:
            date = datetime.utcfromtimestamp(ts / 1000).date()
            volume_by_date[date].append(vol)
        avg_volume = {d: sum(v) / len(v) for d, v in volume_by_date.items()}

        inserted = 0
        with conn.cursor() as cur:
            for c in candles:
                ts = int(c[0])
                date = datetime.utcfromtimestamp(ts / 1000).date()

                open_p = float(c[1])
                high_p = float(c[2])
                low_p = float(c[3])
                close_p = float(c[4])
                change = round(((close_p - open_p) / open_p) * 100, 2)
                volume = float(avg_volume.get(date, 0.0))

                cur.execute("""
                    INSERT INTO market_data_7d (symbol, date, open, high, low, close, change, volume, created_at)
                    VALUES ('BTC', %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (symbol, date)
                    DO UPDATE SET
                        open=EXCLUDED.open,
                        high=EXCLUDED.high,
                        low=EXCLUDED.low,
                        close=EXCLUDED.close,
                        change=EXCLUDED.change,
                        volume=EXCLUDED.volume,
                        created_at=NOW();
                """, (date, open_p, high_p, low_p, close_p, change, volume))
                inserted += 1

        conn.commit()
        logger.info(f"✅ market_data_7d updated ({inserted} rows).")

    except Exception:
        logger.error("❌ Fout in fetch_market_data_7d")
        logger.error(traceback.format_exc())

    finally:
        conn.close()


# =====================================================
# 📈 Forward returns (blijft zoals origineel)
# =====================================================
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
            for days in periods:
                j = i + days
                if j >= len(data):
                    continue
                end = data[j]
                change = ((end["price"] - start["price"]) / start["price"]) * 100
                avg_daily = change / days
                results.append((start["date"], end["date"], days, change, avg_daily))

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
                        avg_daily = EXCLUDED.avg_daily;
                """, (f"{days}d", s_date, e_date, round(change, 2), round(avg_daily, 3)))

        conn.commit()
        logger.info(f"✅ Forward returns opgeslagen ({len(results)} rows).")

    except Exception:
        logger.error("❌ Fout in calculate_and_save_forward_returns")
        logger.error(traceback.format_exc())

    finally:
        if 'conn' in locals() and conn:
            conn.close()
