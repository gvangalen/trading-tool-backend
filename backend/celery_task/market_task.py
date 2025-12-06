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
from backend.utils.market_interpreter import interpret_market_indicator  # ✅ centrale interpretatie

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
# 🔍 Market RAW Endpoints (database-driven)
# =====================================================
def load_market_raw_endpoints():
    """
    Haalt ALLE market_raw endpoints op:
    - btc_price
    - btc_ohlc
    - btc_volume
    - btc_change_24h

    Worden ALTIJD opgehaald, geen active-filter.
    (Deze zijn globaal, niet user-specifiek.)
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
# 🌐 RAW Market Data ophalen
# =====================================================
def fetch_raw_market_data():
    """
    Haalt price, volume en change_24h op via market_raw endpoints.
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
        # Prijs + 24h verandering
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

    return {
        "price": price,
        "volume": volume,
        "change_24h": change_24h
    }


# =====================================================
# 💾 RAW opslaan in DB (globaal, geen user-id)
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
        logger.info(f"💾 RAW market data opgeslagen voor {symbol}")

    except Exception:
        logger.error("❌ Fout opslaan market_data")
        logger.error(traceback.format_exc())

    finally:
        conn.close()


# =====================================================
# 🧮 Helper: indicator → waarde (op basis van RAW)
# =====================================================
def resolve_market_value(indicator_name: str, price: float, volume: float, change_24h: float):
    """
    Koppelt een indicatornaam aan de juiste ruwe waarde.

    Voorbeelden:
    - btc_change_24h   → change_24h
    - volume_strength  → volume
    - price_trend      → price
    - volatility       → abs(change_24h)
    """
    name = indicator_name.lower()

    if name == "btc_change_24h":
        return change_24h
    if name == "volume_strength":
        return volume
    if name == "price_trend":
        return price
    if name == "volatility":
        return abs(change_24h)

    # fallback: None → wordt geskipt
    return None


# =====================================================
# 📊 Market Scoring (per USER)
# =====================================================
def apply_market_scoring(user_id: int):
    """
    Zet de RAW market_data om naar indicator-scores per user
    in market_data_indicators.

    Gebruikt:
    - indicators(category='market', user_id=...)
    - market_indicator_rules(indicator, user_id, ...) via interpret_market_indicator()
    """
    conn = get_db_connection()

    try:
        with conn.cursor() as cur:

            # RAW ophalen (globaal)
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

            # Vandaag wissen voor deze user
            cur.execute("""
                DELETE FROM market_data_indicators
                WHERE DATE(timestamp) = CURRENT_DATE
                  AND user_id = %s
            """, (user_id,))

            # MARKET indicators ophalen per user
            cur.execute("""
                SELECT name
                FROM indicators
                WHERE category='market'
                  AND active = TRUE
                  AND user_id = %s
            """, (user_id,))
            indicators = [r[0] for r in cur.fetchall()]

            for ind in indicators:
                # 1️⃣ juiste ruwe waarde bepalen
                value = resolve_market_value(ind, price, volume, change_24h)
                if value is None:
                    logger.warning(f"⚠️ Geen waarde voor market-indicator '{ind}' (user_id={user_id})")
                    continue

                # 2️⃣ interpretatie + score via centrale helper (DB rules)
                interpretation = interpret_market_indicator(ind, value, user_id)
                if not interpretation:
                    logger.warning(f"⚠️ Geen interpretatie voor market-indicator '{ind}' (user_id={user_id})")
                    continue

                score = interpretation.get("score", 50)
                trend = interpretation.get("trend", "")
                interp = interpretation.get("interpretation", "")
                action = interpretation.get("action", "")

                # 3️⃣ opslaan in market_data_indicators
                cur.execute("""
                    INSERT INTO market_data_indicators
                        (user_id, name, value, trend, interpretation, action, score, timestamp)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                """, (user_id, ind, value, trend, interp, action, score))

        conn.commit()
        logger.info(f"📊 Market scoring uitgevoerd voor user_id={user_id}")

    except Exception:
        logger.error("❌ Fout bij market scoring")
        logger.error(traceback.format_exc())

    finally:
        conn.close()


# =====================================================
# 🔁 RAW → DB → SCORE (per USER)
# =====================================================
def process_market_now(user_id: int):
    raw = fetch_raw_market_data()
    if not raw:
        logger.warning("⚠️ Geen raw market data ontvangen.")
        return

    store_market_data_db("BTC", raw["price"], raw["volume"], raw["change_24h"])
    apply_market_scoring(user_id=user_id)


# =====================================================
# 🚀 Celery Task (15m) — per USER
# =====================================================
@shared_task(name="backend.celery_task.market_task.fetch_market_data")
def fetch_market_data_task(user_id: int):
    """
    Live market update + scoring voor een specifieke user.
    In Celery beat kun je bv. user_id=1 doorgeven.
    """
    logger.info(f"📈 Start live market data taak voor user_id={user_id}...")
    try:
        process_market_now(user_id=user_id)
        Path(CACHE_FILE).touch()
        logger.info("✅ Live market data verwerkt.")
    except Exception:
        logger.error("❌ Fout in fetch_market_data_task()")
        logger.error(traceback.format_exc())


# =====================================================
# 🕛 Dagelijkse snapshot (globaal, gebruikt default user)
# =====================================================
@shared_task(name="backend.celery_task.market_task.save_market_data_daily")
def save_market_data_daily():
    logger.info("🕛 Dagelijkse market snapshot gestart...")

    try:
        # Gebruik een default user voor de dagelijkse scoring (bijv. admin user_id=1)
        process_market_now(user_id=1)
        logger.info("✅ Raw + scoring gedaan.")

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

        # En update de laatste 7 extra dagen
        fetch_market_data_7d()

    except Exception:
        logger.error("❌ Fout in save_market_data_daily")
        logger.error(traceback.format_exc())


# =====================================================
# 📆 7-daagse update (database-driven URLs, globaal)
# =====================================================
@shared_task(name="backend.celery_task.market_task.fetch_market_data_7d")
def fetch_market_data_7d():
    logger.info("📆 Start fetch_market_data_7d...")
    conn = get_db_connection()

    if not conn:
        logger.error("❌ Geen DB-verbinding.")
        return

    try:
        # OHLC URL uit DB
        with conn.cursor() as cur:
            cur.execute("""
                SELECT link FROM indicators
                WHERE name='btc_ohlc' AND category='market_raw'
            """)
            row = cur.fetchone()

        binance_url = row[0] if row else "https://api.binance.com/api/v3/klines"

        params = {"symbol": "BTCUSDT", "interval": "1d", "limit": 7}
        candles = safe_get(binance_url, params=params)

        # Volume uit market_raw endpoint
        with conn.cursor() as cur:
            cur.execute("""
                SELECT link FROM indicators
                WHERE name='btc_volume' AND category='market_raw'
            """)
            vr = cur.fetchone()

        volume_url = vr[0] if vr else "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart"

        vol_data = safe_get(volume_url, params={"vs_currency": "usd", "days": "7"})
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
                    INSERT INTO market_data_7d 
                    (symbol, date, open, high, low, close, change, volume, created_at)
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
# 📈 Forward returns (globaal, geen user-id)
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

    except Exception:
        logger.error("❌ Fout in calculate_and_save_forward_returns")
        logger.error(traceback.format_exc())

    finally:
        if 'conn' in locals() and conn:
            conn.close()
