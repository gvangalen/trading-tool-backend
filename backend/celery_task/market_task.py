import logging
import requests
from datetime import datetime
from collections import defaultdict

from celery import shared_task
from tenacity import retry, stop_after_attempt, wait_exponential

from backend.utils.db import get_db_connection
from backend.celery_task.btc_price_history_task import update_btc_history
from backend.utils.scoring_utils import generate_scores_db

# =====================================================
# ⚙️ Config
# =====================================================
TIMEOUT = 10
HEADERS = {"Content-Type": "application/json"}
SYMBOL = "BTC"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
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
# 🔍 Market RAW endpoints (DB-gedreven, GLOBAAL)
# =====================================================
def load_market_raw_endpoints():
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT name, link
                FROM indicators
                WHERE category = 'market_raw'
                  AND active = TRUE
            """)
            return {r[0]: r[1] for r in cur.fetchall()}
    finally:
        conn.close()

# =====================================================
# 🌐 RAW market data ophalen (GLOBAAL)
# =====================================================
def fetch_raw_market_data():
    endpoints = load_market_raw_endpoints()

    change_url = endpoints.get(
        "btc_change_24h",
        "https://api.coingecko.com/api/v3/simple/price"
        "?ids=bitcoin&vs_currencies=usd&include_24hr_change=true"
    )

    volume_url = endpoints.get(
        "btc_volume",
        "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart"
    )

    r = safe_get(change_url)
    price = float(r["bitcoin"]["usd"])
    change_24h = float(r["bitcoin"]["usd_24h_change"])

    r2 = safe_get(volume_url, params={"vs_currency": "usd", "days": "1"})
    volume = float(r2["total_volumes"][-1][1]) if r2.get("total_volumes") else None

    return {
        "price": price,
        "volume": volume,
        "change_24h": change_24h,
    }

# =====================================================
# 💾 Opslaan market_data (GLOBAAL)
# =====================================================
def store_market_data_db(symbol, price, volume, change_24h):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO market_data
                    (symbol, price, volume, change_24h, timestamp)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                symbol,
                price,
                volume,
                change_24h,
                datetime.utcnow()
            ))
        conn.commit()
        logger.info("💾 market_data opgeslagen (globaal).")
    except Exception:
        conn.rollback()
        logger.error("❌ Fout bij opslaan market_data", exc_info=True)
    finally:
        conn.close()

# =====================================================
# 🔁 RAW → DB helper
# =====================================================
def process_market_now():
    raw = fetch_raw_market_data()
    if not raw:
        logger.warning("⚠️ Geen RAW market data ontvangen.")
        return
    store_market_data_db(
        SYMBOL,
        raw["price"],
        raw["volume"],
        raw["change_24h"]
    )

# =====================================================
# 🚀 Celery — live market update (GLOBAAL)
# =====================================================
@shared_task(name="backend.celery_task.market_task.fetch_market_data")
def fetch_market_data():
    logger.info("📈 Start live market RAW fetch...")
    try:
        process_market_now()
        logger.info("✅ Live market RAW data verwerkt.")
    except Exception:
        logger.error("❌ Fout in fetch_market_data", exc_info=True)

# =====================================================
# 🕛 Dagelijkse snapshot (GLOBAAL)
# =====================================================
@shared_task(name="backend.celery_task.market_task.save_market_data_daily")
def save_market_data_daily():
    logger.info("🕛 Dagelijkse market snapshot gestart...")
    try:
        process_market_now()
        update_btc_history()
        logger.info("✅ Dagelijkse market snapshot voltooid.")
    except Exception:
        logger.error("❌ Fout in save_market_data_daily", exc_info=True)

# =====================================================
# 📆 7-daagse OHLC + volume (GLOBAAL)
# =====================================================
@shared_task(name="backend.celery_task.market_task.fetch_market_data_7d")
def fetch_market_data_7d():
    logger.info("📆 Start fetch_market_data_7d...")
    conn = get_db_connection()

    try:
        binance_url = "https://api.binance.com/api/v3/klines"
        candles = safe_get(binance_url, params={
            "symbol": "BTCUSDT",
            "interval": "1d",
            "limit": 7
        })

        vol_data = safe_get(
            "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart",
            params={"vs_currency": "usd", "days": "7"}
        )

        volume_by_date = defaultdict(list)
        for ts, vol in vol_data.get("total_volumes", []):
            date = datetime.utcfromtimestamp(ts / 1000).date()
            volume_by_date[date].append(vol)

        avg_volume = {d: sum(v) / len(v) for d, v in volume_by_date.items()}

        with conn.cursor() as cur:
            for c in candles:
                date = datetime.utcfromtimestamp(int(c[0]) / 1000).date()
                open_p, high_p, low_p, close_p = map(float, c[1:5])
                change = round(((close_p - open_p) / open_p) * 100, 2)
                volume = float(avg_volume.get(date, 0))

                cur.execute("""
                    INSERT INTO market_data_7d
                        (symbol, date, open, high, low, close, change, volume, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (symbol, date)
                    DO UPDATE SET
                        open = EXCLUDED.open,
                        high = EXCLUDED.high,
                        low = EXCLUDED.low,
                        close = EXCLUDED.close,
                        change = EXCLUDED.change,
                        volume = EXCLUDED.volume,
                        created_at = NOW();
                """, (
                    SYMBOL, date,
                    open_p, high_p, low_p, close_p,
                    change, volume
                ))

        conn.commit()
        logger.info("✅ market_data_7d bijgewerkt.")
    except Exception:
        logger.error("❌ Fout in fetch_market_data_7d", exc_info=True)
    finally:
        conn.close()

# =====================================================
# 📈 Forward returns (GLOBAAL)
# =====================================================
@shared_task(name="backend.celery_task.market_task.calculate_and_save_forward_returns")
def calculate_and_save_forward_returns():
    logger.info("📈 Forward returns berekenen...")
    conn = get_db_connection()

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT date, price
                FROM btc_price_history
                ORDER BY date ASC
            """)
            rows = cur.fetchall()

        if not rows:
            return

        data = [{"date": r[0], "price": float(r[1])} for r in rows]
        periods = [7, 30, 90]

        with conn.cursor() as cur:
            for i, start in enumerate(data):
                for days in periods:
                    j = i + days
                    if j >= len(data):
                        continue

                    end = data[j]
                    change = ((end["price"] - start["price"]) / start["price"]) * 100
                    avg_daily = change / days

                    cur.execute("""
                        INSERT INTO market_forward_returns
                            (symbol, period, start_date, end_date, change, avg_daily)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (symbol, period, start_date)
                        DO UPDATE SET
                            end_date = EXCLUDED.end_date,
                            change = EXCLUDED.change,
                            avg_daily = EXCLUDED.avg_daily;
                    """, (
                        SYMBOL,
                        f"{days}d",
                        start["date"],
                        end["date"],
                        round(change, 2),
                        round(avg_daily, 3)
                    ))

        conn.commit()
        logger.info("✅ Forward returns opgeslagen.")
    except Exception:
        logger.error("❌ Fout in calculate_and_save_forward_returns", exc_info=True)
    finally:
        conn.close()

# =====================================================
# 📊 USER-AWARE MARKET INDICATORS (FINAL & CORRECT)
# =====================================================
def fetch_and_process_market_indicators(user_id: int):
    logger.info(f"📊 Market indicators ingestie gestart (user_id={user_id})")

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding")
        return

    try:
        # 1️⃣ Actieve market-indicators VOOR DEZE USER
        with conn.cursor() as cur:
            cur.execute("""
                SELECT name
                FROM indicators
                WHERE category = 'market'
                  AND active = TRUE
                  AND user_id = %s
            """, (user_id,))
            indicators = [r[0] for r in cur.fetchall()]

        if not indicators:
            logger.info("ℹ️ Geen actieve market-indicators voor user")
            return

        # 2️⃣ Laatste globale market snapshot
        with conn.cursor() as cur:
            cur.execute("""
                SELECT price, volume, change_24h
                FROM market_data
                ORDER BY timestamp DESC
                LIMIT 1
            """)
            row = cur.fetchone()

        if not row:
            logger.warning("⚠️ Geen market_data beschikbaar")
            return

        market_values = {
            "price": row[0],
            "volume": row[1],
            "change_24h": row[2],
        }

        # 3️⃣ Score + opslag per indicator
        for name in indicators:
            value = market_values.get(name)
            if value is None:
                continue

            # ✅ FIX: market rules zijn globaal → GEEN user_id filteren
            score_data = generate_scores_db(
                category="market",
                data={name: value}
            )

            score = score_data.get("scores", {}).get(name)
            if not score:
                logger.warning(f"⚠️ Geen scoreregels gevonden voor market indicator: {name}")
                continue

            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO market_data_indicators
                        (user_id, name, value, trend, interpretation, action, score, timestamp)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                """, (
                    user_id,
                    name,
                    value,
                    score.get("trend"),
                    score.get("interpretation"),
                    score.get("action"),
                    score.get("score"),
                ))

        conn.commit()
        logger.info(f"✅ Market indicators opgeslagen (user_id={user_id})")

    except Exception:
        conn.rollback()
        logger.error("❌ Fout in market indicators", exc_info=True)

    finally:
        conn.close()


# =====================================================
# 🚀 Celery wrapper (via dispatcher)
# =====================================================
@shared_task(name="backend.celery_task.market_task.fetch_market_indicators")
def fetch_market_indicators(user_id: int):
    if user_id is None:
        raise ValueError("❌ user_id verplicht")
    fetch_and_process_market_indicators(user_id)
