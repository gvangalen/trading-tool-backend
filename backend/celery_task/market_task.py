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
# 📊 USER-AWARE MARKET INDICATORS (PRO DEBUG + MAPPING)
# =====================================================
def fetch_and_process_market_indicators(user_id: int):
    logger.info("========================================")
    logger.info(f"📊 START market indicators ingestie (user_id={user_id})")

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding (market indicators)")
        return

    inserted = 0

    try:
        # 1️⃣ Actieve market-indicators
        with conn.cursor() as cur:
            cur.execute("""
                SELECT name
                FROM indicators
                WHERE category = 'market'
                  AND active = TRUE
                ORDER BY name
            """)
            indicators = [r[0] for r in cur.fetchall()]

        logger.info(f"📊 Actieve market indicators: {indicators}")
        if not indicators:
            return

        # 2️⃣ Laatste 2 market snapshots (voor volume %)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT price, volume, change_24h, timestamp
                FROM market_data
                ORDER BY timestamp DESC
                LIMIT 2
            """)
            rows = cur.fetchall()

        if not rows:
            logger.warning("⚠️ Geen market_data gevonden")
            return

        latest = rows[0]
        previous = rows[1] if len(rows) > 1 else None

        price_now, volume_now, change_24h, ts = latest
        volume_prev = previous[1] if previous else None

        logger.info(
            f"🕒 Snapshot: price={price_now}, volume={volume_now}, "
            f"change_24h={change_24h}"
        )

        # 3️⃣ Volume % verandering (ABS)
        volume_change_pct = None
        if volume_prev and volume_prev > 0 and volume_now:
            volume_change_pct = abs(
                ((volume_now - volume_prev) / volume_prev) * 100
            )

        logger.info(f"📈 volume_change_pct={volume_change_pct}")

        # 4️⃣ Indicator → waarde (%)
        indicator_value_map = {
            "change_24h": float(change_24h),        # al %
            "volume": float(volume_change_pct) if volume_change_pct is not None else None,
        }

        # 5️⃣ Score + opslag per user
        for name in indicators:
            logger.info(f"➡️ Verwerk market indicator: {name}")

            value = indicator_value_map.get(name)

            if value is None:
                logger.warning(f"⚠️ Geen value voor {name}, skip")
                continue

            logger.info(f"📊 {name} value_for_scoring={value}")

            score_data = generate_scores_db(
                category="market",
                data={name: value},
                user_id=user_id,
            )

            score_obj = (score_data or {}).get("scores", {}).get(name)
            if not score_obj:
                logger.warning(f"⚠️ Geen scoreregels voor {name}")
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
                    score_obj["trend"],
                    score_obj["interpretation"],
                    score_obj["action"],
                    score_obj["score"],
                ))

            inserted += 1
            logger.info(f"💾 Insert OK: {name}")

        conn.commit()
        logger.info(f"✅ EINDE market ingestie — inserted={inserted}")
        logger.info("========================================")

    except Exception:
        conn.rollback()
        logger.exception("❌ Fout in market indicators ingestie")
    finally:
        conn.close()

@shared_task(name="backend.celery_task.market_task.run_market_agent_daily")
def run_market_agent_daily(user_id: int):
    logger.info(f"🧠 START Market AI Agent (user_id={user_id})")

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # Haal laatste market indicatoren
            cur.execute("""
                SELECT name, score, interpretation, action
                FROM market_data_indicators
                WHERE user_id = %s
                ORDER BY timestamp DESC
            """, (user_id,))
            rows = cur.fetchall()

        if not rows:
            summary = (
                "Nog onvoldoende marktdata beschikbaar om een "
                "betrouwbare market-analyse te genereren."
            )
            top_signals = []
            score = None
            trend = "neutral"
            bias = "neutral"
            risk = "unknown"
        else:
            scores = [r[1] for r in rows if r[1] is not None]
            score = round(sum(scores) / len(scores), 1) if scores else None

            top_signals = [
                f"{r[0]}: {r[2]}" for r in rows[:3]
            ]

            summary = (
                "De marktanalyse is gebaseerd op recente prijsverandering "
                "en volumegedrag."
            )
            trend = "bullish" if score and score >= 60 else "bearish" if score and score <= 40 else "neutral"
            bias = trend
            risk = "hoog" if score and score >= 80 else "laag" if score and score <= 20 else "matig"

        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO ai_category_insights
                    (user_id, category, score, trend, bias, risk, summary, top_signals, created_at)
                VALUES (%s, 'market', %s, %s, %s, %s, %s, %s, NOW())
            """, (
                user_id,
                score,
                trend,
                bias,
                risk,
                summary,
                top_signals,
            ))

        conn.commit()
        logger.info(f"✅ Market AI insight opgeslagen (user_id={user_id})")

    except Exception:
        conn.rollback()
        logger.exception("❌ Fout in Market AI Agent")
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
