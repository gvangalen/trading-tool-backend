import os
import logging
import traceback
import requests
from datetime import datetime, timedelta
from pathlib import Path
from tenacity import retry, stop_after_attempt, wait_exponential
from celery import shared_task

# ✅ Eigen utils
from backend.utils.db import get_db_connection
from backend.utils.scoring_utils import generate_scores_db  # gebruikt DB-regels (technical_indicator_rules)

# === ✅ Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# === Config
TIMEOUT = 10
HEADERS = {"Content-Type": "application/json"}
CACHE_FILE = "/tmp/last_market_data_fetch.txt"
MIN_INTERVAL_MINUTES = 15
ASSETS = {"BTC": "bitcoin"}  # CoinGecko id mapping


# =====================================================
# ♻️ Helper: check rate limit
# =====================================================
def recently_fetched():
    """Controleer of marktdata minder dan X minuten geleden is opgehaald."""
    try:
        mtime = datetime.fromtimestamp(Path(CACHE_FILE).stat().st_mtime)
        if datetime.now() - mtime < timedelta(minutes=MIN_INTERVAL_MINUTES):
            logger.info(f"♻️ Marktdata < {MIN_INTERVAL_MINUTES} minuten oud, overslaan.")
            return True
    except FileNotFoundError:
        return False
    return False


# =====================================================
# 🔁 Safe request wrapper met retry
# =====================================================
@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=5, max=20), reraise=True)
def safe_get(url, params=None):
    """Veilige HTTP GET met retries."""
    resp = requests.get(url, headers=HEADERS, params=params, timeout=TIMEOUT)
    resp.raise_for_status()
    return resp.json()


# =====================================================
# 🌐 CoinGecko fetch helper
# =====================================================
def fetch_coingecko_market(symbol_id: str = "bitcoin"):
    """
    Haal marktdata op voor BTC via CoinGecko.
    Retourneert dict met 'price', 'volume', 'change_24h' of None bij fout.
    """
    try:
        url = f"https://api.coingecko.com/api/v3/coins/{symbol_id}"
        resp = requests.get(url, params={"localization": "false"}, timeout=TIMEOUT)

        if resp.status_code == 429:
            logger.warning("🚫 CoinGecko rate limit bereikt, overslaan.")
            return None
        if resp.status_code != 200:
            logger.error(f"❌ CoinGecko foutstatus {resp.status_code}: {resp.text}")
            return None

        data = resp.json()
        md = data.get("market_data", {}) or {}

        price = md.get("current_price", {}).get("usd")
        volume = md.get("total_volume", {}).get("usd")
        change = md.get("price_change_percentage_24h", 0)

        if price is None or volume is None:
            logger.warning("⚠️ Onvolledige CoinGecko market_data.")
            return None

        return {
            "price": float(price),
            "volume": float(volume),
            "change_24h": float(change or 0),
        }
    except Exception as e:
        logger.error(f"❌ Fout bij ophalen CoinGecko market: {e}")
        logger.error(traceback.format_exc())
        return None


# =====================================================
# 💾 Opslaan in market_data (1 rij per indicator)
# =====================================================
def store_market_indicator_db(symbol: str, indicator: str, value: float, score: int,
                              trend: str, interpretation: str, action: str,
                              source: str = "coingecko", ts: datetime | None = None):
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
                (ts or datetime.utcnow().replace(microsecond=0))
            ))
        conn.commit()
        logger.info(f"💾 market_data opgeslagen: {indicator} ({value}) score={score}")
    except Exception as e:
        logger.error(f"❌ Fout bij opslaan market_data ({indicator}): {e}")
        logger.error(traceback.format_exc())
    finally:
        conn.close()


# =====================================================
# 📊 Marktdata ophalen en scoren (via DB-regels - technical)
# =====================================================
def process_market_now():
    """
    Haalt de live marktdata op (price, volume, change_24h),
    scoort met DB-regels uit technical_indicator_rules,
    en slaat elke indicator op in market_data.
    """
    cg_id = ASSETS.get("BTC", "bitcoin")
    live = fetch_coingecko_market(symbol_id=cg_id)
    if not live:
        logger.warning("⚠️ Geen live marketdata beschikbaar.")
        return

    # 🧠 Scoreberekening via DB-scoreregels (we gebruiken category='technical')
    input_values = {
        "price": live["price"],
        "volume": live["volume"],
        "change_24h": live["change_24h"],
    }

    scored = generate_scores_db("technical", input_values)  # -> {"scores": {...}, "total_score": X}
    scores_dict = (scored or {}).get("scores", {})

    utc_now = datetime.utcnow().replace(microsecond=0)
    for indicator, result in scores_dict.items():
        store_market_indicator_db(
            symbol="BTC",
            indicator=indicator,
            value=input_values.get(indicator, 0.0),
            score=int(result.get("score", 10)),
            trend=result.get("trend", "–"),
            interpretation=result.get("interpretation", "–"),
            action=result.get("action", "–"),
            source="coingecko",
            ts=utc_now
        )


# =====================================================
# 🚀 Celery-taken
# =====================================================

# 1) Live market data elke ~15 min (meter/UX)
@shared_task(name="backend.celery_task.market_task.fetch_market_data")
def fetch_market_data_task():
    logger.info("📈 Start live marktdata taak...")
    if recently_fetched():
        return
    try:
        process_market_now()
        Path(CACHE_FILE).touch()
        logger.info("✅ Live marktdata verwerkt en opgeslagen.")
    except Exception:
        logger.error("❌ Fout in fetch_market_data_task()")
        logger.error(traceback.format_exc())


# 2) Dagelijkse snapshot (00:00 UTC of jouw cron) — voor historische views/rapport
@shared_task(name="backend.celery_task.market_task.save_market_data_daily")
def save_market_data_daily():
    logger.info("🕛 Dagelijkse market snapshot ophalen...")
    try:
        cg_id = ASSETS.get("BTC", "bitcoin")
        live = fetch_coingecko_market(symbol_id=cg_id)
        if not live:
            logger.warning("⚠️ Geen live marketdata voor daily snapshot.")
            return

        # Score via technical rules
        input_values = {
            "price": live["price"],
            "volume": live["volume"],
            "change_24h": live["change_24h"],
        }
        scored = generate_scores_db("technical", input_values)
        scores_dict = (scored or {}).get("scores", {})

        midnight = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        for indicator, result in scores_dict.items():
            store_market_indicator_db(
                symbol="BTC",
                indicator=indicator,
                value=input_values.get(indicator, 0.0),
                score=int(result.get("score", 10)),
                trend=result.get("trend", "–"),
                interpretation=result.get("interpretation", "–"),
                action=result.get("action", "–"),
                source="daily_close",
                ts=midnight
            )

        logger.info("✅ Dagelijkse market snapshot opgeslagen.")
    except Exception:
        logger.error("❌ Fout in save_market_data_daily()")
        logger.error(traceback.format_exc())


# 3) Historische BTC-prijs (365d) -> direct DB insert in btc_price_history
@shared_task(name="backend.celery_task.market_task.fetch_btc_price_history")
def fetch_btc_price_history():
    logger.info("⏳ BTC prijs historie (365d) ophalen...")
    try:
        cg_id = ASSETS.get("BTC", "bitcoin")
        url = f"https://api.coingecko.com/api/v3/coins/{cg_id}/market_chart"
        params = {"vs_currency": "usd", "days": "365", "interval": "daily"}

        data = safe_get(url, params=params)
        prices = data.get("prices", []) or []
        if not prices:
            logger.warning("⚠️ Geen 'prices' in CoinGecko response.")
            return

        conn = get_db_connection()
        if not conn:
            logger.error("❌ Geen DB-verbinding voor btc_price_history.")
            return

        inserted = 0
        with conn:
            with conn.cursor() as cur:
                for ts, price in prices:
                    date_iso = datetime.utcfromtimestamp(ts / 1000).date().isoformat()
                    cur.execute("""
                        INSERT INTO btc_price_history (date, price)
                        VALUES (%s, %s)
                        ON CONFLICT (date) DO UPDATE SET price = EXCLUDED.price
                    """, (date_iso, round(float(price), 2)))
                    inserted += 1

        logger.info(f"✅ btc_price_history upserted ({inserted} rijen).")
    except Exception:
        logger.error("❌ Fout in fetch_btc_price_history()")
        logger.error(traceback.format_exc())


# 4) Forward returns berekenen en opslaan in market_forward_returns
@shared_task(name="backend.celery_task.market_task.calculate_and_save_forward_returns")
def calculate_and_save_forward_returns():
    logger.info("📈 Forward returns berekenen...")
    try:
        conn = get_db_connection()
        if not conn:
            logger.error("❌ Geen DB-verbinding bij forward returns.")
            return

        with conn.cursor() as cur:
            cur.execute("SELECT date, price FROM btc_price_history ORDER BY date ASC")
            rows = cur.fetchall()

        series = [{"date": r[0], "price": float(r[1])} for r in rows]
        if not series:
            logger.warning("⚠️ Geen prijsdata in btc_price_history.")
            return

        periods = [7, 30, 90]
        payload = []

        for i, row in enumerate(series):
            for d in periods:
                j = i + d
                if j >= len(series):
                    continue
                start = row
                end = series[j]

                change = ((end["price"] - start["price"]) / start["price"]) * 100.0
                avg_daily = change / d

                payload.append({
                    "symbol": "BTC",
                    "period": f"{d}d",
                    "start_date": start["date"],
                    "end_date": end["date"],
                    "change": round(change, 2),
                    "avg_daily": round(avg_daily, 3),
                })

        if not payload:
            logger.warning("⚠️ Geen forward-returns te bewaren.")
            return

        conn = get_db_connection()
        if not conn:
            logger.error("❌ Geen DB-verbinding bij opslaan forward returns.")
            return

        inserted = 0
        with conn:
            with conn.cursor() as cur:
                for row in payload:
                    cur.execute("""
                        INSERT INTO market_forward_returns
                            (symbol, period, start_date, end_date, change, avg_daily)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (symbol, period, start_date) DO UPDATE
                            SET end_date = EXCLUDED.end_date,
                                change = EXCLUDED.change,
                                avg_daily = EXCLUDED.avg_daily
                    """, (
                        row["symbol"], row["period"], row["start_date"], row["end_date"],
                        row["change"], row["avg_daily"]
                    ))
                    inserted += 1

        logger.info(f"✅ Forward returns opgeslagen/ geüpdatet ({inserted} rijen).")
    except Exception:
        logger.error("❌ Fout in calculate_and_save_forward_returns()")
        logger.error(traceback.format_exc())


# 5) 7-daags aggregaat vullen uit eigen DB (optioneel – hier als voorbeeld)
@shared_task(name="backend.celery_task.market_task.save_market_data_7d")
def save_market_data_7d():
    """
    Bouw eenvoudig 7d aggregaat op uit market_data (laatste 7 unieke dagen).
    Past aan als je al een aparte API/route hebt — dit is de DB-native variant.
    """
    logger.info("📊 Opbouwen 7-daagse marktdata (DB-native)...")
    try:
        conn = get_db_connection()
        if not conn:
            logger.error("❌ Geen DB-verbinding.")
            return

        with conn.cursor() as cur:
            # Haal per dag de laatste waarde per indicator op (price/volume/change_24h)
            cur.execute("""
                WITH per_day AS (
                    SELECT
                        DATE(timestamp) AS dag,
                        indicator,
                        value,
                        ROW_NUMBER() OVER (PARTITION BY DATE(timestamp), indicator ORDER BY timestamp DESC) AS rn
                    FROM market_data
                    WHERE symbol = 'BTC'
                )
                SELECT dag, indicator, value
                FROM per_day
                WHERE rn = 1
                ORDER BY dag DESC
                LIMIT 21;  -- genoeg om tot 7 unieke dagen per indicator te komen
            """)
            rows = cur.fetchall()

        # Je kunt hier zelf een aggregaat maken en opslaan in market_data_7d.
        # Voorbeeld: laatste 7 dagen 'price' middelen, idem volume, etc.
        # (Implementatie hangt af van jouw gewenste structuur van market_data_7d.)

        logger.info(f"ℹ️ 7d helper fetched {len(rows)} rijen (pas aggregaatlogica aan naar wens).")
    except Exception:
        logger.error("❌ Fout in save_market_data_7d()")
        logger.error(traceback.format_exc())
