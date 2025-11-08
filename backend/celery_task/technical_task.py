import os
import logging
import traceback
import requests
from datetime import datetime
from celery import shared_task
from tenacity import retry, stop_after_attempt, wait_exponential

# ✅ Eigen utils
from backend.utils.db import get_db_connection
from backend.utils.scoring_utils import generate_scores_db  # leest scoreregels uit DB

# === ✅ Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

TIMEOUT = 10
HEADERS = {"Content-Type": "application/json"}


# ============================================
# 📈 RSI-berekening
# ============================================
def calculate_rsi(closes, period=14):
    """Bereken RSI op basis van slotkoersen."""
    if len(closes) < period + 1:
        return None
    gains, losses = [], []
    for i in range(1, period + 1):
        delta = closes[-i] - closes[-i - 1]
        gains.append(max(delta, 0))
        losses.append(max(-delta, 0))
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return round(100 - (100 / (1 + rs)), 2)


# ============================================
# 🔁 Retry wrapper
# ============================================
@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=5, max=20), reraise=True)
def safe_request(url, params=None):
    """Veilige HTTP request met retries."""
    try:
        resp = requests.get(url, headers=HEADERS, params=params, timeout=TIMEOUT)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.error(f"❌ API-fout bij {url}: {e}")
        raise


# ============================================
# 📊 Indicatoren ophalen
# ============================================
def get_active_technical_indicators():
    """Haal alle actieve technische indicatoren op uit de database."""
    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding.")
        return []

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, name, source, link, symbol
                FROM indicators
                WHERE category = 'technical'
            """)
            rows = cur.fetchall()
            return [{"id": r[0], "name": r[1], "source": r[2], "link": r[3], "symbol": r[4]} for r in rows]
    except Exception as e:
        logger.error(f"❌ Fout bij ophalen technische indicatoren: {e}")
        return []
    finally:
        conn.close()


# ============================================
# 💾 Opslaan score
# ============================================
def store_technical_score_db(symbol, indicator, value, score, trend, interpretation, action, timestamp):
    """Slaat technische indicator-score op in de database."""
    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding")
        return

    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO technical_indicators
                    (symbol, indicator, value, score, advies, uitleg, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (symbol, indicator, value, score, action, interpretation, timestamp))
        conn.commit()
        logger.info(f"✅ Score opgeslagen voor {indicator}")
    except Exception as e:
        logger.error(f"❌ Fout bij DB-opslag {indicator}: {e}")
        logger.error(traceback.format_exc())
    finally:
        conn.close()


# ============================================
# ⚙️ Waarde ophalen per indicator (DB-driven)
# ============================================
def fetch_value_from_source(indicator):
    """
    Haalt ruwe data op via de 'link' en 'source' velden uit de DB.
    Ondersteunt verschillende bronnen zoals Binance, CoinGecko, etc.
    """
    name = indicator["name"]
    source = indicator.get("source", "").lower()
    link = indicator.get("link")
    symbol = indicator.get("symbol", "BTC")

    if not link:
        logger.warning(f"⚠️ Geen API-link voor indicator {name}")
        return None

    try:
        data = safe_request(link)

        # === Binance (candlestick data)
        if "binance" in source:
            closes = [float(k[4]) for k in data]
            volumes = [float(k[5]) for k in data]
            if name.lower() == "rsi":
                return calculate_rsi(closes)
            elif "ma" in name.lower():
                period = int(''.join(filter(str.isdigit, name))) or 200
                return round(sum(closes[-period:]) / period, 2)
            elif "volume" in name.lower():
                return round(sum(volumes[-10:]), 2)
            else:
                logger.warning(f"⚠️ Geen berekening gedefinieerd voor Binance indicator '{name}'")
                return None

        # === CoinGecko (directe prijs- of dominantie-data)
        elif "coingecko" in source:
            if "market_cap_percentage" in data.get("data", {}):
                return float(data["data"]["market_cap_percentage"].get("btc", 0))
            return float(data.get("market_data", {}).get("current_price", {}).get("usd", 0))

        else:
            logger.warning(f"⚠️ Geen fetch-logica voor bron '{source}'")
            return None

    except Exception as e:
        logger.error(f"❌ Fout bij ophalen waarde voor {name}: {e}")
        return None


# ============================================
# 🧠 Hoofdfunctie
# ============================================
def fetch_and_process_technical():
    """Dynamisch ophalen en scoren van technische indicatoren via DB-config."""
    try:
        logger.info("🚀 Start dynamische technische dataverwerking...")

        indicators = get_active_technical_indicators()
        if not indicators:
            logger.warning("⚠️ Geen technische indicatoren gevonden in DB.")
            return

        utc_now = datetime.utcnow().replace(microsecond=0)

        for ind in indicators:
            name = ind["name"]
            logger.info(f"📊 Verwerk indicator: {name}")

            value = fetch_value_from_source(ind)
            if value is None:
                logger.warning(f"⚠️ Geen waarde opgehaald voor {name}")
                continue

            # 📈 Score berekenen vanuit DB-scoreregels
            score_data = generate_scores_db("technical", {name: value})
            if not score_data or "scores" not in score_data:
                logger.warning(f"⚠️ Geen scoreregels gevonden voor {name}")
                continue

            result = score_data["scores"].get(name)
            if not result:
                logger.warning(f"⚠️ Geen resultaat gevonden voor {name}")
                continue

            store_technical_score_db(
                symbol=ind.get("symbol", "BTC"),
                indicator=name,
                value=value,
                score=result.get("score", 10),
                trend=result.get("trend", "–"),
                interpretation=result.get("interpretation", "–"),
                action=result.get("action", "–"),
                timestamp=utc_now
            )

        logger.info("✅ Alle technische indicatoren succesvol verwerkt.")

    except Exception as e:
        logger.error("❌ Fout in fetch_and_process_technical()")
        logger.error(traceback.format_exc())


# ============================================
# 🚀 Celery-taak
# ============================================
@shared_task(name="backend.celery_task.technical_task.fetch_technical_data_day")
def fetch_technical_data_day():
    """Dagelijkse taak: haalt technische data dynamisch op via DB."""
    fetch_and_process_technical()
