import os
import logging
import traceback
from datetime import datetime
from celery import shared_task
from tenacity import retry, stop_after_attempt, wait_exponential
import requests

# ✅ Eigen utils
from backend.utils.db import get_db_connection
from backend.utils.scoring_utils import generate_scores_db  # nieuwe versie die uit DB leest

# ✅ Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ✅ Config
BINANCE_BASE_URL = "https://api.binance.com"
TIMEOUT = 10
HEADERS = {"Content-Type": "application/json"}


# ============================================
# 🔁 Helper functies
# ============================================

@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=5, max=20), reraise=True)
def safe_request(url, method="GET", payload=None, headers=None):
    """Veilige HTTP request met retries."""
    try:
        response = requests.request(
            method, url,
            json=payload if method.upper() == "POST" else None,
            params=payload if method.upper() == "GET" else None,
            headers=headers or HEADERS,
            timeout=TIMEOUT
        )
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"❌ API-fout bij {url}: {e}")
        raise


def get_active_indicators():
    """Haal alle actieve technische indicatoren op uit de database."""
    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding.")
        return []

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, name, source, symbol
                FROM indicators
                WHERE category = 'technical'
            """)
            rows = cur.fetchall()
            return [{"id": r[0], "name": r[1], "source": r[2], "symbol": r[3]} for r in rows]
    except Exception as e:
        logger.error(f"❌ Fout bij ophalen actieve indicatoren: {e}")
        return []
    finally:
        conn.close()


def store_technical_score_db(symbol, indicator, value, score, trend, interpretation, action, timestamp):
    """Slaat of updatet technische indicator-score per dag."""
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
                ON CONFLICT (indicator, date_ts) DO UPDATE
                SET value = EXCLUDED.value,
                    score = EXCLUDED.score,
                    advies = EXCLUDED.advies,
                    uitleg = EXCLUDED.uitleg,
                    timestamp = EXCLUDED.timestamp
            """, (symbol, indicator, value, score, action, interpretation, timestamp))
        conn.commit()
        logger.info(f"✅ Dagrecord opgeslagen voor {indicator}")
    except Exception as e:
        logger.error(f"❌ Fout bij DB-opslag {indicator}: {e}")
        logger.error(traceback.format_exc())
    finally:
        conn.close()


# ============================================
# 🧠 Hoofdfunctie: dynamische verwerking
# ============================================

def fetch_and_post_dynamic(symbol="BTCUSDT", interval="1d", limit=300):
    """
    Dynamisch ophalen en scoren van alle technische indicatoren
    op basis van de databaseconfiguratie.
    """
    try:
        logger.info("🚀 Start dynamische technische dataverwerking...")

        # 🔍 Haal actieve indicatoren op uit de database
        active_indicators = get_active_indicators()
        if not active_indicators:
            logger.warning("⚠️ Geen actieve indicatoren gevonden in DB.")
            return

        utc_now = datetime.utcnow().replace(microsecond=0)

        for item in active_indicators:
            name = item["name"]
            source = item["source"]
            our_symbol = item["symbol"] or "BTC"

            logger.info(f"📊 Verwerk indicator: {name} (bron: {source})")

            # 🔗 Bouw API-call (nu standaard Binance, later uitbreidbaar)
            if source == "binance":
                url = f"{BINANCE_BASE_URL}/api/v3/klines"
                params = {"symbol": symbol, "interval": interval, "limit": limit}
                data = safe_request(url, payload=params)

                closes = [float(k[4]) for k in data]
                volumes = [float(k[5]) for k in data]
                value = None

                # 🧮 Bereken waarde per indicator dynamisch
                if name.lower() == "rsi":
                    value = calculate_rsi(closes)
                elif name.lower() == "ma200":
                    value = round(sum(closes[-200:]) / 200, 2)
                elif name.lower() == "volume":
                    value = round(sum(volumes[-10:]), 2)
                else:
                    logger.info(f"⏭️ Geen berekeningslogica gedefinieerd voor {name}, overslaan.")
                    continue

                # 📈 Genereer score vanuit scoreregels in DB
                score_obj = generate_scores_db(name, value)
                if not score_obj:
                    logger.warning(f"⚠️ Geen scoreregels gevonden voor {name}")
                    continue

                store_technical_score_db(
                    symbol=our_symbol,
                    indicator=name,
                    value=value,
                    score=score_obj.get("score"),
                    trend=score_obj.get("trend"),
                    interpretation=score_obj.get("interpretation"),
                    action=score_obj.get("action"),
                    timestamp=utc_now
                )

            else:
                logger.warning(f"⚠️ Onbekende source '{source}' voor indicator {name}")

    except Exception as e:
        logger.error("❌ Fout in fetch_and_post_dynamic()")
        logger.error(traceback.format_exc())


# ============================================
# 🚀 Celery taak
# ============================================

@shared_task(name="backend.celery_task.technical_task.fetch_technical_data_day")
def fetch_technical_data_day():
    """Dagelijkse taak: haalt technische data dynamisch op uit DB-config."""
    fetch_and_post_dynamic()
