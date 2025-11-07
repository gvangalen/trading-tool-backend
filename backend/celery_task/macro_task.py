import os
import logging
import traceback
import requests
from datetime import datetime
from tenacity import retry, stop_after_attempt, wait_exponential
from celery import shared_task

# ✅ Utils
from backend.utils.db import get_db_connection
from backend.utils.scoring_utils import generate_scores_db  # nieuwe centrale DB-scorefunctie
from backend.utils.macro_interpreter import fetch_macro_value  # aangepaste async/HTTP helper

# === ✅ Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# === ✅ Config
TIMEOUT = 10
HEADERS = {"Content-Type": "application/json"}


# =====================================================
# 🔁 Helper functies
# =====================================================
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


def get_active_macro_indicators():
    """Haalt alle actieve macro-indicatoren op uit de database."""
    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding.")
        return []

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, name, source, symbol, link
                FROM indicators
                WHERE category = 'macro'
            """)
            rows = cur.fetchall()
            return [{"id": r[0], "name": r[1], "source": r[2], "symbol": r[3], "link": r[4]} for r in rows]
    except Exception as e:
        logger.error(f"❌ Fout bij ophalen macro-indicatoren: {e}")
        return []
    finally:
        conn.close()


def already_fetched_today(indicator_name: str) -> bool:
    """Controleert of indicator vandaag al is opgeslagen in de database."""
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id FROM macro_data
                WHERE name = %s AND DATE(timestamp) = CURRENT_DATE
            """, (indicator_name,))
            return cur.fetchone() is not None
    except Exception as e:
        logger.error(f"⚠️ Fout bij controleren bestaande macro-data: {e}")
        return False
    finally:
        conn.close()


def store_macro_score_db(payload: dict):
    """Slaat de macro-score direct op in de database."""
    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding bij macro-data opslag.")
        return
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO macro_data 
                    (name, value, score, trend, interpretation, action, symbol, source, link, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                payload.get("name"),
                payload.get("value"),
                payload.get("score"),
                payload.get("trend"),
                payload.get("interpretation"),
                payload.get("action"),
                payload.get("symbol", "BTC"),
                payload.get("source"),
                payload.get("link"),
                datetime.utcnow().replace(microsecond=0)
            ))
        conn.commit()
        logger.info(f"💾 Macro-score opgeslagen voor {payload.get('name')}")
    except Exception as e:
        logger.error(f"❌ Fout bij opslaan macro-score: {e}")
        logger.error(traceback.format_exc())
    finally:
        conn.close()


# =====================================================
# 🧠 Hoofdfunctie: dynamische macroverwerking
# =====================================================
def fetch_and_process_macro():
    """Haal alle macro-indicatoren op en sla scores op via DB-config."""
    logger.info("🚀 Start dynamische macro-verwerking...")

    indicators = get_active_macro_indicators()
    if not indicators:
        logger.warning("⚠️ Geen macro-indicatoren gevonden in DB.")
        return

    for item in indicators:
        name = item["name"]
        source = item["source"]
        symbol = item.get("symbol", "BTC")
        link = item.get("link", "")

        if already_fetched_today(name):
            logger.info(f"⏩ {name} is vandaag al opgehaald, overslaan.")
            continue

        logger.info(f"➡️ Verwerk macro-indicator: {name} (bron: {source})")

        try:
            # 🔄 Ophalen actuele waarde
            value = fetch_macro_value(name, source, link)
            if value is None:
                logger.warning(f"⚠️ Geen waarde opgehaald voor {name}")
                continue

            # 🧮 Score berekenen via scoreregels uit DB
            score_info = generate_scores_db(name, value)
            if not score_info:
                logger.warning(f"⚠️ Geen scoreregels gevonden voor {name}")
                continue

            # 💾 Opslaan
            payload = {
                "name": name,
                "value": value,
                "score": score_info.get("score", 10),
                "trend": score_info.get("trend", "–"),
                "interpretation": score_info.get("interpretation", "–"),
                "action": score_info.get("action", "–"),
                "symbol": symbol,
                "source": source,
                "link": link,
            }
            store_macro_score_db(payload)

        except Exception as e:
            logger.error(f"❌ Fout bij verwerken van {name}: {e}")
            logger.error(traceback.format_exc())

    logger.info("✅ Alle macro-indicatoren succesvol verwerkt.")


# =====================================================
# 🚀 Celery taak
# =====================================================
@shared_task(name="backend.celery_task.macro_task.fetch_macro_data")
def fetch_macro_data():
    """Dagelijkse taak: haalt macrodata op uit DB en verwerkt via scoreregels."""
    fetch_and_process_macro()
