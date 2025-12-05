import os
import logging
import traceback
import requests
from datetime import datetime
from celery import shared_task
from tenacity import retry, stop_after_attempt, wait_exponential

# Eigen utils
from backend.utils.db import get_db_connection
from backend.utils.technical_interpreter import (
    fetch_technical_value,
    interpret_technical_indicator_db  # ← nieuwe versie die user_id ondersteunt
)

# === Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

TIMEOUT = 10
HEADERS = {"Content-Type": "application/json"}


# =====================================================
# 🔁 Retry wrapper
# =====================================================
@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=5, max=20), reraise=True)
def safe_request(url, params=None):
    try:
        resp = requests.get(url, headers=HEADERS, params=params, timeout=TIMEOUT)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.error(f"❌ API-fout bij {url}: {e}")
        raise


# =====================================================
# 📅 Check per user of indicator al verwerkt is
# =====================================================
def already_fetched_today(indicator: str, user_id: int) -> bool:
    conn = get_db_connection()
    if not conn:
        return False

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 1 
                FROM technical_indicators
                WHERE indicator = %s
                  AND user_id = %s
                  AND DATE(timestamp) = CURRENT_DATE
            """, (indicator, user_id))
            return cur.fetchone() is not None

    except Exception as e:
        logger.error(f"⚠️ Fout bij controleren bestaande technical data: {e}")
        return False

    finally:
        conn.close()


# =====================================================
# 💾 Opslaan score per gebruiker
# =====================================================
def store_technical_score_db(payload: dict, user_id: int):
    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding bij technische opslag.")
        return

    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO technical_indicators 
                    (user_id, indicator, value, score, advies, uitleg, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, NOW())
            """, (
                user_id,
                payload.get("indicator"),
                payload.get("value"),
                payload.get("score"),
                payload.get("advies"),
                payload.get("uitleg"),
            ))
        conn.commit()

        logger.info(
            f"💾 [user={user_id}] opgeslagen {payload.get('indicator').upper()} "
            f"value={payload.get('value')} | score={payload.get('score')} | advies={payload.get('advies')}"
        )

    except Exception as e:
        logger.error(f"❌ Fout bij opslaan technical indicator: {e}")
        logger.error(traceback.format_exc())
    finally:
        conn.close()


# =====================================================
# 📊 Technische indicatoren ophalen per user
# =====================================================
def get_active_technical_indicators(user_id: int):
    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding.")
        return []

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT name, source, link
                FROM indicators
                WHERE category = 'technical'
                  AND active = TRUE
                  AND user_id = %s
            """, (user_id,))
            rows = cur.fetchall()

        return [{"name": r[0], "source": r[1], "link": r[2]} for r in rows]

    except Exception as e:
        logger.error(f"❌ Fout bij ophalen technische indicatoren: {e}")
        return []

    finally:
        conn.close()


# =====================================================
# 🧠 Hoofdfunctie
# =====================================================
def fetch_and_process_technical(user_id: int):
    logger.info(f"🚀 Start technische dataverwerking voor user_id={user_id}...")

    indicators = get_active_technical_indicators(user_id)
    if not indicators:
        logger.warning(f"⚠️ Geen technische indicatoren gevonden in DB voor user {user_id}.")
        return

    for ind in indicators:
        name = ind["name"]
        source = ind.get("source")
        link = ind.get("link")

        logger.info(f"➡️ Verwerk indicator '{name}' voor user={user_id}")

        # Al gedaan vandaag?
        if already_fetched_today(name, user_id):
            logger.info(f"⏩ '{name}' is vandaag al verwerkt voor user={user_id}.")
            continue

        # Waarde ophalen
        try:
            result = fetch_technical_value(name, source, link)
        except Exception as e:
            logger.error(f"❌ Fout bij ophalen waarde voor '{name}': {e}")
            continue

        if not result or "value" not in result:
            logger.warning(f"⚠️ Geen waarde opgehaald voor '{name}'.")
            continue

        value = result["value"]
        logger.info(f"📊 {name.upper()} waarde={value}")

        # Interpretatie + score (per user, via DB-rules)
        interpretation = interpret_technical_indicator_db(name, value, user_id)

        if not interpretation:
            logger.warning(f"⚠️ Geen scoreregels gevonden voor '{name}' (user_id={user_id})")
            continue

        payload = {
            "indicator": name,
            "value": value,
            "score": interpretation.get("score", 50),
            "advies": interpretation.get("action", "–"),
            "uitleg": interpretation.get("interpretation", "–"),
        }

        store_technical_score_db(payload, user_id)

    logger.info(f"✅ Alle technische indicatoren verwerkt voor user_id={user_id}.")


# =====================================================
# 🚀 Celery Task
# =====================================================
@shared_task(name="backend.celery_task.technical_task.fetch_technical_data_day")
def fetch_technical_data_day(user_id: int = 1):
    """
    Dagelijkse technische indicator taak.
    Je kunt meerdere users schedulen:
    - fetch_technical_data_day.apply_async(kwargs={"user_id": 1})
    - fetch_technical_data_day.apply_async(kwargs={"user_id": 2})
    etc.
    """
    logger.info(f"📌 Celery technical task gestart voor user_id={user_id}")
    try:
        fetch_and_process_technical(user_id)
    except Exception as e:
        logger.error(f"❌ Fout in fetch_technical_data_day(): {e}")
        logger.error(traceback.format_exc())
