import os
import logging
import traceback
import requests
from datetime import datetime
from celery import shared_task
from tenacity import retry, stop_after_attempt, wait_exponential

# ✅ Eigen utils
from backend.utils.db import get_db_connection
from backend.utils.scoring_utils import generate_scores_db
from backend.utils.technical_interpreter import (
    fetch_technical_value,
    interpret_technical_indicator
)

# === ✅ Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

TIMEOUT = 10
HEADERS = {"Content-Type": "application/json"}


# =====================================================
# 🔁 Retry wrapper
# =====================================================
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


# =====================================================
# 📅 Check of al verwerkt vandaag
# =====================================================
def already_fetched_today(indicator: str) -> bool:
    conn = get_db_connection()
    if not conn:
        return False
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 1 FROM technical_indicators
                WHERE indicator = %s AND DATE(timestamp) = CURRENT_DATE
            """, (indicator,))
            return cur.fetchone() is not None
    except Exception as e:
        logger.error(f"⚠️ Fout bij controleren bestaande technische data: {e}")
        return False
    finally:
        conn.close()


# =====================================================
# 💾 Opslaan score
# =====================================================
def store_technical_score_db(payload: dict):
    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding bij technische opslag.")
        return

    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO technical_indicators (indicator, value, score, advies, uitleg, timestamp)
                VALUES (%s, %s, %s, %s, %s, NOW());
            """, (
                payload.get("indicator"),
                payload.get("value"),
                payload.get("score"),
                payload.get("advies"),
                payload.get("uitleg"),
            ))
        conn.commit()

        logger.info(
            f"💾 Opgeslagen {payload.get('indicator').upper()} — "
            f"waarde={payload.get('value')} | score={payload.get('score')} | advies={payload.get('advies')} | "
            f"tijd={datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}"
        )

    except Exception as e:
        logger.error(f"❌ Fout bij opslaan technische indicator: {e}")
        logger.error(traceback.format_exc())
    finally:
        conn.close()


# =====================================================
# 📊 Indicatoren ophalen uit DB
# =====================================================
def get_active_technical_indicators():
    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding.")
        return []

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT name, source, link
                FROM indicators
                WHERE category = 'technical' AND active = TRUE;
            """)
            rows = cur.fetchall()
            return [{"name": r[0], "source": r[1], "link": r[2]} for r in rows]
    except Exception as e:
        logger.error(f"❌ Fout bij ophalen technische indicatoren: {e}")
        return []
    finally:
        conn.close()


# =====================================================
# 🧠 Hoofdfunctie (SYNC!)
# =====================================================
def fetch_and_process_technical():
    logger.info("🚀 Start technische dataverwerking...")

    indicators = get_active_technical_indicators()
    if not indicators:
        logger.warning("⚠️ Geen technische indicatoren gevonden in DB.")
        return

    for ind in indicators:
        name = ind["name"]
        source = ind.get("source")
        link = ind.get("link")

        logger.info(f"➡️ Verwerk indicator: {name}")

        # ⏩ Dubbele opslag vermijden
        if already_fetched_today(name):
            logger.info(f"⏩ {name} is vandaag al verwerkt, overslaan.")
            continue

        # === ✅ Waarde ophalen (GEEN async!) ===
        try:
            result = fetch_technical_value(name, source, link)
        except Exception as e:
            logger.error(f"❌ Fout bij ophalen waarde voor '{name}': {e}")
            continue

        if not result or "value" not in result:
            logger.warning(f"⚠️ Geen waarde opgehaald voor '{name}'.")
            continue

        value = result["value"]
        logger.info(f"📊 {name.upper()} actuele waarde: {value}")

        # === 📈 Score berekenen ===
        interpretation = interpret_technical_indicator(name, value)

        if not interpretation:
            logger.warning(f"⚠️ Geen scoreregels gevonden voor '{name}'")
            continue

        payload = {
            "indicator": name,
            "value": value,
            "score": interpretation.get("score", 50),
            "advies": interpretation.get("action", "–"),
            "uitleg": interpretation.get("interpretation", "–"),
        }

        # Opslaan
        store_technical_score_db(payload)

    logger.info("✅ Alle technische indicatoren succesvol verwerkt en opgeslagen.")


# =====================================================
# 🚀 Celery-taak
# =====================================================
@shared_task(name="backend.celery_task.technical_task.fetch_technical_data_day")
def fetch_technical_data_day():
    """Dagelijkse Celery-taak voor technische indicatoren."""
    try:
        fetch_and_process_technical()
    except Exception as e:
        logger.error(f"❌ Fout in fetch_technical_data_day(): {e}")
        logger.error(traceback.format_exc())
