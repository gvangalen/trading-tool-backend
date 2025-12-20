import logging
import traceback
from celery import shared_task
from tenacity import retry, stop_after_attempt, wait_exponential

from backend.utils.db import get_db_connection
from backend.utils.technical_interpreter import (
    fetch_technical_value,
    interpret_technical_indicator_db
)

# =====================================================
# 🪵 Logging
# =====================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# =====================================================
# 📅 Check of indicator vandaag al verwerkt is
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
                  AND timestamp::date = CURRENT_DATE
            """, (indicator, user_id))
            return cur.fetchone() is not None
    except Exception:
        logger.error("⚠️ Fout bij check technical_indicators", exc_info=True)
        return False
    finally:
        conn.close()


# =====================================================
# 💾 Opslaan technische indicator (user-specifiek)
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
                payload["indicator"],
                payload["value"],
                payload["score"],
                payload.get("advies"),
                payload.get("uitleg"),
            ))
        conn.commit()

        logger.info(
            f"💾 [user={user_id}] {payload['indicator']} "
            f"value={payload['value']} score={payload['score']}"
        )

    except Exception:
        conn.rollback()
        logger.error("❌ Fout bij opslaan technical indicator", exc_info=True)
    finally:
        conn.close()


# =====================================================
# 📊 Actieve technische indicatoren per user
# =====================================================
def get_active_technical_indicators(user_id: int):
    conn = get_db_connection()
    if not conn:
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
            return [
                {"name": r[0], "source": r[1], "link": r[2]}
                for r in cur.fetchall()
            ]
    except Exception:
        logger.error("❌ Fout bij ophalen technische indicatoren", exc_info=True)
        return []
    finally:
        conn.close()


# =====================================================
# 🧠 Hoofdlogica (GEEN Celery)
# =====================================================
def fetch_and_process_technical(user_id: int):
    logger.info(f"🚀 Technische data ingestie gestart (user_id={user_id})")

    indicators = get_active_technical_indicators(user_id)
    if not indicators:
        logger.warning(f"⚠️ Geen technische indicatoren voor user_id={user_id}")
        return

    for ind in indicators:
        name = ind["name"]

        if already_fetched_today(name, user_id):
            logger.info(f"⏩ {name} al verwerkt vandaag (user_id={user_id})")
            continue

        try:
            result = fetch_technical_value(name, ind.get("source"), ind.get("link"))
            if not result or "value" not in result:
                continue

            value = result["value"]
            interpretation = interpret_technical_indicator_db(name, value, user_id)

            if not interpretation:
                logger.warning(f"⚠️ Geen scoreregels voor {name}")
                continue

            payload = {
                "indicator": name,
                "value": value,
                "score": interpretation.get("score", 50),
                "advies": interpretation.get("action", "–"),
                "uitleg": interpretation.get("interpretation", "–"),
            }

            store_technical_score_db(payload, user_id)

        except Exception:
            logger.error(f"❌ Fout bij technische indicator {name}", exc_info=True)

    logger.info(f"✅ Technische ingestie voltooid (user_id={user_id})")


# =====================================================
# 🚀 Celery Task — ALTIJD via dispatcher
# =====================================================
@shared_task(name="backend.celery_task.technical_task.fetch_technical_data_day")
def fetch_technical_data_day(user_id: int):
    if user_id is None:
        raise ValueError("❌ user_id is verplicht voor technical task")

    logger.info(f"📌 Celery technical task gestart (user_id={user_id})")
    fetch_and_process_technical(user_id)
