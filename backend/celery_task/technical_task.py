import logging
import asyncio
from celery import shared_task

from backend.utils.db import get_db_connection
from backend.utils.technical_interpreter import (
    fetch_technical_value,
    interpret_technical_indicator_db,
)
from backend.ai_agents.technical_ai_agent import run_technical_agent

# =====================================================
# 🪵 Logging
# =====================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


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
# 📊 Actieve technische indicatoren (globaal)
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
            """)
            rows = cur.fetchall()

        logger.info(f"📊 {len(rows)} technische indicatoren geladen (globaal)")

        return [
            {"name": r[0], "source": r[1], "link": r[2]}
            for r in rows
        ]

    except Exception:
        logger.exception("❌ Fout bij ophalen technische indicatoren")
        return []

    finally:
        conn.close()


# =====================================================
# 🧠 Technische ingestie
# =====================================================
def fetch_and_process_technical(user_id: int):
    logger.info("========================================")
    logger.info(f"🚀 START technical ingestie (user_id={user_id})")

    indicators = get_active_technical_indicators(user_id)
    logger.info(f"📊 Aantal actieve technical indicators gevonden: {len(indicators)}")

    if not indicators:
        logger.warning(
            f"⚠️ GEEN technische indicatoren gevonden voor user_id={user_id} "
            "(check indicators tabel!)"
        )
        return

    for ind in indicators:
        name = ind["name"]
        logger.info(f"➡️ Verwerk indicator: {name}")

        try:
            logger.info(f"🌐 Ophalen waarde voor {name}")

            result = asyncio.run(
                fetch_technical_value(
                    name,
                    ind.get("source"),
                    ind.get("link")
                )
            )

            if not result:
                logger.warning(f"⚠️ Geen result terug van fetch_technical_value({name})")
                continue

            if "value" not in result:
                logger.warning(f"⚠️ Result zonder 'value' voor {name}: {result}")
                continue

            value = result["value"]
            logger.info(f"📈 {name} waarde opgehaald: {value}")

            interpretation = interpret_technical_indicator_db(
                name,
                value,
                user_id
            )

            if not interpretation:
                logger.warning(
                    f"⚠️ Geen interpretatie/scoreregels voor {name} (user_id={user_id})"
                )
                continue

            logger.info(
                f"🧠 Interpretatie {name}: score={interpretation.get('score')}"
            )

            payload = {
                "indicator": name,
                "value": value,
                "score": interpretation.get("score", 50),
                "advies": interpretation.get("action", "–"),
                "uitleg": interpretation.get("interpretation", "–"),
            }

            logger.info(f"💾 Opslaan {name} voor user_id={user_id}")
            store_technical_score_db(payload, user_id)

        except Exception:
            logger.exception(f"❌ HARD ERROR bij technische indicator {name}")

    logger.info(f"✅ EINDE technical ingestie (user_id={user_id})")
    logger.info("========================================")


# =====================================================
# 🚀 Celery Task — TECHNICAL INGESTIE
# =====================================================
@shared_task(name="backend.celery_task.technical_task.fetch_technical_data_day")
def fetch_technical_data_day(user_id: int):
    if user_id is None:
        raise ValueError("❌ user_id is verplicht voor technical task")

    logger.info(f"📌 Celery technical ingestie gestart (user_id={user_id})")
    fetch_and_process_technical(user_id)


# =====================================================
# 🤖 Celery Task — TECHNICAL AI AGENT
# =====================================================
@shared_task(name="backend.celery_task.technical_task.run_technical_agent_daily")
def run_technical_agent_daily(user_id: int):
    if user_id is None:
        raise ValueError("❌ user_id is verplicht voor technical AI task")

    logger.info(f"🤖 Celery technical AI agent gestart (user_id={user_id})")
    run_technical_agent(user_id=user_id)
