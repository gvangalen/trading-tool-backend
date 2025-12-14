import logging
import traceback
from celery import shared_task

from backend.utils.db import get_db_connection
from backend.ai_agents.strategy_ai_agent import generate_strategy_from_setup

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# ============================================================
# 🔎 Setup laden uit DB (ZONDER score – die bestaat niet meer)
# ============================================================
def load_setup_from_db(setup_id: int, user_id: int) -> dict:
    conn = get_db_connection()
    if not conn:
        raise RuntimeError("Geen databaseverbinding")

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    id,
                    name,
                    symbol,
                    timeframe,
                    strategy_type,
                    description
                FROM setups
                WHERE id = %s
                  AND user_id = %s
                LIMIT 1;
            """, (setup_id, user_id))

            row = cur.fetchone()
            if not row:
                raise ValueError("Setup niet gevonden")

            return {
                "id": row[0],
                "name": row[1],
                "symbol": row[2],
                "timeframe": row[3],
                "strategy_type": row[4] or "trading",
                "description": row[5],
            }

    finally:
        conn.close()


# ============================================================
# 📦 Payload builder (EXACT wat strategy_api verwacht)
# ============================================================
def build_strategy_payload(setup: dict, strategy: dict) -> dict:
    return {
        "setup_id": setup["id"],
        "setup_name": setup["name"],
        "strategy_type": setup["strategy_type"],
        "symbol": setup["symbol"],
        "timeframe": setup["timeframe"],

        # AI-gegenereerde velden
        "entry": strategy.get("entry"),
        "targets": strategy.get("targets", []),
        "stop_loss": strategy.get("stop_loss"),
        "risk_reward": strategy.get("risk_reward"),
        "ai_explanation": strategy.get("explanation"),
    }


# ============================================================
# 🚀 CELERY TASK — AI STRATEGY GENERATION
# ============================================================
@shared_task(name="backend.celery_task.strategy_task.generate_for_setup")
def generate_for_setup(user_id: int, setup_id: int, overwrite: bool = True):
    logger.info(f"🚀 AI strategie genereren | user={user_id} setup={setup_id}")

    try:
        # 1️⃣ Setup laden (DB)
        setup = load_setup_from_db(setup_id, user_id)
        logger.info(f"📄 Setup geladen: {setup['name']}")

        # 2️⃣ AI strategie genereren
        logger.info("🧠 AI strategy agent starten")
        strategy = generate_strategy_from_setup(setup, user_id=user_id)

        if not strategy:
            raise RuntimeError("AI gaf geen strategie terug")

        # 3️⃣ Strategy opslaan in DB
        conn = get_db_connection()
        if not conn:
            raise RuntimeError("Geen databaseverbinding")

        try:
            payload = build_strategy_payload(setup, strategy)

            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO strategies (
                        user_id,
                        setup_id,
                        setup_name,
                        strategy_type,
                        symbol,
                        timeframe,
                        entry,
                        target,
                        stop_loss,
                        risk_profile,
                        explanation,
                        created_at
                    ) VALUES (
                        %(user_id)s,
                        %(setup_id)s,
                        %(setup_name)s,
                        %(strategy_type)s,
                        %(symbol)s,
                        %(timeframe)s,
                        %(entry)s,
                        %(targets)s,
                        %(stop_loss)s,
                        %(risk_reward)s,
                        %(ai_explanation)s,
                        NOW()
                    )
                    RETURNING id;
                """, {
                    **payload,
                    "user_id": user_id,
                    "targets": ",".join(payload["targets"]) if payload["targets"] else None,
                })

                strategy_id = cur.fetchone()[0]
                conn.commit()

        finally:
            conn.close()

        logger.info(f"✅ AI strategie opgeslagen (id={strategy_id})")

        return {
            "state": "SUCCESS",
            "success": True,
            "strategy_id": strategy_id,
        }

    except Exception as e:
        logger.error("❌ Fout in generate_for_setup")
        logger.error(traceback.format_exc())

        return {
            "state": "FAILURE",
            "success": False,
            "error": str(e),
        }


# =========================================================
# 🔄 BULK GENERATIE (BEWUST UIT)
# =========================================================
@shared_task(name="backend.celery_task.strategy_task.generate_all")
def generate_all(user_id: int):
    return {
        "state": "FAILURE",
        "success": False,
        "error": "Bulk AI strategie-generatie nog niet geactiveerd",
    }
