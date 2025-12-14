import logging
import traceback
import json
from celery import shared_task

from backend.utils.db import get_db_connection
from backend.ai_agents.strategy_ai_agent import generate_strategy_from_setup

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# ============================================================
# 🔹 Load setup (STRICT volgens DB schema)
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
                    description,
                    filters
                FROM setups
                WHERE id = %s AND user_id = %s
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
                "strategy_type": row[4],
                "description": row[5],
                "filters": row[6],
            }

    finally:
        conn.close()


# ============================================================
# 🚀 Generate strategy (CELERY TASK)
# ============================================================
@shared_task(name="backend.celery_task.strategy_task.generate_for_setup")
def generate_for_setup(user_id: int, setup_id: int):
    logger.info(f"🚀 AI strategie genereren | user={user_id} setup={setup_id}")

    conn = None

    try:
        # --------------------------------------------------
        # 1️⃣ Setup laden
        # --------------------------------------------------
        setup = load_setup_from_db(setup_id, user_id)
        logger.info(f"📄 Setup geladen: {setup['name']}")

        # --------------------------------------------------
        # 2️⃣ AI strategie genereren
        # --------------------------------------------------
        strategy = generate_strategy_from_setup(setup, user_id=user_id)

        if not isinstance(strategy, dict):
            raise ValueError("AI gaf geen geldige strategie terug")

        # --------------------------------------------------
        # 3️⃣ Opslaan in strategies (JSONB = json.dumps)
        # --------------------------------------------------
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO strategies (
                    setup_id,
                    entry,
                    target,
                    stop_loss,
                    explanation,
                    risk_profile,
                    strategy_type,
                    data,
                    user_id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
                RETURNING id;
            """, (
                setup_id,
                strategy.get("entry"),
                ",".join(map(str, strategy.get("targets", []))),
                strategy.get("stop_loss"),
                strategy.get("explanation"),
                strategy.get("risk_reward"),      # ← kolom heet risk_profile
                setup.get("strategy_type"),
                json.dumps(strategy),             # ✅ DIT WAS DE BUG
                user_id,
            ))

            strategy_id = cur.fetchone()[0]
            conn.commit()

        logger.info(f"✅ Strategie opgeslagen (id={strategy_id})")

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

    finally:
        if conn:
            conn.close()


# ============================================================
# 🔄 BULK GENERATIE (UIT)
# ============================================================
@shared_task(name="backend.celery_task.strategy_task.generate_all")
def generate_all(user_id: int):
    return {
        "state": "FAILURE",
        "success": False,
        "error": "Bulk AI strategie-generatie nog niet geactiveerd",
    }
