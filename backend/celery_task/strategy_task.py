import logging
import traceback
from celery import shared_task

from backend.utils.db import get_db_connection
from backend.ai_agents.strategy_ai_agent import generate_strategy_from_setup

# ---------------------------------------------------------
# 🔧 Logging
# ---------------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# =========================================================
# 🔐 INTERNAL DB LOADER (CELERY → GEEN API!)
# =========================================================
def load_setup_from_db(setup_id: int, user_id: int) -> dict:
    """
    Celery mag GEEN API gebruiken (auth/cookies).
    Setup wordt DIRECT uit DB geladen.
    """
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
                    score
                FROM setups
                WHERE id = %s
                  AND user_id = %s
            """, (setup_id, user_id))

            row = cur.fetchone()
            if not row:
                raise ValueError("Setup niet gevonden of niet van gebruiker")

            return {
                "id": row[0],
                "name": row[1],
                "symbol": row[2],
                "timeframe": row[3],
                "strategy_type": row[4],
                "score": row[5],
            }

    finally:
        conn.close()


# =========================================================
# 📦 PAYLOAD BUILDER → /api/strategies
# =========================================================
def build_payload(setup: dict, strategy: dict) -> dict:
    """
    Payload die EXACT matcht met strategy_api.py
    """
    return {
        "setup_id": setup["id"],
        "setup_name": setup["name"],

        # ❗ strategy_api.py accepteert: manual | trading | dca
        # AI-strategie = trading
        "strategy_type": "trading",

        "symbol": setup.get("symbol", "BTC"),
        "timeframe": setup.get("timeframe", "1D"),
        "score": setup.get("score", 0),

        # AI-output
        "ai_explanation": strategy.get("explanation"),
        "risk_reward": strategy.get("risk_reward"),
        "entry": strategy.get("entry"),
        "targets": strategy.get("targets") or [],
        "stop_loss": strategy.get("stop_loss"),
    }


# =========================================================
# 🚀 AI STRATEGY GENERATION (PER SETUP)
# =========================================================
@shared_task(name="backend.celery_task.strategy_task.generate_for_setup")
def generate_for_setup(user_id: int, setup_id: int, overwrite: bool = True):
    """
    Wordt aangeroepen via:
    POST /api/strategies/generate/{setup_id}

    ❗ Celery:
    - GEEN API-auth
    - GEEN cookies
    - GEEN user-routes
    """
    try:
        logger.info(f"🚀 AI strategie genereren | user={user_id} setup={setup_id}")

        # -------------------------------------------------
        # 1️⃣ Setup DIRECT uit DB laden
        # -------------------------------------------------
        setup = load_setup_from_db(setup_id, user_id)
        logger.info(f"📄 Setup geladen: {setup['name']}")

        # -------------------------------------------------
        # 2️⃣ AI strategie genereren
        # -------------------------------------------------
        logger.info("🧠 AI strategy agent starten…")
        strategy = generate_strategy_from_setup(
            setup=setup,
            user_id=user_id,
        )

        if not strategy:
            raise ValueError("AI gaf geen strategie terug")

        # -------------------------------------------------
        # 3️⃣ Opslaan DIRECT in DB via strategy_api helper
        # -------------------------------------------------
        conn = get_db_connection()
        if not conn:
            raise RuntimeError("Geen databaseverbinding bij opslaan")

        try:
            payload = build_payload(setup, strategy)

            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO strategies
                        (setup_id, entry, target, stop_loss, explanation,
                         strategy_type, data, created_at, user_id)
                    VALUES (
                        %(setup_id)s,
                        %(entry)s,
                        %(target)s,
                        %(stop_loss)s,
                        %(ai_explanation)s,
                        %(strategy_type)s,
                        %(data)s::jsonb,
                        NOW(),
                        %(user_id)s
                    )
                    RETURNING id
                """, {
                    "setup_id": payload["setup_id"],
                    "entry": payload.get("entry", ""),
                    "target": ",".join(payload.get("targets", [])),
                    "stop_loss": payload.get("stop_loss", ""),
                    "ai_explanation": payload.get("ai_explanation", ""),
                    "strategy_type": payload["strategy_type"],
                    "data": payload | {"user_id": user_id},
                    "user_id": user_id,
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
