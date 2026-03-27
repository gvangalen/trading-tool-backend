import logging
from celery import shared_task

from backend.ai_agents.setup_ai_agent import run_setup_agent
from backend.utils.db import get_db_connection

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# ============================================================
# 🤖 Celery Task — Setup Agent (PER USER)
# ============================================================
@shared_task(name="backend.celery_task.setup_task.run_setup_agent_daily")
def run_setup_agent_daily(user_id: int):
    """
    Draait de Setup AI Agent per gebruiker.

    Flow:
    1. Haalt unieke assets uit setups
    2. Roept Setup AI Agent aan per asset

    Output:
    - daily_setup_scores
    - ai_category_insights
    """

    logger.info(f"🤖 [Setup-Task] Start Setup-Agent voor user_id={user_id}")

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen databaseverbinding in Setup-Task")
        return

    try:
        # ----------------------------------------------------
        # 1️⃣ Unieke assets ophalen (clean)
        # ----------------------------------------------------
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT symbol
                FROM setups
                WHERE user_id = %s
                  AND symbol IS NOT NULL
                  AND TRIM(symbol) <> '';
            """, (user_id,))
            rows = cur.fetchall()

        assets = [r[0].strip() for r in rows if r[0]]

    except Exception:
        logger.error("❌ Fout bij ophalen assets in Setup-Task", exc_info=True)
        return

    finally:
        try:
            conn.close()
        except Exception:
            pass

    # ----------------------------------------------------
    # ⚠️ Geen setups → skip
    # ----------------------------------------------------
    if not assets:
        logger.info(f"ℹ️ Geen setups/assets gevonden voor user_id={user_id}")
        return

    logger.info(f"📊 [Setup-Task] Assets gevonden: {assets}")

    # ----------------------------------------------------
    # 2️⃣ Per asset Setup AI Agent draaien
    # ----------------------------------------------------
    for asset in assets:

        logger.info(
            f"🔄 [Setup-Task] Run Setup-Agent user_id={user_id}, asset={asset}"
        )

        try:
            run_setup_agent(user_id=user_id, asset=asset)

            logger.info(
                f"✅ [Setup-Task] Setup-Agent voltooid user_id={user_id}, asset={asset}"
            )

        except Exception:
            logger.error(
                f"❌ [Setup-Task] Fout in Setup-Agent user_id={user_id}, asset={asset}",
                exc_info=True,
            )

    logger.info(f"🎯 [Setup-Task] Alle Setup-Agent runs voltooid user_id={user_id}")
