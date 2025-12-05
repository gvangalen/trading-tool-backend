import logging
import traceback
from celery import shared_task

from backend.ai_agents.setup_ai_agent import run_setup_agent  # ✅ functie, geen task
from backend.utils.db import get_db_connection

# Logging configureren
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ============================================================
# 🤖 Celery Task — draait dagelijks per user_id
# ============================================================
@shared_task(name="backend.celery_task.setup_task.run_setup_agent_daily")
def run_setup_agent_daily(user_id: int):
    """
    Draait de Setup-AI-Agent per gebruiker voor elk uniek asset.
    De agent kiest de beste setup van vandaag en slaat die op in daily_setup_scores
    (user-specifiek).
    """
    logger.info(f"🤖 [Setup-Agent Task] Start dagelijkse Setup-Agent run voor user_id={user_id}...")

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen databaseverbinding in Setup-Agent Task.")
        return

    try:
        # ----------------------------------------------------
        # Alle unieke assets voor deze user ophalen
        # ----------------------------------------------------
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT symbol
                FROM setups
                WHERE symbol IS NOT NULL
                  AND user_id = %s
                """,
                (user_id,),
            )
            assets = [row[0] for row in cur.fetchall()]

        if not assets:
            logger.warning(f"⚠️ Geen assets gevonden in setups-tabel voor user_id={user_id}.")
            return

        # ----------------------------------------------------
        # Per asset Setup-AI-Agent uitvoeren (user-specifiek)
        # ----------------------------------------------------
        for asset in assets:
            logger.info(f"🔄 Setup-Agent draaien voor user_id={user_id}, asset={asset}")
            try:
                results = run_setup_agent(user_id=user_id, asset=asset)

                if not results:
                    logger.warning(
                        f"⚠️ Setup-Agent gaf geen resultaten terug voor user_id={user_id}, asset={asset}."
                    )
                else:
                    all_setups = results.get("all_setups", [])
                    logger.info(
                        f"✅ Setup-Agent succesvol uitgevoerd voor user_id={user_id}, asset={asset} "
                        f"({len(all_setups)} setups verwerkt)."
                    )

            except Exception as inner:
                logger.error(
                    f"❌ Fout tijdens uitvoeren Setup-Agent voor user_id={user_id}, asset={asset}: {inner}",
                    exc_info=True,
                )

        logger.info(f"🎯 Alle Setup-Agent runs voltooid voor user_id={user_id}.")

    except Exception:
        logger.error("❌ Algemene fout in setup_task:", exc_info=True)
        logger.error(traceback.format_exc())

    finally:
        try:
            conn.close()
            logger.info("🔒 Databaseverbinding gesloten.")
        except Exception:
            pass
