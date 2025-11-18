import logging
import traceback
from celery import shared_task

from backend.ai_agents.setup_ai_agent import run_setup_agent  # ✅ juiste import: functie, geen task
from backend.utils.db import get_db_connection

# Logging configureren
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ============================================================
# 🤖 Celery Task — draait dagelijks automatisch
# ============================================================
@shared_task(name="backend.celery_task.setup_task.run_setup_agent_daily")
def run_setup_agent_daily():
    """
    Draait de nieuwe Setup-AI-Agent voor elk uniek asset.
    De agent kiest de beste setup van vandaag en slaat dat op in daily_setup_scores.
    """
    logger.info("🤖 [Setup-Agent Task] Start dagelijkse Setup-Agent run...")

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen databaseverbinding in Setup-Agent Task.")
        return

    try:
        # ----------------------------------------------------
        # Alle unieke assets ophalen
        # ----------------------------------------------------
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT symbol FROM setups WHERE symbol IS NOT NULL")
            assets = [row[0] for row in cur.fetchall()]

        if not assets:
            logger.warning("⚠️ Geen assets gevonden in setups-tabel.")
            return

        # ----------------------------------------------------
        # Per asset Setup-AI-Agent uitvoeren
        # ----------------------------------------------------
        for asset in assets:
            logger.info(f"🔄 Setup-Agent draaien voor asset: {asset}")
            try:
                results = run_setup_agent(asset=asset)

                if not results:
                    logger.warning(f"⚠️ Setup-Agent gaf geen resultaten terug voor {asset}.")
                else:
                    logger.info(
                        f"✅ Setup-Agent succesvol uitgevoerd voor {asset} "
                        f"({len(results)} setups verwerkt)."
                    )

            except Exception as inner:
                logger.error(f"❌ Fout tijdens uitvoeren Setup-Agent voor {asset}: {inner}", exc_info=True)

        logger.info("🎯 Alle Setup-Agent runs voltooid.")

    except Exception:
        logger.error("❌ Algemene fout in setup_task:", exc_info=True)
        logger.error(traceback.format_exc())

    finally:
        try:
            conn.close()
            logger.info("🔒 Databaseverbinding gesloten.")
        except Exception:
            pass
