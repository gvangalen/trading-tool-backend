import logging
import traceback
from datetime import date
from celery import shared_task

# Nieuwe AI Setup Agent importeren
from backend.ai_agents.setup_ai_agent import run_setup_agent_task
from backend.utils.db import get_db_connection

# Logging configureren
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ============================================================
# 🤖 Celery Task — Draait dagelijks automatisch
# ============================================================
@shared_task(name="backend.celery_task.setup_task.run_setup_agent_daily")
def run_setup_agent_daily():
    """
    Draait de nieuwe Setup-AI-Agent voor elk uniek asset.
    De agent kiest de beste setup van vandaag en slaat die op
    in daily_setup_scores.
    """
    logger.info("🤖 [Setup-Agent Task] Start dagelijkse Setup-Agent run...")

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen databaseverbinding in Setup-Agent Task.")
        return

    try:
        # ----------------------------------------------------
        # Unieke assets ophalen
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
                results = run_setup_agent_task(asset)

                if not results:
                    logger.warning(f"⚠️ Geen resultaten terug van Setup-Agent voor: {asset}")
                else:
                    logger.info(
                        f"✅ Setup-Agent uitgevoerd voor {asset} – "
                        f"{len(results)} setups verwerkt"
                    )

            except Exception:
                logger.error(
                    f"❌ Fout tijdens uitvoeren Setup-Agent voor asset: {asset}",
                    exc_info=True
                )

        logger.info("🎯 Alle Setup-Agent runs voltooid.")

    except Exception:
        logger.error("❌ Algemene fout in setup_task:", exc_info=True)

    finally:
        try:
            conn.close()
            logger.info("🔒 Databaseverbinding gesloten.")
        except:
            pass
