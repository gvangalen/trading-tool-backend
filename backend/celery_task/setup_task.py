import logging
import traceback
from celery import shared_task
from backend.utils.db import get_db_connection
from backend.utils.setup_validator import validate_setups  # Check of deze functie daadwerkelijk bestaat op dit pad

# Logging configureren
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@shared_task(name="celery_task.setup_task.validate_setups_task")
def validate_setups_task():
    try:
        logger.info("🔎 Start setup-validatie taak voor alle assets...")

        conn = get_db_connection()
        if not conn:
            logger.error("❌ Geen databaseverbinding bij setup-validatie.")
            return

        with conn.cursor() as cur:
            # Unieke symbolen ophalen uit setups
            cur.execute("SELECT DISTINCT symbol FROM setups WHERE symbol IS NOT NULL")
            assets = [row[0] for row in cur.fetchall()]

        if not assets:
            logger.warning("⚠️ Geen assets gevonden in setups-tabel.")
            return

        for asset in assets:
            logger.info(f"🔄 Valideer setups voor asset: {asset}")
            resultaat = validate_setups(asset=asset)

            if isinstance(resultaat, list) and resultaat:
                logger.info(f"✅ {len(resultaat)} setups gevalideerd voor asset {asset}.")
            elif isinstance(resultaat, list):
                logger.warning(f"⚠️ Geen geldige setups gevonden voor asset {asset}.")
            else:
                logger.error(f"❌ Ongeldig resultaat van validate_setups() voor asset {asset}: {type(resultaat)}")
                logger.debug(f"Inhoud resultaat: {resultaat}")

    except Exception as e:
        logger.error(f"❌ Fout bij setup-validatie: {e}")
        logger.error(traceback.format_exc())
    finally:
        if 'conn' in locals() and conn:
            conn.close()