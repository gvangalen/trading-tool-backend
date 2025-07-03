import logging
import traceback
from celery import shared_task
from utils.setup_validator import validate_setups

# ✅ Logging instellen
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ✅ Setup validatie taak
@shared_task(name="setup.validate_setups_task")
def validate_setups_task():
    try:
        logger.info("🔎 Start setup-validatie taak...")
        resultaat = validate_setups(asset="BTC")
        if resultaat:
            logger.info(f"✅ {len(resultaat)} setups gevalideerd.")
        else:
            logger.warning("⚠️ Geen geldige setups gevonden.")
    except Exception as e:
        logger.error(f"❌ Fout bij setup-validatie: {e}")
        logger.error(traceback.format_exc())
