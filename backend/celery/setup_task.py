from celery import Celery
import os
import logging
import traceback
from utils.setup_validator import validate_setups

# ✅ Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ✅ Celery initialisatie
celery = Celery(
    "setup_task",
    broker=os.getenv("CELERY_BROKER_URL", "redis://redis:6379/0"),
    backend=os.getenv("CELERY_RESULT_BACKEND", "redis://redis:6379/0"),
)
celery.conf.timezone = "UTC"
celery.conf.enable_utc = True

# ✅ Setup validatie taak
@celery.task(name="celery_worker.validate_setups_task")
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
