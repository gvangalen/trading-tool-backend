from celery import shared_task
import logging
import traceback
import json
from setup_validator import validate_setups

logger = logging.getLogger(__name__)

@shared_task(name="ai_tasks.validate_setups_task")
def validate_setups_task():
    logger.info("🧠 Start setup-validatie (AI)")
    try:
        results = validate_setups()
        with open("validated_setups.json", "w") as f:
            json.dump(results, f, indent=2)
        logger.info(f"✅ {len(results)} setups gevalideerd en opgeslagen")
    except Exception as e:
        logger.error(f"❌ Fout in validate_setups_task: {e}")
        logger.error(traceback.format_exc())
