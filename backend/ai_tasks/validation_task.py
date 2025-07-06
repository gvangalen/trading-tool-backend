from celery import shared_task
import logging
import traceback
import json
from setup_validator import validate_setups

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

@shared_task(name="ai_tasks.validate_setups_task")
def validate_setups_task():
    logger.info("🧠 Start setup-validatie (AI)")
    try:
        results = validate_setups()

        if not results:
            logger.warning("⚠️ Geen setups gevalideerd of lege lijst ontvangen")
            return

        # Optioneel: resultaten opslaan als JSON voor inspectie/debugging
        try:
            with open("validated_setups.json", "w") as f:
                json.dump(results, f, indent=2)
            logger.info(f"✅ {len(results)} setups gevalideerd en opgeslagen naar validated_setups.json")
        except Exception as write_err:
            logger.warning(f"⚠️ Kon resultaten niet opslaan als JSON: {write_err}")

    except Exception as e:
        logger.error(f"❌ Fout in validate_setups_task: {e}")
        logger.error(traceback.format_exc())
