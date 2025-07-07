from fastapi import APIRouter, HTTPException, Body
from ai_tasks.validation_task import validate_setups_task
from typing import Dict, Any
import logging

# ✅ Logging instellen
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

router = APIRouter()

# ✅ Interne validator
def is_valid_setup(setup: Dict[str, Any]) -> bool:
    required_fields = ["name", "symbol", "trend", "timeframe", "indicators"]
    for field in required_fields:
        if field not in setup or not setup[field]:
            logger.debug(f"❌ Ongeldige setup – ontbrekend of leeg veld: '{field}'")
            return False

    score_fields = ["macro_score", "technical_score", "sentiment_score"]
    for field in score_fields:
        value = setup.get(field)
        if not isinstance(value, (int, float)):
            logger.debug(f"❌ Ongeldige setup – '{field}' moet een getal zijn. Gevonden: {value}")
            return False

    return True

# ✅ 1. Trigger volledige Celery-validatie
@router.post("/setups/validate")
async def trigger_setup_validation():
    try:
        task = validate_setups_task.delay()
        logger.info("✅ Celery-validatie gestart via /setups/validate")
        return {
            "message": "Setup-validatie gestart.",
            "task_id": task.id
        }
    except Exception as e:
        logger.error(f"❌ Fout bij starten van Celery-validatie: {e}")
        raise HTTPException(status_code=500, detail=f"Fout bij starten van validatie: {e}")

# ✅ 2. Valideer 1 losse setup direct via API
@router.post("/setups/validate/single")
async def validate_single_setup(setup: Dict[str, Any] = Body(...)):
    try:
        result = is_valid_setup(setup)
        logger.info(f"✅ Validatie uitgevoerd voor setup '{setup.get('name', '-')}' → Resultaat: {result}")
        return {
            "valid": result,
            "message": "✅ Geldige setup." if result else "❌ Ongeldige setup – controleer velden en scores."
        }
    except Exception as e:
        logger.error(f"❌ Fout bij single setup-validatie: {e}")
        raise HTTPException(status_code=400, detail=f"Fout bij setup-validatie: {e}")
