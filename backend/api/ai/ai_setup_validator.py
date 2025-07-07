from fastapi import APIRouter, HTTPException
from typing import Dict, Any
import logging

router = APIRouter()
logger = logging.getLogger(__name__)

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

@router.post("/validate/setup")
def validate_setup_api(setup: Dict[str, Any]):
    """
    ✅ Valideer setup via API.
    """
    if not isinstance(setup, dict):
        raise HTTPException(status_code=400, detail="Setup moet een JSON object zijn.")
    
    result = is_valid_setup(setup)
    return {"valid": result}
