from fastapi import APIRouter, HTTPException, Request
from typing import Dict, Any
import logging

router = APIRouter()
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def is_valid_setup(setup: Dict[str, Any]) -> bool:
    """
    Valideert of een setup geldig is: bevat verplichte velden en correcte types.
    """
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

@router.post("/setup")
async def validate_setup_api(request: Request):
    """
    ✅ Valideer één trading setup via AI-validatieregels.
    """
    try:
        setup: Dict[str, Any] = await request.json()

        if not isinstance(setup, dict):
            raise HTTPException(status_code=400, detail="Input moet een JSON-object zijn.")

        result = is_valid_setup(setup)
        logger.info(f"✅ Setup validatie uitgevoerd – resultaat: {result}")
        return {"valid": result}

    except Exception as e:
        logger.error(f"❌ Validatiefout: {e}")
        raise HTTPException(status_code=500, detail="Fout tijdens setup-validatie.")
