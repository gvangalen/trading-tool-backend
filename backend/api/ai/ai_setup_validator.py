import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

def is_valid_setup(setup: Dict[str, Any]) -> bool:
    """
    Valideert of een setup geschikt is voor AI-verwerking.
    
    ✅ Vereiste velden: name, symbol, trend, timeframe, indicators
    ✅ Vereiste scores: macro_score, technical_score, sentiment_score (moet numeriek zijn)
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
