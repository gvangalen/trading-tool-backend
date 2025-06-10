import logging

logger = logging.getLogger(__name__)

def is_valid_setup(setup: dict) -> bool:
    """
    Controleert of een setup alle vereiste velden bevat om AI-logica op toe te passen.
    """

    required_fields = ["name", "symbol", "trend", "timeframe", "indicators"]
    for field in required_fields:
        if not setup.get(field):
            logger.debug(f"❌ Setup ongeldig: ontbrekend veld '{field}'")
            return False

    for score_field in ["macro_score", "technical_score", "sentiment_score"]:
        value = setup.get(score_field)
        if not isinstance(value, (int, float)):
            logger.debug(f"❌ Setup ongeldig: '{score_field}' is geen getal ({value})")
            return False

    return True
