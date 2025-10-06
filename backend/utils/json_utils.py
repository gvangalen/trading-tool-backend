import json
import logging

logger = logging.getLogger(__name__)

def sanitize_json_input(data, context=""):
    """
    Zorgt ervoor dat een AI-antwoord of json-achtig veld altijd een dict is.
    - Als het al een dict is, retourneer gewoon.
    - Als het een string is, probeer te parsen als JSON.
    - Log en retourneer fallback bij mislukking.
    """
    if isinstance(data, dict):
        return data
    if isinstance(data, str):
        try:
            return json.loads(data)
        except Exception as e:
            logger.warning(f"[sanitize_json_input] ❌ String kon niet als JSON worden geparsed in context '{context}': {e}")
            return {"error": f"Invalid JSON in {context}", "raw": data}
    logger.warning(f"[sanitize_json_input] ❌ Onverwacht datatype ({type(data)}) in context '{context}'")
    return {"error": f"Unexpected type in {context}", "raw": str(data)}
