import logging
from backend.config.config_loader import load_technical_config
from backend.utils.scoring_utils import calculate_score_from_config  # 👈 Nieuwe scoringmethode

# ✅ Logging instellen
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def extract_nested_value(data, key_path):
    """
    ➤ Haalt een geneste waarde uit een dict op basis van pad zoals 'data.0.value'
    """
    try:
        keys = key_path.split(".")
        for key in keys:
            if isinstance(data, list) and key.isdigit():
                data = data[int(key)]
            elif isinstance(data, dict):
                data = data.get(key)
            else:
                return None
        return float(data)
    except (TypeError, ValueError) as e:
        logger.warning(f"⚠️ Ongeldige waarde bij extractie '{key_path}': {e}")
        return None


def process_technical_indicator(name, value, config):
    """
    ➤ Verwerkt één technische indicator volgens de config + scoringsutils
    Returns:
        dict met name, value, trend, interpretation, score, explanation, action
    """
    try:
        if value is None:
            raise ValueError("Waarde is None")

        score_data = calculate_score_from_config(value, config)
        if not score_data:
            raise ValueError("Scoredata ontbreekt")

        result = {
            "name": name,
            "value": value,
            "score": score_data["score"],
            "trend": score_data["trend"],
            "interpretation": score_data["interpretation"],
            "action": score_data["action"],
            "explanation": config.get("explanation"),
        }

        logger.info(f"✅ {name}: {value} → {result['trend']} (score: {result['score']})")
        return result

    except Exception as e:
        logger.error(f"❌ Fout bij verwerken technische indicator '{name}': {e}")
        return None


def process_all_technical(data: dict):
    """
    ➤ Verwerkt alle technische indicatoren met bijbehorende config.
    Verwacht dict zoals: {"rsi": 44.1, "volume": 380000000, "ma_200": 0.94}
    Returns:
        {"rsi": {score, uitleg, ...}, "volume": {..}, ...}
    """
    try:
        config = load_technical_config()
        indicators = config.get("indicators", {})
    except Exception as e:
        logger.error(f"❌ Config laden mislukt: {e}")
        return {}

    results = {}
    for name, raw_value in data.items():
        if name not in indicators:
            logger.warning(f"⚠️ Geen interpretatieconfig voor: {name}")
            continue

        result = process_technical_indicator(name, raw_value, indicators[name])
        if result:
            results[name] = result

    return results
