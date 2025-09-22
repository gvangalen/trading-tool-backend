import logging
from backend.config.config_loader import load_technical_config  # ✅ Vaste loader voor technische config

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


def interpret_value(value, thresholds, positive=True):
    """
    ➤ Geeft interpretatie ('Zwak', 'Neutraal', 'Sterk', 'Zeer sterk') op basis van thresholds.
    """
    if value is None:
        return "Ongeldig"

    try:
        v = float(value)
    except ValueError:
        logger.warning(f"⚠️ Ongeldige numerieke waarde: {value}")
        return "Ongeldig"

    if positive:
        if v >= thresholds[2]:
            return "Zeer sterk"
        elif v >= thresholds[1]:
            return "Sterk"
        elif v >= thresholds[0]:
            return "Neutraal"
        else:
            return "Zwak"
    else:
        if v <= thresholds[0]:
            return "Zeer sterk"
        elif v <= thresholds[1]:
            return "Sterk"
        elif v <= thresholds[2]:
            return "Neutraal"
        else:
            return "Zwak"


def calculate_score(value, thresholds, positive=True):
    """
    ➤ Genereert een score op basis van thresholds (0–3)
    """
    if value is None:
        return 0

    try:
        v = float(value)
    except ValueError:
        return 0

    if positive:
        if v >= thresholds[2]:
            return 3
        elif v >= thresholds[1]:
            return 2
        elif v >= thresholds[0]:
            return 1
        else:
            return 0
    else:
        if v <= thresholds[0]:
            return 3
        elif v <= thresholds[1]:
            return 2
        elif v <= thresholds[2]:
            return 1
        else:
            return 0


def process_technical_indicator(name, value, config):
    """
    ➤ Verwerkt één technische indicator volgens de config.
    Returns:
        dict met name, value, interpretation, score, etc.
    """
    try:
        thresholds = config.get("thresholds", [])
        positive = config.get("positive", True)

        if not thresholds or len(thresholds) != 3:
            logger.error(f"❌ Ongeldige thresholds-configuratie voor '{name}': {thresholds}")
            return None

        interpretation = interpret_value(value, thresholds, positive)
        score = calculate_score(value, thresholds, positive)

        result = {
            "name": name,
            "value": value,
            "interpretation": interpretation,
            "score": score,
            "correlation": config.get("correlation"),
            "explanation": config.get("explanation"),
            "action": config.get("action"),
            "category": config.get("category", "technical")
        }

        logger.info(f"✅ {name}: {value} → {interpretation} (score: {score})")
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
    except Exception as e:
        logger.error(f"❌ Config laden mislukt: {e}")
        return {}

    results = {}
    for name, raw_value in data.items():
        if name not in config.get("indicators", {}):
            logger.warning(f"⚠️ Geen interpretatieconfig voor: {name}")
            continue

        result = process_technical_indicator(name, raw_value, config["indicators"][name])
        if result:
            results[name] = result

    return results
