import logging

# ✅ Logging instellen
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def extract_nested_value(data, key_path):
    """Haalt een geneste waarde uit een dict op basis van een pad zoals 'rsi' of 'volume'."""
    keys = key_path.split(".")
    for key in keys:
        if isinstance(data, list) and key.isdigit():
            data = data[int(key)]
        elif isinstance(data, dict):
            data = data.get(key, {})
        else:
            return None
    return data if isinstance(data, (int, float, str)) else None

def interpret_value(value, thresholds, positive=True):
    """Geeft interpretatie en actie terug op basis van thresholds en positive/negative trend."""
    try:
        numeric_value = float(value)
    except ValueError:
        logger.warning(f"⚠️ Ongeldige numerieke waarde: {value}")
        return None

    if positive:
        if numeric_value >= thresholds[2]:
            return "Zeer sterk"
        elif numeric_value >= thresholds[1]:
            return "Sterk"
        elif numeric_value >= thresholds[0]:
            return "Neutraal"
        else:
            return "Zwak"
    else:
        if numeric_value <= thresholds[0]:
            return "Zeer sterk"
        elif numeric_value <= thresholds[1]:
            return "Sterk"
        elif numeric_value <= thresholds[2]:
            return "Neutraal"
        else:
            return "Zwak"

def process_technical_indicator(name, value, config):
    """
    Verwerkt één technische indicator volgens config.
    Returns:
        dict met naam, waarde, interpretatie
    """
    try:
        thresholds = config.get("thresholds", [])
        positive = config.get("positive", True)

        if not thresholds or len(thresholds) != 3:
            logger.error(f"❌ Ongeldige thresholds-configuratie voor '{name}': {thresholds}")
            return None

        interpretation = interpret_value(value, thresholds, positive)

        result = {
            "name": name,
            "value": value,
            "interpretation": interpretation
        }

        logger.info(f"✅ {name}: {value} → {interpretation}")
        return result

    except Exception as e:
        logger.error(f"❌ Fout bij verwerken technische indicator '{name}': {e}")
        return None
