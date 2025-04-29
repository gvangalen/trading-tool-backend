import requests
import json
import logging

# ✅ Logging instellen
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def extract_nested_value(data, key_path):
    """Haalt een geneste waarde uit een dict op basis van een pad zoals 'data.0.value'."""
    keys = key_path.split(".")
    for key in keys:
        if isinstance(data, list) and key.isdigit():
            data = data[int(key)]
        elif isinstance(data, dict):
            data = data.get(key, {})
        else:
            return None
    return data if isinstance(data, (int, float, str)) else None


def interpret_value(value, rules):
    """Geeft interpretatie en actie terug op basis van drempelwaarden."""
    try:
        numeric_value = float(value)
    except ValueError:
        logger.warning(f"⚠️ Ongeldige numerieke waarde: {value}")
        return None, None

    for rule in sorted(rules, key=lambda r: r["threshold"], reverse=True):
        if numeric_value >= rule["threshold"]:
            return rule["interpretation"], rule["action"]

    return None, None


def process_macro_indicator(name, config):
    """
    Verwerkt één macro-indicator volgens config.
    Returns:
        dict met naam, waarde, interpretatie, actie
    """
    try:
        url = config["api_url"]
        extract_key = config["extract_key"]
        rules = config.get("interpretation_rules", [])

        logger.info(f"🌐 Ophalen: {name} → {url}")
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()

        value = extract_nested_value(data, extract_key)
        if value is None:
            logger.warning(f"⚠️ Geen waarde gevonden voor '{name}' met key '{extract_key}'")
            return None

        interpretation, action = interpret_value(value, rules)
        result = {
            "name": name,
            "value": value,
            "interpretation": interpretation,
            "action": action,
        }

        logger.info(f"✅ {name}: {value} → {interpretation} ({action})")
        return result

    except Exception as e:
        logger.error(f"❌ Fout bij verwerken indicator '{name}': {e}")
        return None
