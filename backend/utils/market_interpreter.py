import logging
from backend.config.config_loader import load_config_file

# ✅ Logging instellen
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ✅ Config-pad gebruiken zoals macro & technical
CONFIG_PATH = "config/market_data_config.json"

def interpret_market_data(data, config_path=CONFIG_PATH):
    """
    ➤ Interpreteert marktdata (zoals prijs, 24h verandering) op basis van thresholds uit config.
    ➤ Geeft een score per metric terug tussen 0–100 + interpretatie-label.
    """
    try:
        config = load_config_file(config_path)
    except Exception as e:
        logger.error(f"❌ Fout bij laden market config: {e}")
        return {}

    results = {}

    for key, raw_value in data.items():
        if key not in config:
            logger.warning(f"⚠️ Geen interpretatieconfig gevonden voor: {key}")
            continue

        try:
            value = float(raw_value)
        except (ValueError, TypeError):
            logger.warning(f"⚠️ Ongeldige numerieke waarde voor '{key}': {raw_value}")
            results[key] = {
                "value": raw_value,
                "score": 0,
                "label": "Ongeldig"
            }
            continue

        thresholds = config[key].get("thresholds", [])
        is_positive = config[key].get("positive", True)

        score = calculate_score(value, thresholds, is_positive)
        label = generate_label(value, thresholds, is_positive)

        results[key] = {
            "value": value,
            "score": score,
            "label": label,
        }

    return results


def calculate_score(value, thresholds, positive=True):
    """
    ➤ Bereken een score (0–100) op basis van thresholds.
    """
    try:
        levels = sorted(thresholds)
        for i, t in enumerate(levels):
            if value < t:
                fraction = i / len(levels)
                return round(fraction * 100 if positive else (1 - fraction) * 100)
        return 100 if positive else 0
    except Exception as e:
        logger.error(f"❌ Fout bij scoreberekening: {e}")
        return 0


def generate_label(value, thresholds, positive=True):
    """
    ➤ Genereer interpretatielabel: 'Laag', 'Gemiddeld', 'Hoog', 'Zeer hoog', etc.
    """
    if not thresholds or len(thresholds) < 2:
        return "Onbekend"

    try:
        if value < thresholds[0]:
            return "Zeer laag" if positive else "Zeer hoog"
        elif value < thresholds[1]:
            return "Laag" if positive else "Hoog"
        elif len(thresholds) == 3 and value < thresholds[2]:
            return "Gemiddeld"
        else:
            return "Hoog" if positive else "Laag"
    except Exception as e:
        logger.warning(f"⚠️ Labelgeneratie mislukt voor waarde {value}: {e}")
        return "Onbekend"
