import logging
import json

# ✅ Logging instellen
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def interpret_market_data(data, config_path="market_data_config.json"):
    """
    Interpreteert marktdata (zoals prijs, 24h verandering) op basis van thresholds uit config.
    Geeft een score per metric terug tussen 0–100 + interpretatie en adviesrichting.
    """
    try:
        with open(config_path) as f:
            config = json.load(f)
    except Exception as e:
        logger.error(f"❌ Fout bij laden market config: {e}")
        return {}

    results = {}

    for key, value in data.items():
        if key not in config:
            logger.warning(f"⚠️ Geen interpretatieconfig gevonden voor: {key}")
            continue

        try:
            value = float(value)
            thresholds = config[key].get("thresholds", [])
            is_positive = config[key].get("positive", True)

            score = calculate_score(value, thresholds, is_positive)
            label = generate_label(value, thresholds)

            results[key] = {
                "value": value,
                "score": score,
                "label": label,
            }
        except Exception as e:
            logger.error(f"❌ Fout bij interpreteren van {key}: {e}")

    return results

def calculate_score(value, thresholds, positive=True):
    """
    Bereken een score op basis van thresholds.
    Voorbeeld:
      thresholds = [60000, 70000, 80000] → score = 25, 50, 75, 100 afhankelijk van waarde
    """
    try:
        levels = sorted(thresholds)
        for i, t in enumerate(levels):
            if value < t:
                return (i / len(levels)) * 100 if positive else (1 - i / len(levels)) * 100
        return 100 if positive else 0
    except Exception as e:
        logger.error(f"❌ Fout bij scoreberekening: {e}")
        return 0

def generate_label(value, thresholds):
    """
    Genereer een simpele label zoals 'laag', 'gemiddeld', 'hoog'.
    """
    if not thresholds or len(thresholds) < 2:
        return "Onbekend"

    if value < thresholds[0]:
        return "Laag"
    elif value < thresholds[1]:
        return "Gemiddeld"
    else:
        return "Hoog"
