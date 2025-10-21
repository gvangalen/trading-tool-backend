import logging
from backend.config.config_loader import load_config_file
from backend.utils.scoring_utils import calculate_score, generate_label  # ✅ Externe scoringlogica gebruiken

# ✅ Logging instellen
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

CONFIG_PATH = "config/market_data_config.json"

def interpret_market_data(data, config_path=CONFIG_PATH):
    """
    ➤ Interpreteert marktdata zoals prijs, 24h verandering en volume op basis van thresholds uit config.
    ➤ Retourneert: waarde, score (0–100), en interpretatielabel ('Laag', 'Gemiddeld', etc.)
    """
    try:
        config = load_config_file(config_path)
        indicators = config.get("indicators", {})
    except Exception as e:
        logger.error(f"❌ Fout bij laden market config: {e}")
        return {}

    results = {}

    for key, raw_value in data.items():
        if key not in indicators:
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

        indicator_cfg = indicators[key]
        thresholds = indicator_cfg.get("thresholds", [])
        is_positive = indicator_cfg.get("positive", True)

        score = calculate_score(value, thresholds, is_positive)
        label = generate_label(value, thresholds, is_positive)

        results[key] = {
            "value": value,
            "score": score,
            "label": label
        }

    logger.info(f"✅ Geïnterpreteerde market data: {results}")
    return results
