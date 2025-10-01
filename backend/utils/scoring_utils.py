# ✅ backend/utils/scoring_utils.py
import logging
import json
from pathlib import Path
from typing import Dict, Any

# ✅ Logging instellen
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ✅ Basis directory bepalen (automatisch)
BASE_DIR = Path(__file__).resolve().parent.parent

# ✅ Config loader
def load_config(relative_path: str) -> Dict[str, Any]:
    """
    Load a JSON configuration file from a relative path.
    """
    full_path = BASE_DIR / relative_path
    try:
        with open(full_path, "r") as f:
            config = json.load(f)
        logger.info(f"✅ Config loaded from {relative_path}")
        return config
    except Exception as e:
        logger.error(f"❌ Failed to load config ({relative_path}): {e}")
        return {}

# ✅ Score calculator
def calculate_score(value: float, thresholds: list, positive: bool = True) -> int:
    """
    Calculate a score between -2 and 2 based on thresholds.
    If positive=True: higher values are better.
    """
    if value is None:
        return 0
    try:
        value = float(value)
    except (ValueError, TypeError):
        return 0

    if positive:
        return 2 if value > thresholds[2] else 1 if value > thresholds[1] else -1 if value > thresholds[0] else -2
    else:
        return 2 if value < thresholds[0] else 1 if value < thresholds[1] else -1 if value < thresholds[2] else -2

# ✅ Score generator
def generate_scores(data: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Generate individual and total scores based on the provided config.
    """
    scores = {}
    total = 0
    count = 0

    for name, conf in config.items():
        value = data.get(name)
        thresholds = conf.get("thresholds", [0, 50, 100])
        positive = conf.get("positive", True)

        score = calculate_score(value, thresholds, positive)
        scores[name] = {
            "value": value,
            "score": score,
            "thresholds": thresholds,
            "positive": positive
        }

        if isinstance(score, (int, float)):
            total += score
            count += 1

    avg_score = round(total / count, 2) if count else 0
    logger.info(f"✅ Scored {count} indicators (average: {avg_score})")
    return {"scores": scores, "total_score": avg_score}

# ✅ Nieuw: wrapper voor gebruik in rapporten
def get_scores_for_symbol(symbol: str = "BTC") -> Dict[str, Any]:
    """
    Laad data (mock of later live uit DB), bereken macro-, technische en markt-scores.
    Retourneer scores zoals nodig voor tradingrapport.
    """
    # 🔁 Simulatie / tijdelijk hardcoded data (later vervangen door DB-query)
    data = {
        "fear_greed_index": 77,
        "btc_dominance": 52.1,
        "dxy": 104.3,
        "rsi": 63,
        "volume": 8000000000,
        "ma_200": 71000,
        "price": 73000,
        "change_24h": 1.2
    }

    macro_conf = load_config("config/macro_indicators_config.json")
    tech_conf = load_config("config/technical_indicators_config.json")
    market_conf = load_config("config/market_data_config.json")

    macro = generate_scores(data, macro_conf)
    technical = generate_scores(data, tech_conf)
    market = generate_scores(data, market_conf)

    return {
        "macro_score": macro["total_score"],
        "technical_score": technical["total_score"],
        "setup_score": 1.0,  # ➕ later dynamisch ophalen via DB of validatie
        "sentiment_score": market["total_score"]
    }

# ✅ Testfunctie voor lokaal testen
def test_scoring_utils():
    test_data = {
        "fear_greed_index": 77,
        "btc_dominance": 52.1,
        "dxy": 104.3,
        "rsi": 63,
        "volume": 8000000000,
        "ma_200": 71000,
        "price": 73000,
        "change_24h": 1.2
    }

    macro_conf = load_config("config/macro_indicators_config.json")
    tech_conf = load_config("config/technical_indicators_config.json")
    market_conf = load_config("config/market_data_config.json")

    macro_scores = generate_scores(test_data, macro_conf)
    tech_scores = generate_scores(test_data, tech_conf)
    market_scores = generate_scores(test_data, market_conf)

    print("\n✅ Macro Scores:\n", json.dumps(macro_scores, indent=2))
    print("\n✅ Technical Scores:\n", json.dumps(tech_scores, indent=2))
    print("\n✅ Market Scores:\n", json.dumps(market_scores, indent=2))

if __name__ == "__main__":
    test_scoring_utils()
    print("\n✅ Scores for symbol:\n", get_scores_for_symbol("BTC"))
