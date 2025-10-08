import logging
import json
from pathlib import Path
from typing import Dict, Any

from backend.utils.db import get_db_connection  # ✅ Nieuw

# ✅ Logging instellen
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ✅ Basis directory bepalen (automatisch)
BASE_DIR = Path(__file__).resolve().parent.parent

# ✅ Config loader
def load_config(relative_path: str) -> Dict[str, Any]:
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
    Haalt macro-, technische-, sentiment- en setup-score uit DB (setups-tabel).
    """
    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen databaseverbinding voor get_scores_for_symbol")
        return {}

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT macro_score, technical_score, sentiment_score, setup_score
                FROM setups
                WHERE symbol = %s
                ORDER BY created_at DESC
                LIMIT 1
            """, (symbol,))
            row = cur.fetchone()
    except Exception as e:
        logger.error(f"❌ Database query mislukt in get_scores_for_symbol: {e}")
        return {}

    if row:
        macro, technical, sentiment, setup = row
        logger.info(f"✅ Setup-scores geladen uit DB voor {symbol}: macro={macro}, tech={technical}, sentiment={sentiment}, setup={setup}")
        return {
            "macro_score": macro or 0,
            "technical_score": technical or 0,
            "sentiment_score": sentiment or 0,
            "setup_score": setup or 0
        }

    logger.warning(f"⚠️ Geen setup-scores gevonden in DB voor {symbol}")
    return {}

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
    print("\n✅ DB Scores:\n", json.dumps(get_scores_for_symbol("BTC"), indent=2))

if __name__ == "__main__":
    test_scoring_utils()
