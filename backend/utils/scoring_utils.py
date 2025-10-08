import logging
import json
from pathlib import Path
from typing import Dict, Any

from backend.utils.db import get_db_connection

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

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

# ✅ Alle actuele scores ophalen
def get_scores_for_symbol(symbol: str = "BTC") -> Dict[str, Any]:
    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen databaseverbinding voor get_scores_for_symbol")
        return {}

    try:
        with conn.cursor() as cur:
            # 1. Macro ophalen
            cur.execute("""
                SELECT name, value FROM macro_data
                WHERE timestamp = (SELECT MAX(timestamp) FROM macro_data)
            """)
            macro_rows = cur.fetchall()
            macro_data = {name: float(value) for name, value in macro_rows}

            # 2. Technische indicators ophalen
            cur.execute("""
                SELECT indicator, value FROM technical_indicators
                WHERE symbol = %s AND timestamp = (
                    SELECT MAX(timestamp) FROM technical_indicators WHERE symbol = %s
                )
            """, (symbol, symbol))
            tech_rows = cur.fetchall()
            tech_data = {indicator: float(value) for indicator, value in tech_rows}

            # 3. Market data ophalen
            cur.execute("""
                SELECT price, volume, change_24h FROM market_data
                WHERE symbol = %s
                ORDER BY timestamp DESC
                LIMIT 1
            """, (symbol,))
            market_row = cur.fetchone()
            market_data = {}
            if market_row:
                market_data = {
                    "price": float(market_row[0]),
                    "volume": float(market_row[1]),
                    "change_24h": float(market_row[2]),
                }

            # 4. Configs laden
            macro_conf = load_config("config/macro_indicators_config.json")
            tech_conf = load_config("config/technical_indicators_config.json")
            market_conf = load_config("config/market_data_config.json")

            # 5. Scoreberekening
            macro_scores = generate_scores(macro_data, macro_conf)
            tech_scores = generate_scores(tech_data, tech_conf)
            market_scores = generate_scores(market_data, market_conf)

            return {
                "macro_score": macro_scores["total_score"],
                "technical_score": tech_scores["total_score"],
                "market_score": market_scores["total_score"],
                "sentiment_score": 0,  # nog niet geïmplementeerd
                "setup_score": round((macro_scores["total_score"] + tech_scores["total_score"]) / 2, 2)
            }

    except Exception as e:
        logger.error(f"❌ Fout bij ophalen en berekenen van scores: {e}")
        return {}

# ✅ Testfunctie
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
    print("\n✅ Live DB Scores:\n", json.dumps(get_scores_for_symbol("BTC"), indent=2))

if __name__ == "__main__":
    test_scoring_utils()
