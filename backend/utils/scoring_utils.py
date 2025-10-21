import logging
import json
from pathlib import Path
from typing import Dict, Any, Optional

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

# ✅ Score calculator per waarde
def calculate_score(value: Optional[float], thresholds: list, positive: bool = True) -> Optional[int]:
    if value is None:
        logger.debug("⚠️ Geen waarde ontvangen voor scoreberekening → None")
        return None
    try:
        value = float(value)
    except (ValueError, TypeError):
        logger.warning(f"⚠️ Ongeldige waarde ({value}) → None")
        return None

    if len(thresholds) != 3:
        logger.warning(f"⚠️ Ongeldige thresholds ({thresholds}) – fallback naar [0, 50, 100]")
        thresholds = [0, 50, 100]

    if positive:
        if value > thresholds[2]:
            return 100
        elif value > thresholds[1]:
            return 75
        elif value > thresholds[0]:
            return 50
        else:
            return 25
    else:
        if value < thresholds[0]:
            return 100
        elif value < thresholds[1]:
            return 75
        elif value < thresholds[2]:
            return 50
        else:
            return 25

# ✅ Score generator op basis van config en data
def generate_scores(data: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
    scores = {}
    total = 0
    count = 0

    data = {k.lower(): v for k, v in data.items()}
    config = {k.lower(): v for k, v in config.items()}

    for name, conf in config.items():
        value = data.get(name)
        thresholds = conf.get("thresholds", [0, 50, 100])
        positive = conf.get("positive", True)

        if len(thresholds) != 3:
            logger.warning(f"⚠️ [{name}] heeft ongeldige thresholds {thresholds} – fallback naar [0, 50, 100]")
            thresholds = [0, 50, 100]

        score = calculate_score(value, thresholds, positive)
        logger.info(f"📊 Indicator: {name} → waarde={value}, score={score}, thresholds={thresholds}, positief={positive}")

        scores[name] = {
            "value": value,
            "score": score,
            "thresholds": thresholds,
            "positive": positive
        }

        if isinstance(score, (int, float)):
            total += score
            count += 1

    avg_score = round(total / count, 2) if count else None
    logger.info(f"✅ {count} geldige indicatoren gescoord (gemiddelde: {avg_score})")
    return {"scores": scores, "total_score": avg_score}

# ✅ Haal macro, technical, market en sentiment scores op uit DB
def get_scores_for_symbol(symbol: str = "BTC") -> Dict[str, Any]:
    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen databaseverbinding voor get_scores_for_symbol")
        return {}

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT name, value FROM macro_data
                WHERE timestamp = (SELECT MAX(timestamp) FROM macro_data)
            """)
            macro_rows = cur.fetchall()
            macro_data = {name: float(value) for name, value in macro_rows}

            cur.execute("""
                SELECT DISTINCT ON (indicator) indicator, value
                FROM technical_indicators
                WHERE symbol = %s
                ORDER BY indicator, timestamp DESC
            """, (symbol,))
            tech_rows = cur.fetchall()
            tech_data = {indicator.lower(): float(value) for indicator, value in tech_rows}

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

            macro_conf_full = load_config("config/macro_indicators_config.json")
            tech_conf = load_config("config/technical_indicators_config.json")
            market_conf_full = load_config("config/market_data_config.json")
            macro_conf = macro_conf_full.get("indicators", {})
            market_conf = market_conf_full.get("indicators", {})

            macro_indicators = {k: v for k, v in macro_conf.items() if v.get("category") == "macro"}
            sentiment_indicators = {k: v for k, v in macro_conf.items() if v.get("category") == "sentiment"}

            sentiment_data = {k: v for k, v in macro_data.items() if k in sentiment_indicators}
            macro_data_cleaned = {k: v for k, v in macro_data.items() if k not in sentiment_data}

            macro_scores = generate_scores(macro_data_cleaned, macro_indicators)
            tech_scores = generate_scores(tech_data, tech_conf)
            market_scores = generate_scores(market_data, market_conf)
            sentiment_scores = generate_scores(sentiment_data, sentiment_indicators)

            macro_avg = macro_scores["total_score"] or 0
            tech_avg = tech_scores["total_score"] or 0
            setup_score = round((macro_avg + tech_avg) / 2, 2) if macro_avg or tech_avg else None

            return {
                "macro_score": macro_scores["total_score"],
                "technical_score": tech_scores["total_score"],
                "market_score": market_scores["total_score"],
                "sentiment_score": sentiment_scores["total_score"],
                "setup_score": setup_score
            }

    except Exception as e:
        logger.error(f"❌ Fout bij ophalen en berekenen van scores: {e}", exc_info=True)
        return {}

    finally:
        conn.close()

# ✅ CLI testfunctie
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

    macro_conf = load_config("config/macro_indicators_config.json").get("indicators", {})
    tech_conf = load_config("config/technical_indicators_config.json")
    market_conf = load_config("config/market_data_config.json").get("indicators", {})

    macro_scores = generate_scores(test_data, macro_conf)
    tech_scores = generate_scores(test_data, tech_conf)
    market_scores = generate_scores(test_data, market_conf)

    print("\n✅ Macro Scores:\n", json.dumps(macro_scores, indent=2))
    print("\n✅ Technical Scores:\n", json.dumps(tech_scores, indent=2))
    print("\n✅ Market Scores:\n", json.dumps(market_scores, indent=2))
    print("\n✅ Live DB Scores:\n", json.dumps(get_scores_for_symbol("BTC"), indent=2))


if __name__ == "__main__":
    test_scoring_utils()
