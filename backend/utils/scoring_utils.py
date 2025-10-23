import logging
import json
from pathlib import Path
from typing import Dict, Any, Optional

from backend.utils.db import get_db_connection

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent


# =========================================================
# ✅ Config Loader
# =========================================================
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


# =========================================================
# ✅ Score logica per type
# =========================================================
def calculate_score(value: Optional[float], thresholds: list, positive: bool = True) -> Optional[int]:
    """
    ➤ Basis scorefunctie met minimale waarde van 10 (nooit 0).
    """
    if value is None:
        return None
    try:
        value = float(value)
    except (ValueError, TypeError):
        return None

    if len(thresholds) != 3:
        thresholds = [0, 50, 100]

    if positive:
        if value >= thresholds[2]:
            return 100
        elif value >= thresholds[1]:
            return 75
        elif value >= thresholds[0]:
            return 50
        else:
            return 25
    else:
        if value <= thresholds[0]:
            return 100
        elif value <= thresholds[1]:
            return 75
        elif value <= thresholds[2]:
            return 50
        else:
            return 25


# =========================================================
# ✅ Score + Interpretatie + Trend per datapunt
# =========================================================
def calculate_score_from_config(value: float, config: dict) -> dict:
    thresholds = config.get("thresholds", [0, 50, 100])
    positive = config.get("positive", True)
    score = calculate_score(value, thresholds, positive)

    if score is None:
        return {
            "score": 10,
            "trend": "Onbekend",
            "interpretation": "Geen geldige waarde ontvangen.",
            "action": config.get("action", "")
        }

    if score >= 90:
        trend = "Zeer sterk"
    elif score >= 75:
        trend = "Sterk"
    elif score >= 50:
        trend = "Neutraal"
    else:
        trend = "Zwak"

    correlation = config.get("correlation", "positief")
    if correlation == "positief":
        interpretation = (
            "Sterk positief signaal" if score >= 75 else
            "Neutraal / licht positief" if score >= 50 else
            "Negatief signaal"
        )
    else:
        interpretation = (
            "Sterk negatief signaal" if score >= 75 else
            "Neutraal / afwachtend" if score >= 50 else
            "Positief teken"
        )

    return {
        "score": score,
        "trend": trend,
        "interpretation": interpretation,
        "action": config.get("actions", {}).get(str(score), "")
    }


# =========================================================
# ✅ Per-type scoregenerator
# =========================================================
def calculate_macro_scores(data: Dict[str, float], config: Dict[str, Any]) -> Dict[str, Any]:
    return generate_scores(data, config)

def calculate_technical_scores(data: Dict[str, float], config: Dict[str, Any]) -> Dict[str, Any]:
    return generate_scores(data, config)

def calculate_market_scores(data: Dict[str, float], config: Dict[str, Any]) -> Dict[str, Any]:
    return generate_scores(data, config)

def calculate_sentiment_scores(data: Dict[str, float], config: Dict[str, Any]) -> Dict[str, Any]:
    return generate_scores(data, config)


def generate_scores(data: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
    scores = {}
    total = 0
    count = 0

    data = {k.lower(): v for k, v in data.items()}
    config = {k.lower(): v for k, v in config.items()}

    for name, conf in config.items():
        value = data.get(name)
        result = calculate_score_from_config(value, conf)
        score = result["score"]

        scores[name] = {
            "value": value,
            "score": score,
            "trend": result["trend"],
            "interpretation": result["interpretation"],
            "action": result["action"],
            "thresholds": conf.get("thresholds"),
            "positive": conf.get("positive", True)
        }

        if isinstance(score, (int, float)):
            total += score
            count += 1

    avg_score = round(total / count, 2) if count else 10
    logger.info(f"✅ {count} geldige indicatoren gescoord (gemiddelde: {avg_score})")
    return {"scores": scores, "total_score": avg_score}


# =========================================================
# ✅ Setup Score Matching
# =========================================================
def match_setups_to_score(setups: list, total_score: float) -> list:
    return [s for s in setups if s.get("min_score", 0) <= total_score <= s.get("max_score", 100)]

def find_best_matching_setup(setups: list, total_score: float) -> Optional[dict]:
    best_setup = None
    smallest_diff = float("inf")
    for setup in setups:
        min_score = setup.get("min_score", 0)
        max_score = setup.get("max_score", 100)
        if min_score <= total_score <= max_score:
            center = (min_score + max_score) / 2
            diff = abs(center - total_score)
            if diff < smallest_diff:
                smallest_diff = diff
                best_setup = setup
    return best_setup


# =========================================================
# ✅ Data ophalen + scores berekenen
# =========================================================
def get_scores_for_symbol() -> Dict[str, Any]:
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
                ORDER BY indicator, timestamp DESC
            """)
            tech_rows = cur.fetchall()
            tech_data = {indicator.lower(): float(value) for indicator, value in tech_rows}

            cur.execute("""
                SELECT price, volume, change_24h FROM market_data
                ORDER BY timestamp DESC
                LIMIT 1
            """)
            market_row = cur.fetchone()
            market_data = {}
            if market_row:
                market_data = {
                    "price": float(market_row[0]),
                    "volume": float(market_row[1]),
                    "change_24h": float(market_row[2])
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

            macro_scores = calculate_macro_scores(macro_data_cleaned, macro_indicators)
            tech_scores = calculate_technical_scores(tech_data, tech_conf.get("indicators", {}))
            market_scores = calculate_market_scores(market_data, market_conf)
            sentiment_scores = calculate_sentiment_scores(sentiment_data, sentiment_indicators)

            macro_avg = macro_scores["total_score"]
            tech_avg = tech_scores["total_score"]
            setup_score = round((macro_avg + tech_avg) / 2, 2)

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


# =========================================================
# ✅ Dashboard functie
# =========================================================
def get_dashboard_scores(macro_data, technical_data, setups):
    macro_scores = [d["score"] for d in macro_data if isinstance(d.get("score"), (int, float))]
    macro_score = round(sum(macro_scores) / len(macro_scores), 2) if macro_scores else 10

    used_scores = [v["score"] for v in technical_data.values()]
    total_possible = len(used_scores) * 100
    technical_score = round((sum(used_scores) / total_possible) * 100, 2) if total_possible else 10

    setup_score = len(setups) * 10 if setups else 10

    return {
        "macro": macro_score,
        "technical": technical_score,
        "setup": setup_score
    }


# =========================================================
# ✅ Test
# =========================================================
def test_scoring_utils():
    test_data = {
        "fear_greed": 77,
        "dxy": 104.3,
        "rsi": 63,
        "volume": 3800,
        "ma_200": 1.02,
        "price": 73000,
        "change_24h": 1.2
    }

    macro_conf = load_config("config/macro_indicators_config.json").get("indicators", {})
    tech_conf = load_config("config/technical_indicators_config.json").get("indicators", {})
    market_conf = load_config("config/market_data_config.json").get("indicators", {})

    macro_scores = generate_scores(test_data, macro_conf)
    tech_scores = generate_scores(test_data, tech_conf)
    market_scores = generate_scores(test_data, market_conf)

    print("\n✅ Macro Scores:\n", json.dumps(macro_scores, indent=2))
    print("\n✅ Technical Scores:\n", json.dumps(tech_scores, indent=2))
    print("\n✅ Market Scores:\n", json.dumps(market_scores, indent=2))
    print("\n✅ Combined DB Fetch:\n", json.dumps(get_scores_for_symbol("BTC"), indent=2))


if __name__ == "__main__":
    test_scoring_utils()
