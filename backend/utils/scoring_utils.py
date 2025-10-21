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
# ✅ Universele Scorefunctie (25–100 schaal)
# =========================================================
def calculate_score(value: Optional[float], thresholds: list, positive: bool = True) -> Optional[int]:
    """
    ➤ Basisfunctie voor ruwe score (25–100 schaal)
    """
    if value is None:
        return None
    try:
        value = float(value)
    except (ValueError, TypeError):
        return None

    if len(thresholds) != 3:
        thresholds = [0, 50, 100]

    # Positieve of negatieve correlatie
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
# ✅ Unified interpretatie + trend + actie
# =========================================================
def calculate_score_from_config(value: float, config: dict) -> dict:
    """
    ➤ Geeft score + trend + interpretatie + actie terug voor macro/technical indicatoren.
    """
    thresholds = config.get("thresholds", [0, 50, 100])
    positive = config.get("positive", True)
    score = calculate_score(value, thresholds, positive)

    if score is None:
        return {
            "score": 0,
            "trend": "Onbekend",
            "interpretation": "Geen geldige waarde ontvangen.",
            "action": config.get("action", "")
        }

    # Trend bepalen
    if score >= 90:
        trend = "Zeer sterk"
    elif score >= 75:
        trend = "Sterk"
    elif score >= 50:
        trend = "Neutraal"
    else:
        trend = "Zwak"

    # Interpretatie op basis van correlatie
    correlation = config.get("correlation", "positief")
    if correlation == "positief":
        if score >= 75:
            interpretation = "Sterk positief signaal"
        elif score >= 50:
            interpretation = "Neutraal / licht positief"
        else:
            interpretation = "Negatief signaal"
    else:
        if score >= 75:
            interpretation = "Sterk negatief signaal"
        elif score >= 50:
            interpretation = "Neutraal / afwachtend"
        else:
            interpretation = "Positief teken"

    return {
        "score": score,
        "trend": trend,
        "interpretation": interpretation,
        "action": config.get("action", "")
    }


# =========================================================
# ✅ Batch generator voor alle indicatoren
# =========================================================
def generate_scores(data: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
    """
    ➤ Verwerkt meerdere indicatoren (macro / technical / market)
    """
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

    avg_score = round(total / count, 2) if count else 0
    logger.info(f"✅ {count} geldige indicatoren gescoord (gemiddelde: {avg_score})")
    return {"scores": scores, "total_score": avg_score}


# =========================================================
# ✅ Haal actuele macro / technical / market / sentiment data uit DB
# =========================================================
def get_scores_for_symbol(symbol: str = "BTC") -> Dict[str, Any]:
    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen databaseverbinding voor get_scores_for_symbol")
        return {}

    try:
        with conn.cursor() as cur:
            # Macro
            cur.execute("""
                SELECT name, value FROM macro_data
                WHERE timestamp = (SELECT MAX(timestamp) FROM macro_data)
            """)
            macro_rows = cur.fetchall()
            macro_data = {name: float(value) for name, value in macro_rows}

            # Technical
            cur.execute("""
                SELECT DISTINCT ON (indicator) indicator, value
                FROM technical_indicators
                WHERE symbol = %s
                ORDER BY indicator, timestamp DESC
            """, (symbol,))
            tech_rows = cur.fetchall()
            tech_data = {indicator.lower(): float(value) for indicator, value in tech_rows}

            # Market
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

            # Configs
            macro_conf_full = load_config("config/macro_indicators_config.json")
            tech_conf = load_config("config/technical_indicators_config.json")
            market_conf_full = load_config("config/market_data_config.json")

            macro_conf = macro_conf_full.get("indicators", {})
            market_conf = market_conf_full.get("indicators", {})

            # Macro & sentiment splitsen
            macro_indicators = {k: v for k, v in macro_conf.items() if v.get("category") == "macro"}
            sentiment_indicators = {k: v for k, v in macro_conf.items() if v.get("category") == "sentiment"}

            sentiment_data = {k: v for k, v in macro_data.items() if k in sentiment_indicators}
            macro_data_cleaned = {k: v for k, v in macro_data.items() if k not in sentiment_data}

            # Scoreberekening per categorie
            macro_scores = generate_scores(macro_data_cleaned, macro_indicators)
            tech_scores = generate_scores(tech_data, tech_conf.get("indicators", {}))
            market_scores = generate_scores(market_data, market_conf)
            sentiment_scores = generate_scores(sentiment_data, sentiment_indicators)

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
# ✅ Testfunctie
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
