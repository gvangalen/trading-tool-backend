import logging
from typing import Dict, Any, Optional
from backend.utils.db import get_db_connection

# =========================================================
# ⚙️ Logging setup
# =========================================================
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# =========================================================
# 🧩 Bekende naam-aliases (optioneel)
# =========================================================
NAME_ALIASES = {
    "fear_and_greed_index": "fear_greed_index",
    "fear_greed": "fear_greed_index",
    "sandp500": "sp500",
    "s&p500": "sp500",
    "s&p_500": "sp500",
    "sp_500": "sp500",
}

# =========================================================
# 🧠 Normalisatie functie (eentje voor ALLES)
# =========================================================
def normalize_indicator_name(name: str) -> str:
    normalized = (
        name.lower()
        .replace("&", "and")
        .replace("s&p", "sp")
        .replace(" ", "_")
        .replace("-", "_")
        .strip()
    )
    return NAME_ALIASES.get(normalized, normalized)


# =========================================================
# ✅ Universele database-functie voor scoreregels
# =========================================================
def get_score_rule_from_db(category: str, indicator_name: str, value: float) -> Optional[dict]:
    conn = get_db_connection()
    if not conn:
        logger.error("❌ get_score_rule_from_db(): geen DB-verbinding")
        return None

    table_map = {
        "technical": "technical_indicator_rules",
        "macro": "macro_indicator_rules",
        "market": "market_indicator_rules",
    }
    table = table_map.get(category)
    if not table:
        logger.error(f"⚠️ Ongeldige categorie: {category}")
        return None

    normalized = normalize_indicator_name(indicator_name)

    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT range_min, range_max, score, trend, interpretation, action
                FROM {table}
                WHERE LOWER(REPLACE(REPLACE(REPLACE(indicator, '&', 'and'), ' ', '_'), '-', '_')) = %s
                ORDER BY range_min ASC
            """, (normalized,))
            rows = cur.fetchall()

        if not rows:
            logger.warning(f"⚠️ Geen scoreregels gevonden voor '{normalized}' ({category})")
            return None

        for r in rows:
            if r[0] <= value <= r[1]:
                return {
                    "score": r[2],
                    "trend": r[3],
                    "interpretation": r[4],
                    "action": r[5],
                }

        # Buiten bereik → geen match
        return None

    except Exception as e:
        logger.error(f"❌ Error get_score_rule_from_db({indicator_name}): {e}", exc_info=True)
        return None
    finally:
        conn.close()


# =========================================================
# ✅ Dynamische scoregenerator (macro, technical, market)
# =========================================================
def generate_scores_db(category: str, data: Optional[Dict[str, float]] = None) -> Dict[str, Any]:
    if data is None:
        data = {}
        conn = get_db_connection()
        if not conn:
            logger.error("❌ generate_scores_db(auto-mode): geen DB-verbinding")
            return {"scores": {}, "total_score": 0}

        try:
            with conn.cursor() as cur:
                # Macro
                if category == "macro":
                    cur.execute("""
                        SELECT DISTINCT ON (name) name, value
                        FROM macro_data
                        ORDER BY name, timestamp DESC;
                    """)
                    rows = cur.fetchall()
                    data = {
                        normalize_indicator_name(r[0]): float(r[1])
                        for r in rows if r[1] is not None
                    }

                # Market (price, volume, change_24h)
                elif category == "market":
                    cur.execute("""
                        SELECT DISTINCT ON (symbol)
                            price, volume, change_24h
                        FROM market_data
                        WHERE symbol = 'BTC'
                        ORDER BY symbol, timestamp DESC;
                    """)
                    row = cur.fetchone()
                    if row:
                        data = {
                            "price": float(row[0]),
                            "volume": float(row[1]),
                            "change_24h": float(row[2]),
                        }

                # Technical
                else:
                    cur.execute("""
                        SELECT DISTINCT ON (indicator) indicator, value
                        FROM technical_indicators
                        ORDER BY indicator, timestamp DESC;
                    """)
                    rows = cur.fetchall()
                    data = {
                        normalize_indicator_name(r[0]): float(r[1])
                        for r in rows if r[1] is not None
                    }

        except Exception as e:
            logger.error(f"❌ generate_scores_db(): fout bij ophalen data ({category}): {e}", exc_info=True)
        finally:
            conn.close()

    if not data:
        logger.warning(f"⚠️ Geen inputdata gevonden voor categorie {category}")
        return {"scores": {}, "total_score": 0}

    # Scoreberekening
    scores = {}
    total_score = 0
    count = 0

    for indicator, value in data.items():
        if value is None:
            continue

        rule = get_score_rule_from_db(category, indicator, value)
        if not rule:
            continue

        score = int(rule["score"])
        scores[indicator] = {
            "value": value,
            "score": score,
            "trend": rule.get("trend", "–"),
            "interpretation": rule.get("interpretation", ""),
            "action": rule.get("action", ""),
        }

        total_score += score
        count += 1

    avg_score = round(total_score / count) if count else 10

    return {"scores": scores, "total_score": avg_score}


# =========================================================
# ✅ Combined scores for dashboard + report
# =========================================================
def get_scores_for_symbol(include_metadata: bool = False) -> Dict[str, Any]:
    conn = get_db_connection()
    if not conn:
        logger.error("❌ get_scores_for_symbol: geen DB-verbinding")
        return {}

    try:
        with conn.cursor() as cur:
            # Macro
            cur.execute("""
                SELECT DISTINCT ON (name) name, value
                FROM macro_data
                ORDER BY name, timestamp DESC;
            """)
            macro_data = {
                normalize_indicator_name(r[0]): float(r[1])
                for r in cur.fetchall() if r[1] is not None
            }

            # Technical
            cur.execute("""
                SELECT DISTINCT ON (indicator) indicator, value
                FROM technical_indicators
                ORDER BY indicator, timestamp DESC;
            """)
            technical_data = {
                normalize_indicator_name(r[0]): float(r[1])
                for r in cur.fetchall() if r[1] is not None
            }

        macro_scores = generate_scores_db("macro", macro_data)
        tech_scores = generate_scores_db("technical", technical_data)
        market_scores = generate_scores_db("market")

        macro_avg = macro_scores["total_score"]
        tech_avg = tech_scores["total_score"]
        market_avg = market_scores["total_score"]

        setup_score = round((macro_avg + tech_avg) / 2)

        result = {
            "macro_score": macro_avg,
            "technical_score": tech_avg,
            "market_score": market_avg,
            "setup_score": setup_score,
        }

        if include_metadata:
            def top(scores_dict):
                if "scores" not in scores_dict:
                    return []
                return sorted(
                    scores_dict["scores"].items(),
                    key=lambda x: x[1]["score"],
                    reverse=True
                )[:3]

            result.update({
                "macro_top_contributors": [i[0] for i in top(macro_scores)],
                "technical_top_contributors": [i[0] for i in top(tech_scores)],
                "market_top_contributors": [i[0] for i in top(market_scores)],
                "macro_interpretation": "Macro-data via scoreregels",
                "technical_interpretation": "Technische data via scoreregels",
                "market_interpretation": "Marktdata via scoreregels",
            })

        return result

    except Exception as e:
        logger.error(f"❌ get_scores_for_symbol(): {e}", exc_info=True)
        return {}

    finally:
        conn.close()


# Backwards compatible wrappers
def calculate_macro_scores(data: Dict[str, float]) -> Dict[str, Any]:
    return generate_scores_db("macro", data)

def calculate_technical_scores(data: Dict[str, float]) -> Dict[str, Any]:
    return generate_scores_db("technical", data)

def calculate_market_scores(data: Dict[str, float]) -> Dict[str, Any]:
    return generate_scores_db("market", data)


def calculate_score_from_rules(value: float, rules: list[dict]) -> dict:
    for r in rules:
        if r["range_min"] <= value <= r["range_max"]:
            return {
                "score": r["score"],
                "trend": r["trend"],
                "interpretation": r["interpretation"],
                "action": r["action"],
            }
    return {
        "score": 50,
        "trend": "Neutraal",
        "interpretation": f"Waarde {value} valt buiten alle ranges.",
        "action": "Geen actie.",
    }
