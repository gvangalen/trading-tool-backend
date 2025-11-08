import logging
from typing import Dict, Any, Optional
from backend.utils.db import get_db_connection

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# =========================================================
# ✅ Universele database-functie voor scoreregels
# =========================================================
def get_score_rule_from_db(category: str, indicator_name: str, value: float) -> Optional[dict]:
    """
    Haalt de juiste scoreregel op uit de database op basis van:
    - category: 'technical', 'macro' of 'market'
    - indicator_name
    - value

    Retourneert een dict met score, trend, interpretation, action.
    """
    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding in get_score_rule_from_db()")
        return None

    table_map = {
        "technical": "technical_indicator_rules",
        "macro": "macro_indicator_rules",
        # Market gebruikt dezelfde scoreregels als technical
        "market": "technical_indicator_rules",
    }

    table = table_map.get(category)
    if not table:
        logger.error(f"⚠️ Ongeldige categorie: {category}")
        return None

    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT range_min, range_max, score, trend, interpretation, action
                FROM {table}
                WHERE indicator = %s
                ORDER BY range_min ASC
            """, (indicator_name,))
            rules = cur.fetchall()

            if not rules:
                logger.warning(f"⚠️ Geen scoreregels gevonden voor {indicator_name} ({category})")
                return None

            for r in rules:
                if r[0] <= value <= r[1]:
                    return {
                        "score": r[2],
                        "trend": r[3],
                        "interpretation": r[4],
                        "action": r[5]
                    }

            # Als geen regel matched
            logger.info(f"ℹ️ Waarde {value} valt buiten gedefinieerde ranges voor {indicator_name}")
            return None

    except Exception as e:
        logger.error(f"❌ Fout in get_score_rule_from_db voor {indicator_name}: {e}")
        return None
    finally:
        conn.close()


# =========================================================
# ✅ Dynamische scoregenerator op basis van DB
# =========================================================
def generate_scores_db(category: str, data: Dict[str, float]) -> Dict[str, Any]:
    """
    Genereert scores voor alle indicatoren in een categorie ('technical', 'macro', 'market')
    op basis van database-scoreregels.
    """
    scores = {}
    total_score = 0
    count = 0

    for indicator, value in data.items():
        if value is None:
            continue

        result = get_score_rule_from_db(category, indicator, float(value))
        if not result:
            logger.info(f"⚠️ Geen score gevonden voor {indicator}, waarde={value}")
            continue

        score = round(result.get("score", 10))
        scores[indicator] = {
            "value": value,
            "score": score,
            "trend": result.get("trend", "Onbekend"),
            "interpretation": result.get("interpretation", ""),
            "action": result.get("action", "")
        }

        total_score += score
        count += 1

    avg_score = round(total_score / count) if count else 10
    logger.info(f"✅ {count} geldige {category}-indicatoren gescoord (gemiddelde: {avg_score})")

    return {"scores": scores, "total_score": avg_score}


# =========================================================
# ✅ Samengestelde scoreberekening voor dashboard/rapport
# =========================================================
def get_scores_for_symbol(include_metadata: bool = False) -> Dict[str, Any]:
    """
    Berekent gecombineerde macro-, technical- en market-scores
    volledig op basis van databasewaarden.
    Market gebruikt een subset van technical-indicatoren (price, volume, change_24h).
    """
    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding in get_scores_for_symbol()")
        return {}

    try:
        with conn.cursor() as cur:
            # === Macro data ===
            cur.execute("""
                SELECT DISTINCT ON (name) name, value
                FROM macro_data
                ORDER BY name, timestamp DESC
            """)
            macro_data = {r[0]: float(r[1]) for r in cur.fetchall() if r[1] is not None}

            # === Technische data ===
            cur.execute("""
                SELECT DISTINCT ON (indicator) indicator, value
                FROM technical_indicators
                ORDER BY indicator, timestamp DESC
            """)
            technical_data = {r[0]: float(r[1]) for r in cur.fetchall() if r[1] is not None}

        # ✅ Subset voor market score
        market_indicators = ["price", "volume", "change_24h"]
        market_data = {k: v for k, v in technical_data.items() if k.lower() in market_indicators}

        # ✅ Scores berekenen uit DB (zonder configs)
        macro_scores = generate_scores_db("macro", macro_data)
        tech_scores = generate_scores_db("technical", technical_data)
        market_scores = generate_scores_db("market", market_data)

        macro_avg = macro_scores["total_score"]
        tech_avg = tech_scores["total_score"]
        market_avg = market_scores["total_score"]
        setup_score = round((macro_avg + tech_avg) / 2)

        result = {
            "macro_score": macro_avg,
            "technical_score": tech_avg,
            "market_score": market_avg,
            "setup_score": setup_score
        }

        # 🧩 Extra metadata
        if include_metadata:
            def extract_top(scores_dict):
                if not scores_dict.get("scores"):
                    return []
                return sorted(
                    scores_dict["scores"].items(),
                    key=lambda x: x[1]["score"],
                    reverse=True
                )[:3]

            result.update({
                "macro_top_contributors": [i[0] for i in extract_top(macro_scores)],
                "technical_top_contributors": [i[0] for i in extract_top(tech_scores)],
                "market_top_contributors": [i[0] for i in extract_top(market_scores)],
                "macro_interpretation": "Macro-data uit database",
                "technical_interpretation": "Technische data uit database",
                "market_interpretation": "Marktdata berekend op basis van prijs, volume en change_24h",
            })

        logger.info(f"✅ DB-scores berekend: {result}")
        return result

    except Exception as e:
        logger.error(f"❌ Fout bij get_scores_for_symbol(): {e}", exc_info=True)
        return {}
    finally:
        conn.close()


# =========================================================
# ✅ Compatibiliteit (oude functies blijven bestaan)
# =========================================================
def calculate_macro_scores(data: Dict[str, float]) -> Dict[str, Any]:
    return generate_scores_db("macro", data)

def calculate_technical_scores(data: Dict[str, float]) -> Dict[str, Any]:
    return generate_scores_db("technical", data)

def calculate_market_scores(data: Dict[str, float]) -> Dict[str, Any]:
    # Market gebruikt technical scoreregels
    return generate_scores_db("technical", data)
