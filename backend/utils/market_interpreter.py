import logging
from backend.utils.db import get_db_connection
from backend.utils.scoring_utils import calculate_score_from_rules  # ✅ centrale scorefunctie

# ✅ Logging instellen
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# =========================================================
# 🎯 Scoreregels ophalen
# =========================================================
def get_rules_from_db(indicator_name: str) -> list[dict]:
    """
    Haalt ALLE scoreregels voor een indicator op uit de DB.
    Wordt daarna door scoring_utils gebruikt om de juiste te vinden.
    """
    conn = get_db_connection()
    rules = []
    if not conn:
        logger.error("❌ Geen DB-verbinding bij ophalen scoreregels.")
        return rules

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT range_min, range_max, score, trend, interpretation, action
                FROM technical_indicator_rules
                WHERE indicator = %s
                ORDER BY range_min ASC
            """, (indicator_name,))
            rows = cur.fetchall()

        for r in rows:
            rules.append({
                "range_min": r[0],
                "range_max": r[1],
                "score": r[2],
                "trend": r[3],
                "interpretation": r[4],
                "action": r[5],
            })

        if not rules:
            logger.warning(f"⚠️ Geen scoreregels gevonden voor '{indicator_name}'")

    except Exception as e:
        logger.error(f"❌ Fout bij ophalen scoreregels ({indicator_name}): {e}", exc_info=True)
    finally:
        conn.close()

    return rules


# =========================================================
# 💹 Verwerking van één market indicator
# =========================================================
def process_market_indicator(name: str, value: float) -> dict | None:
    """
    Verwerkt één market indicator (zoals prijs, volume, change_24h)
    via scoreregels uit de database + centrale scorefunctie.
    """
    try:
        if value is None:
            raise ValueError("Waarde is None")

        rules = get_rules_from_db(name)
        if not rules:
            return None

        score_data = calculate_score_from_rules(value, rules)
        if not score_data:
            logger.warning(f"⚠️ Geen score berekend voor '{name}' (waarde={value})")
            return None

        result = {
            "name": name,
            "value": value,
            "score": score_data["score"],
            "trend": score_data["trend"],
            "interpretation": score_data["interpretation"],
            "action": score_data["action"],
        }

        logger.info(f"✅ {name}: {value} → {result['trend']} (score {result['score']})")
        return result

    except Exception as e:
        logger.error(f"❌ Fout bij verwerken market indicator '{name}': {e}", exc_info=True)
        return None


# =========================================================
# 📊 Verwerking van alle market data
# =========================================================
def process_all_market(data: dict) -> dict:
    """
    ➤ Verwerkt alle market indicatoren via scoreregels + scoring_utils.
      Voorbeeld input:
      {"price": 102345.2, "volume": 91000000000, "change_24h": 0.35}
    """
    if not data:
        logger.warning("⚠️ Geen inputdata voor market verwerking.")
        return {}

    results = {}
    for name, value in data.items():
        result = process_market_indicator(name, value)
        if result:
            results[name] = result

    logger.info(f"✅ {len(results)} market indicatoren verwerkt.")
    return results
