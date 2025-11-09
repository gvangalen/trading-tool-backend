import logging
from backend.utils.db import get_db_connection

# ✅ Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# =========================================================
# 🎯 Helper: scoreregel ophalen uit database
# =========================================================
def get_score_rule_from_db(indicator_name: str, value: float) -> dict | None:
    """
    Haalt passende scoreregel op uit de DB voor een technische indicator.
    """
    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding bij ophalen scoreregel.")
        return None

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT range_min, range_max, score, trend, interpretation, action
                FROM technical_indicator_rules
                WHERE indicator = %s
                ORDER BY range_min ASC
            """, (indicator_name,))
            rows = cur.fetchall()

        if not rows:
            logger.warning(f"⚠️ Geen scoreregels gevonden voor {indicator_name}")
            return None

        for r in rows:
            if r[0] <= value <= r[1]:
                return {
                    "score": r[2],
                    "trend": r[3],
                    "interpretation": r[4],
                    "action": r[5],
                }

        logger.info(f"ℹ️ Waarde {value} valt buiten alle ranges voor {indicator_name}")
        return None

    except Exception as e:
        logger.error(f"❌ Fout in get_score_rule_from_db({indicator_name}): {e}", exc_info=True)
        return None
    finally:
        conn.close()


# =========================================================
# 🔢 Verwerking van één indicator
# =========================================================
def process_technical_indicator(name: str, value: float) -> dict | None:
    """
    Verwerkt één technische indicator via database-scoreregels.
    """
    try:
        if value is None:
            raise ValueError("Waarde is None")

        rule = get_score_rule_from_db(name, value)
        if not rule:
            logger.warning(f"⚠️ Geen passende scoreregel gevonden voor {name}")
            return None

        result = {
            "name": name,
            "value": value,
            "score": round(rule.get("score", 10)),
            "trend": rule.get("trend", "Onbekend"),
            "interpretation": rule.get("interpretation", ""),
            "action": rule.get("action", ""),
        }

        logger.info(f"✅ {name}: {value} → {result['trend']} (score {result['score']})")
        return result

    except Exception as e:
        logger.error(f"❌ Fout bij verwerken indicator '{name}': {e}", exc_info=True)
        return None


# =========================================================
# ⚙️ Verwerking van alle indicatoren
# =========================================================
def process_all_technical(data: dict) -> dict:
    """
    ➤ Verwerkt alle technische indicatoren via scoreregels uit DB.
      Voorbeeld input:
      {"rsi": 44.1, "volume": 380000000, "ma_200": 0.94}
    """
    if not data:
        logger.warning("⚠️ Geen inputdata voor technische verwerking.")
        return {}

    results = {}
    for name, value in data.items():
        result = process_technical_indicator(name, value)
        if result:
            results[name] = result

    logger.info(f"✅ {len(results)} technische indicatoren verwerkt.")
    return results
