import logging
from backend.utils.db import get_db_connection
from backend.utils.scoring_utils import calculate_score_from_rules  # ✅ zelfde logica voor technische & market

# ✅ Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# =========================================================
# 🎯 Haal scoreregels op uit database
# =========================================================
def get_rules_from_db(indicator_name: str) -> list[dict]:
    """
    Haalt ALLE scoreregels voor een technische indicator op uit de database.
    """
    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding bij ophalen scoreregels.")
        return []

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
            logger.warning(f"⚠️ Geen scoreregels gevonden voor indicator '{indicator_name}'")
            return []

        return [
            {
                "range_min": r[0],
                "range_max": r[1],
                "score": r[2],
                "trend": r[3],
                "interpretation": r[4],
                "action": r[5],
            }
            for r in rows
        ]

    except Exception as e:
        logger.error(f"❌ Fout bij ophalen scoreregels voor '{indicator_name}': {e}", exc_info=True)
        return []
    finally:
        conn.close()


# =========================================================
# ⚙️ Verwerk één technische indicator
# =========================================================
def process_technical_indicator(name: str, value: float) -> dict | None:
    """
    Verwerkt één technische indicator door scoreregels + scoring_utils toe te passen.
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
        logger.error(f"❌ Fout bij verwerken indicator '{name}': {e}", exc_info=True)
        return None


# =========================================================
# 📊 Verwerk alle technische indicatoren
# =========================================================
def process_all_technical(data: dict) -> dict:
    """
    ➤ Verwerkt alle technische indicatoren via scoreregels uit DB + scoring_utils.
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
