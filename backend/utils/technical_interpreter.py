import logging
from backend.utils.db import get_db_connection
from backend.utils.scoring_utils import calculate_score_from_rules

logger = logging.getLogger(__name__)

def interpret_technical_indicator(name: str, value: float) -> dict | None:
    """Vertaal technische indicatorwaarde (RSI, MA200, volume, etc.) via scoreregels in DB."""
    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding bij interpretatie technische indicator.")
        return None

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT range_min, range_max, score, trend, interpretation, action
                FROM technical_indicator_rules
                WHERE indicator = %s
                ORDER BY range_min ASC
            """, (name,))
            rows = cur.fetchall()

        if not rows:
            logger.warning(f"⚠️ Geen regels gevonden voor technische indicator '{name}'")
            return None

        rules = [
            {"range_min": r[0], "range_max": r[1], "score": r[2],
             "trend": r[3], "interpretation": r[4], "action": r[5]}
            for r in rows
        ]
        return calculate_score_from_rules(value, rules)

    except Exception as e:
        logger.error(f"❌ Fout bij technische interpretatie '{name}': {e}", exc_info=True)
        return None
    finally:
        conn.close()
