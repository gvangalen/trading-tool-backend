import logging
from backend.utils.db import get_db_connection
from backend.utils.scoring_utils import calculate_score_from_rules

logger = logging.getLogger(__name__)

# 🔄 Mapping zodat API-namen matchen met indicatorregels
MARKET_INDICATOR_MAP = {
    "price": "btc_price",
    "change_24h": "btc_change_24h",
    "volume": "btc_volume",

    # extra market indicatoren die eventueel uit andere tasks komen:
    "price_trend": "price_trend",
    "volatility": "volatility",
    "volume_strength": "volume_strength",
}


def interpret_market_indicator(name: str, value: float) -> dict | None:
    """Vertaal market-indicatorwaarde via scoreregels in DB."""
    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding bij interpretatie market-indicator.")
        return None

    try:
        # 🔄 Gebruik mapping
        indicator_name = MARKET_INDICATOR_MAP.get(name, name)

        with conn.cursor() as cur:
            cur.execute("""
                SELECT range_min, range_max, score, trend, interpretation, action
                FROM market_indicator_rules
                WHERE indicator = %s
                ORDER BY range_min ASC
            """, (indicator_name,))
            rows = cur.fetchall()

        if not rows:
            logger.warning(f"⚠️ Geen market rules voor indicator '{indicator_name}'")
            return None

        rules = [
            {
                "range_min": r[0],
                "range_max": r[1],
                "score": r[2],
                "trend": r[3],
                "interpretation": r[4],
                "action": r[5]
            }
            for r in rows
        ]

        return calculate_score_from_rules(value, rules)

    except Exception as e:
        logger.error(f"❌ Fout bij market-interpretatie '{name}': {e}", exc_info=True)
        return None
    finally:
        conn.close()
