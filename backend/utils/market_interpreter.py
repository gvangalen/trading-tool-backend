import logging
from backend.utils.db import get_db_connection
from backend.utils.scoring_utils import select_rule_for_value  # ✅ nieuwe engine

logger = logging.getLogger(__name__)

# ============================================================
# 🔄 Mapping API-namen → DB-indicatornamen
# ============================================================
MARKET_INDICATOR_MAP = {
    "price": "btc_price",
    "change_24h": "btc_change_24h",
    "volume": "btc_volume",
    "price_trend": "price_trend",
    "volatility": "volatility",
    "volume_strength": "volume_strength",
}


# ============================================================
# 🧠 MARKET INDICATOR INTERPRETATIE VIA DB
# ============================================================
def interpret_market_indicator(indicator: str, value: float, user_id: int):
    """
    Interpreteert een market-indicator (per user):
    - haalt regels uit market_indicator_rules
    - bepaalt juiste regel via select_rule_for_value()
    - retourneert {score, trend, interpretation, action}
    """

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding bij interpret_market_indicator")
        return None

    try:
        # Zet API-naam om naar DB-indicatornaam
        name = MARKET_INDICATOR_MAP.get(indicator, indicator)

        with conn.cursor() as cur:
            cur.execute("""
                SELECT range_min, range_max, score, trend, interpretation, action
                FROM market_indicator_rules
                WHERE indicator = %s
                  AND user_id = %s
                ORDER BY range_min ASC
            """, (name, user_id))

            rows = cur.fetchall()

        if not rows:
            logger.warning(
                f"⚠️ Geen market rules gevonden voor '{name}' (user_id={user_id})"
            )
            return None

        # Scoreregels structureren
        rules = [
            {
                "range_min": float(r[0]),
                "range_max": float(r[1]),
                "score": int(r[2]),
                "trend": r[3],
                "interpretation": r[4],
                "action": r[5],
            }
            for r in rows
        ]

        # Selecteer juiste scoreregel
        rule = select_rule_for_value(value, rules)
        if not rule:
            logger.warning(
                f"⚠️ Geen passende regel voor '{name}' (value={value}, user_id={user_id})"
            )
            return None

        return {
            "score": rule["score"],
            "trend": rule["trend"],
            "interpretation": rule["interpretation"],
            "action": rule["action"],
        }

    except Exception as e:
        logger.error(f"❌ Market interpretatie error voor '{indicator}': {e}", exc_info=True)
        return None

    finally:
        conn.close()
