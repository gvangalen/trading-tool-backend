import logging
from backend.utils.db import get_db_connection
from backend.utils.scoring_utils import get_score_rule_from_db, normalize_indicator_name

logger = logging.getLogger(__name__)

MARKET_INDICATOR_MAP = {
    "price": "btc_price",
    "change_24h": "btc_change_24h",
    "volume": "btc_volume",
    "price_trend": "price_trend",
    "volatility": "volatility",
    "volume_strength": "volume_strength",
}


def interpret_market_indicator(indicator: str, value: float, user_id: int):
    try:
        normalized = MARKET_INDICATOR_MAP.get(indicator, indicator)
        normalized = normalize_indicator_name(normalized)

        rule = get_score_rule_from_db("market", normalized, value)

        if not rule:
            return {
                "score": 50,
                "trend": "neutral",
                "interpretation": "Geen scoreregel gevonden",
                "action": "–",
            }

        return {
            "score": rule["score"],
            "trend": rule["trend"],
            "interpretation": rule["interpretation"],
            "action": rule["action"],
        }

    except Exception as e:
        logger.error(f"❌ interpret_market_indicator fout: {e}", exc_info=True)
        return None
