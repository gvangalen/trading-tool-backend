import logging
from backend.utils.scoring_utils import (
    get_score_rule_from_db,
    normalize_indicator_name,
)

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
    """
    Interpreteert market indicator via centrale scoring engine.
    """

    try:
        # 🔹 normalize naam
        normalized = MARKET_INDICATOR_MAP.get(indicator, indicator)
        normalized = normalize_indicator_name(normalized)

        result = get_score_rule_from_db("market", normalized, value)

        if not result:
            # extreme fallback (zou niet moeten gebeuren)
            return {
                "score": 10,
                "trend": "neutral",
                "interpretation": "Geen scoreregel beschikbaar",
                "action": "Geen actie",
            }

        return {
            "score": result.get("score", 10),
            "trend": result.get("trend") or "neutral",
            "interpretation": result.get("interpretation")
                or "Geen interpretatie beschikbaar",
            "action": result.get("action") or "Geen actie",
        }

    except Exception as e:
        logger.error("❌ interpret_market_indicator fout", exc_info=True)
        return {
            "score": 10,
            "trend": "neutral",
            "interpretation": "Interpretatie fout",
            "action": "Controleer logs",
        }
