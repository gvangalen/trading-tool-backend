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
    Interpreteert market indicator via centrale DB scoreregels.
    Crasht nooit.
    """

    try:
        if value is None:
            return _fallback("Geen waarde beschikbaar")

        # 🔹 veilig normaliseren
        indicator = (indicator or "").strip().lower()

        mapped = MARKET_INDICATOR_MAP.get(indicator, indicator)
        normalized = normalize_indicator_name(mapped)

        logger.debug(f"Market interpret → {indicator} → {normalized} = {value}")

        rule = get_score_rule_from_db("market", normalized, value)

        if not rule:
            logger.warning(
                f"⚠️ Geen rule match → {normalized} value={value}"
            )
            return _fallback("Geen scoreregel beschikbaar")

        return {
            "score": rule.get("score", 10),
            "trend": rule.get("trend") or "neutral",
            "interpretation": rule.get("interpretation")
                or "Geen interpretatie beschikbaar",
            "action": rule.get("action") or "Geen actie",
        }

    except Exception:
        logger.error("❌ interpret_market_indicator fout", exc_info=True)
        return _fallback("Interpretatiefout")


# =========================================================
# fallback helper
# =========================================================
def _fallback(reason: str):
    return {
        "score": 10,
        "trend": "neutral",
        "interpretation": reason,
        "action": "Geen actie",
    }
