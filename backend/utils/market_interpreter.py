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


# =========================================================
# 🔹 Normalisatie naar 0–100 (Optie B)
# =========================================================
def normalize_market_value(indicator: str, value: float) -> float:
    """
    Zet raw market value om naar genormaliseerde 0–100 schaal.
    Alles wordt uniform en UX-proof.
    """

    try:
        if value is None:
            return 0

        value = float(value)

        # -------------------------------------------------
        # BTC Volume (% afwijking t.o.v. 30d gemiddelde)
        # -------------------------------------------------
        if indicator == "btc_volume":
            abs_dev = abs(value)
            cap = 80  # 80% afwijking = extreem
            return min(100, (abs_dev / cap) * 100)

        # -------------------------------------------------
        # 24h price change (%)
        # -------------------------------------------------
        if indicator == "btc_change_24h":
            abs_dev = abs(value)
            cap = 20  # 20% daily move = extreem
            return min(100, (abs_dev / cap) * 100)

        # -------------------------------------------------
        # Volatility (%)
        # -------------------------------------------------
        if indicator == "volatility":
            abs_dev = abs(value)
            cap = 15
            return min(100, (abs_dev / cap) * 100)

        # -------------------------------------------------
        # Price trend / volume strength (als al 0–100)
        # -------------------------------------------------
        if indicator in ["price_trend", "volume_strength"]:
            return max(0, min(100, value))

        # -------------------------------------------------
        # Fallback
        # -------------------------------------------------
        return max(0, min(100, value))

    except Exception:
        logger.error("❌ Normalisatie fout", exc_info=True)
        return 0


# =========================================================
# 🔹 Market Interpreter
# =========================================================
def interpret_market_indicator(indicator: str, value: float, user_id: int):
    """
    Interpreteert market indicator via centrale DB scoreregels.
    Flow:
    raw_value → normalized_value (0–100) → DB rules → score
    """

    try:
        if value is None:
            return _fallback("Geen waarde beschikbaar")

        # 🔹 veilig normaliseren
        indicator = (indicator or "").strip().lower()

        mapped = MARKET_INDICATOR_MAP.get(indicator, indicator)
        normalized_name = normalize_indicator_name(mapped)

        logger.debug(
            f"Market interpret → {indicator} → {normalized_name} raw={value}"
        )

        # 🔥 NIEUW: eerst normaliseren naar 0–100
        normalized_value = normalize_market_value(normalized_name, value)

        logger.debug(
            f"Normalized value (0–100): {normalized_value}"
        )

        # 🔹 daarna rule lookup
        rule = get_score_rule_from_db(
            "market",
            normalized_name,
            normalized_value,
        )

        if not rule:
            logger.warning(
                f"⚠️ Geen rule match → {normalized_name} value={normalized_value}"
            )
            return _fallback("Geen scoreregel beschikbaar")

        return {
            "score": max(0, min(100, rule.get("score", 10))),
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
