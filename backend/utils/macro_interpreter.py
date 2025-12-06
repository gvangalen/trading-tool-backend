import logging
import requests

from backend.utils.db import get_db_connection
from backend.utils.scoring_utils import (
    normalize_indicator_name,
    get_score_rule_from_db,   # ✅ BESTAAT WEL — jouw echte engine
)

logger = logging.getLogger(__name__)


# ============================================================
# 🌐 Macro waarde ophalen via externe API
# ============================================================
def fetch_macro_value(name: str, source: str = None, link: str = None):
    """
    Haalt de ruwe waarde op van een macro-indicator.
    Consistente logica voor alle macro-indicatoren.
    """

    if not link:
        logger.warning(f"⚠️ Geen link voor macro-indicator '{name}'")
        return None

    logger.info(f"🌐 Fetch macro '{name}' via {source} → {link}")

    try:
        resp = requests.get(link, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.error(f"❌ Macro fetch error voor '{name}': {e}", exc_info=True)
        return None

    source = (source or "").lower()

    # ------------------------------------------------------------
    # 1) Fear & Greed Index (Alternative.me)
    # ------------------------------------------------------------
    if "alternative" in source or "feargreed" in link.lower():
        try:
            v = data["data"][0]["value"]
            return {"value": float(v)}
        except Exception:
            logger.warning(f"⚠️ Fear&Greed parse error voor '{name}'")

    # ------------------------------------------------------------
    # 2) CoinGecko BTC Dominance
    # ------------------------------------------------------------
    if "coingecko" in source:
        try:
            v = data["data"]["market_cap_percentage"]["btc"]
            return {"value": float(v)}
        except Exception:
            logger.warning(f"⚠️ CoinGecko dominance parse error voor '{name}'")

    # ------------------------------------------------------------
    # 3) Yahoo Finance (S&P500, VIX, DXY...)
    # ------------------------------------------------------------
    if "yahoo" in source:
        try:
            result = data["chart"]["result"][0]
            meta = result["meta"]
            return {"value": float(meta["regularMarketPrice"])}
        except Exception:
            logger.warning(f"⚠️ Yahoo Finance parse error voor '{name}'")

    # ------------------------------------------------------------
    # 4) FRED macro data (inflatie, rente)
    # ------------------------------------------------------------
    if "fred" in source:
        try:
            obs = data.get("observations", [])
            if obs:
                v = obs[-1].get("value")
                if v not in [None, ".", ""]:
                    return {"value": float(v)}
        except Exception:
            logger.warning(f"⚠️ FRED parse error voor '{name}'")

    # ------------------------------------------------------------
    # 5) GENERIC fallback
    # ------------------------------------------------------------
    if isinstance(data, dict):
        for key in ["value", "price", "index"]:
            if key in data:
                try:
                    return {"value": float(data[key])}
                except:
                    pass

    logger.warning(f"⚠️ Onbekend macroformaat voor '{name}': {str(data)[:200]}")
    return None


# ============================================================
# 🧠 Macro interpretatie — DB-regels via jouw echte engine
# ============================================================
def interpret_macro_indicator(name: str, value: float, user_id: int):
    """
    Vertaalt een ruwe macrowaarde naar scoreregels (per user).
    Gebruikt jouw echte engine get_score_rule_from_db().
    """

    try:
        # Normaliseer voor consistentie
        normalized = normalize_indicator_name(name)

        # Jouw echte scoring-engine
        rule = get_score_rule_from_db("macro", normalized, value)

        if not rule:
            logger.warning(
                f"⚠️ Geen macro rule match voor '{normalized}' (value={value}, user_id={user_id})"
            )
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
        logger.error(f"❌ Macro interpretatiefout voor '{name}': {e}", exc_info=True)
        return None
