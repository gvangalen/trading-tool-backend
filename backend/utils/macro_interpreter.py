import logging
import requests

from backend.utils.scoring_utils import (
    normalize_indicator_name,
    get_score_rule_from_db,
)

logger = logging.getLogger(__name__)

YAHOO_DXY = "https://query1.finance.yahoo.com/v8/finance/chart/%5EDXY"
ALT_FNG = "https://api.alternative.me/fng/?limit=1"


# ============================================================
# 🌐 Macro waarde ophalen
# ============================================================
def fetch_macro_value(name: str, source: str = None, link: str = None):
    """
    Haalt macro waarde op.
    Crasht nooit.
    """

    normalized = normalize_indicator_name(name)
    logger.info(f"🌐 Fetch macro '{normalized}'")

    # ------------------------------------------------------------
    # 🟦 DXY
    # ------------------------------------------------------------
    if normalized == "dxy":
        try:
            r = requests.get(YAHOO_DXY, timeout=10)
            r.raise_for_status()
            data = r.json()

            value = data["chart"]["result"][0]["meta"]["regularMarketPrice"]
            return {"value": float(value)}

        except Exception as e:
            logger.warning(f"DXY Yahoo failed → fallback FNG: {e}")

        # fallback → fear & greed
        try:
            r = requests.get(ALT_FNG, timeout=10)
            r.raise_for_status()
            fg = r.json()
            value = float(fg["data"][0]["value"])
            return {"value": value}
        except Exception:
            return {"value": None}

    # ------------------------------------------------------------
    # 🟩 Fear & Greed
    # ------------------------------------------------------------
    if "alternative" in (source or "").lower():
        try:
            r = requests.get(ALT_FNG, timeout=10)
            r.raise_for_status()
            fg = r.json()
            return {"value": float(fg["data"][0]["value"])}
        except Exception:
            return {"value": None}

    # ------------------------------------------------------------
    # 🟧 BTC Dominance
    # ------------------------------------------------------------
    if normalized in ("btc_dominance", "bitcoin_dominance"):
        try:
            r = requests.get("https://api.coingecko.com/api/v3/global", timeout=10)
            r.raise_for_status()
            data = r.json()
            return {"value": float(data["data"]["market_cap_percentage"]["btc"])}
        except Exception:
            return {"value": None}

    # ------------------------------------------------------------
    # 🟨 Yahoo based indicators
    # ------------------------------------------------------------
    if source and "yahoo" in source.lower() and link:
        try:
            r = requests.get(link, timeout=10)
            r.raise_for_status()
            data = r.json()
            meta = data["chart"]["result"][0]["meta"]
            return {"value": float(meta["regularMarketPrice"])}
        except Exception:
            return {"value": None}

    # ------------------------------------------------------------
    # 🟪 FRED data
    # ------------------------------------------------------------
    if source and "fred" in source.lower() and link:
        try:
            r = requests.get(link, timeout=10)
            r.raise_for_status()
            fred = r.json()
            obs = fred.get("observations", [])
            if obs and obs[-1]["value"] not in ("", ".", None):
                return {"value": float(obs[-1]["value"])}
        except Exception:
            pass
        return {"value": None}

    # ------------------------------------------------------------
    # 🟫 Generic JSON API
    # ------------------------------------------------------------
    if link:
        try:
            r = requests.get(link, timeout=10)
            r.raise_for_status()
            data = r.json()

            for key in ("value", "price", "index"):
                if key in data:
                    return {"value": float(data[key])}

        except Exception as e:
            logger.warning(f"Generic macro fetch fail '{normalized}': {e}")

    return {"value": None}


# ============================================================
# 🧠 Macro interpretatie via DB rules
# ============================================================
def interpret_macro_indicator(name: str, value: float, user_id: int):
    try:
        normalized = normalize_indicator_name(name)

        rule = get_score_rule_from_db("macro", normalized, value)

        if not rule:
            return {
                "score": 10,
                "trend": "neutral",
                "interpretation": "Geen scoreregel match",
                "action": "Geen actie",
            }

        return {
            "score": rule.get("score", 10),
            "trend": rule.get("trend"),
            "interpretation": rule.get("interpretation"),
            "action": rule.get("action"),
        }

    except Exception as e:
        logger.error("interpret_macro_indicator error", exc_info=True)
        return {
            "score": 10,
            "trend": "neutral",
            "interpretation": "Interpretatiefout",
            "action": "Controleer logs",
        }
