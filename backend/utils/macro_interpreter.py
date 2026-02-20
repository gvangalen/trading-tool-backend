import logging
import requests

from backend.utils.scoring_utils import (
    normalize_indicator_name,
    get_score_rule_from_db,
)

logger = logging.getLogger(__name__)

# ------------------------------------------------------------
# 📌 URLs
# ------------------------------------------------------------
YAHOO_DXY = "https://query1.finance.yahoo.com/v8/finance/chart/%5EDXY"
ALT_FNG = "https://api.alternative.me/fng/?limit=1"


# ============================================================
# 🌐 Macro waarde ophalen met volledige fallback logica
# ============================================================
def fetch_macro_value(name: str, source: str = None, link: str = None):
    """
    Haalt macro waarde op met veilige fallback logica.
    Crasht nooit.

    Fallback volgorde:
    1) primaire bron
    2) alternatieve bron (indien logisch)
    3) neutrale fallback waarde (50)
    """

    normalized = normalize_indicator_name(name)
    logger.info(f"🌐 Fetch macro '{normalized}' via {source} → {link}")

    # ------------------------------------------------------------
    # 🟦 1) DXY (Dollar Index)
    # ------------------------------------------------------------
    if normalized == "dxy":
        try:
            resp = requests.get(YAHOO_DXY, timeout=10)
            resp.raise_for_status()
            data = resp.json()

            value = data["chart"]["result"][0]["meta"]["regularMarketPrice"]
            logger.info(f"✔️ DXY fetched: {value}")
            return {"value": float(value)}

        except Exception as e:
            logger.error(f"❌ DXY fetch error: {e}")

        # veilige fallback
        logger.warning("⚠️ DXY fallback → neutrale waarde gebruikt")
        return {"value": 50.0}

    # ------------------------------------------------------------
    # 🟩 2) Fear & Greed Index
    # ------------------------------------------------------------
    if "alternative" in (source or "").lower():
        try:
            resp = requests.get(link or ALT_FNG, timeout=10)
            resp.raise_for_status()
            data = resp.json()

            value = float(data["data"][0]["value"])
            return {"value": value}

        except Exception as e:
            logger.warning(f"⚠️ Fear & Greed fetch error: {e}")
            return {"value": 50.0}

    # ------------------------------------------------------------
    # 🟧 3) BTC Dominance
    # ------------------------------------------------------------
    if normalized in ["btc_dominance", "bitcoin_dominance"]:
        try:
            resp = requests.get(
                "https://api.coingecko.com/api/v3/global",
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json()

            dom = data["data"]["market_cap_percentage"]["btc"]
            return {"value": float(dom)}

        except Exception as e:
            logger.error(f"❌ BTC dominance fetch error: {e}")
            return {"value": 50.0}

    # ------------------------------------------------------------
    # 🟨 4) Yahoo Finance indicators (VIX, SP500 etc.)
    # ------------------------------------------------------------
    if "yahoo" in (source or "").lower():
        try:
            resp = requests.get(link, timeout=10)
            resp.raise_for_status()
            data = resp.json()

            meta = data["chart"]["result"][0]["meta"]
            return {"value": float(meta["regularMarketPrice"])}

        except Exception as e:
            logger.warning(f"⚠️ Yahoo parse error '{normalized}': {e}")
            return {"value": 50.0}

    # ------------------------------------------------------------
    # 🟪 5) FRED (macro data)
    # ------------------------------------------------------------
    if "fred" in (source or "").lower():
        try:
            resp = requests.get(link, timeout=10)
            resp.raise_for_status()
            fred = resp.json()

            obs = fred.get("observations", [])
            if obs and obs[-1].get("value") not in ["", ".", None]:
                return {"value": float(obs[-1]["value"])}

        except Exception as e:
            logger.warning(f"⚠️ FRED parse error '{normalized}': {e}")

        return {"value": 50.0}

    # ------------------------------------------------------------
    # 🟫 6) Generic JSON API
    # ------------------------------------------------------------
    try:
        resp = requests.get(link, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        for key in ["value", "price", "index"]:
            if key in data:
                return {"value": float(data[key])}

        logger.warning(f"⚠️ Generic API geen value veld: {data}")

    except Exception as e:
        logger.error(f"❌ Generic macro fetch error '{normalized}': {e}")

    # ------------------------------------------------------------
    # 🟥 7) ALTIJD VEILIGE DEFAULT
    # ------------------------------------------------------------
    return {"value": 50.0}


# ============================================================
# 🧠 Macro interpretatie — regels uit database
# ============================================================
def interpret_macro_indicator(name: str, value: float, user_id: int):
    """
    Interpreteert macro waarde via DB scoreregels.
    Crasht nooit.
    """

    try:
        normalized = normalize_indicator_name(name)

        result = get_score_rule_from_db("macro", normalized, value)

        return {
            "score": result.get("score", 10),
            "trend": result.get("trend") or "neutral",
            "interpretation": result.get("interpretation")
                or "Geen interpretatie beschikbaar",
            "action": result.get("action") or "Geen actie",
        }

    except Exception as e:
        logger.error("❌ interpret_macro_indicator error", exc_info=True)

        return {
            "score": 10,
            "trend": "neutral",
            "interpretation": "Interpretatiefout",
            "action": "Controleer logs",
        }
