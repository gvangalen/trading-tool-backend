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
    Haalt de macro waarde op met multilevel fallback:
    1) Yahoo (DXY)
    2) Alternative.me fallback
    3) Neutrale fallback waarde 50
    Werkt voor ALLE macro's zonder crash.
    """

    normalized = normalize_indicator_name(name)
    logger.info(f"🌐 Fetch macro '{normalized}' via {source} → {link}")

    # ------------------------------------------------------------
    # 🟦 1) SPECIALE CASE — DXY MET FALLBACKS
    # ------------------------------------------------------------
    if normalized == "dxy":

        # ----- Try Yahoo -----
        try:
            resp = requests.get(YAHOO_DXY, timeout=10)
            resp.raise_for_status()
            data = resp.json()

            value = data["chart"]["result"][0]["meta"]["regularMarketPrice"]
            logger.info(f"✔️ DXY Yahoo fetched: {value}")
            return {"value": float(value)}

        except Exception as e:
            logger.error(f"❌ DXY Yahoo error: {e}")

        # ----- Fallback: Alternative.me Fear&Greed -----
        try:
            resp = requests.get(ALT_FNG, timeout=10)
            resp.raise_for_status()
            fg = resp.json()

            value = float(fg["data"][0]["value"])
            logger.warning(f"⚠️ DXY FALLBACK gebruikt → Alternative.me: {value}")
            return {"value": value}

        except Exception as e2:
            logger.error(f"❌ DXY fallback ook gefaald: {e2}")

        # ----- Veiligste fallback -----
        logger.warning("⚠️ DXY → harde fallback 50 gebruikt")
        return {"value": 50.0}

    # ------------------------------------------------------------
    # 🟩 2) FEAR & GREED INDEX
    # ------------------------------------------------------------
    if "alternative" in (source or "").lower():
        try:
            v = data["data"][0]["value"]
            return {"value": float(v)}
        except Exception:
            logger.warning(f"⚠️ Fear & Greed parse error voor '{normalized}'")

    # ------------------------------------------------------------
    # 🟧 3) BTC DOMINANCE (Coingecko)
    # ------------------------------------------------------------
    if normalized in ["btc_dominance", "bitcoin_dominance"]:
        try:
            resp = requests.get("https://api.coingecko.com/api/v3/global", timeout=10)
            resp.raise_for_status()
            data = resp.json()

            dom = data["data"]["market_cap_percentage"]["btc"]
            return {"value": float(dom)}
        except Exception as e:
            logger.error(f"❌ BTC dominance fetch error: {e}")
            return {"value": 50}

    # ------------------------------------------------------------
    # 🟨 4) ANDERE YAHOO (VIX, SP500 etc.)
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
            return {"value": 50}

    # ------------------------------------------------------------
    # 🟪 5) FRED DATA
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

        return {"value": 50}

    # ------------------------------------------------------------
    # 🟫 6) GENERIC JSON API
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
    Gebruikt DB scoreregels om een macrowaarde te interpreteren.
    Crasht nooit.
    """

    try:
        normalized = normalize_indicator_name(name)
        rule = get_score_rule_from_db("macro", normalized, value)

        if not rule:
            logger.warning(
                f"⚠️ Geen scoreregels voor '{normalized}' (value={value})"
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
        logger.error(f"❌ interpret_macro_indicator error: {e}", exc_info=True)
        return {
            "score": 50,
            "trend": "neutral",
            "interpretation": "Fout bij interpretatie",
            "action": "–",
        }
