import logging
import requests

from backend.utils.db import get_db_connection
from backend.utils.scoring_utils import select_rule_for_value  # ✅ Nieuwe engine

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
        logger.warning(f"⚠️ Geen link opgegeven voor macro-indicator '{name}'")
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
            logger.warning(f"⚠️ Fout bij parse van CoinGecko BTC dominance voor '{name}'")

    # ------------------------------------------------------------
    # 3) Yahoo Finance (S&P500, VIX, etc.)
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
    # 5) Fallback: neem 'value' key of herkenbare keys
    # ------------------------------------------------------------
    if isinstance(data, dict):
        for key in ["value", "price", "index"]:
            if key in data:
                try:
                    return {"value": float(data[key])}
                except Exception:
                    pass

    logger.warning(f"⚠️ Onbekend macroformaat voor '{name}': {str(data)[:200]}")
    return None


# ============================================================
# 🧠 Macro indicator interpretatie via DB-regels (per user)
# ============================================================
def interpret_macro_indicator(name: str, value: float, user_id: int):
    """
    Vertaalt een ruwe macrowaarde naar:
    - score (10–100)
    - trend
    - interpretation
    - action

    Op basis van user-specifieke regels in:
    ▶ macro_indicator_rules (indicator, range_min, range_max, score, trend, interpretation, action)
    """

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding bij interpret_macro_indicator")
        return None

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT range_min, range_max, score, trend, interpretation, action
                FROM macro_indicator_rules
                WHERE indicator = %s
                  AND user_id = %s
                ORDER BY range_min ASC;
            """, (name, user_id))

            rows = cur.fetchall()

        if not rows:
            logger.warning(
                f"⚠️ Geen macro rules gevonden voor '{name}' (user_id={user_id})"
            )
            return None

        # Bouw scoreregels
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

        # Selecteer passende regel
        rule = select_rule_for_value(value, rules)
        if not rule:
            logger.warning(
                f"⚠️ Geen passende macro rule voor '{name}' (value={value}, user_id={user_id})"
            )
            return None

        return {
            "score": rule["score"],
            "trend": rule["trend"],
            "interpretation": rule["interpretation"],
            "action": rule["action"],
        }

    except Exception as e:
        logger.error(f"❌ Macro interpret error voor '{name}': {e}", exc_info=True)
        return None

    finally:
        conn.close()
