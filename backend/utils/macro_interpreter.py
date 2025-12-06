import requests
import logging
from backend.utils.db import get_db_connection
from backend.utils.scoring_utils import calculate_score_from_rules

logger = logging.getLogger(__name__)


# =====================================================
# 🌐 Macro waarde ophalen
# =====================================================
async def fetch_macro_value(name: str, source: str = None, link: str = None):
    try:
        if not link:
            logger.warning(f"⚠️ Geen link opgegeven voor '{name}'")
            return None

        logger.info(f"🌐 Ophalen macro indicator '{name}' via {source} → {link}")
        resp = requests.get(link, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        # === Fear & Greed Index
        if "alternative" in (source or "").lower():
            try:
                v = data.get("data", [{}])[0].get("value")
                if v is not None:
                    return {"value": float(v)}
            except Exception:
                pass

        # === BTC Dominance (CoinGecko)
        if "coingecko" in (source or "").lower():
            try:
                v = data.get("data", {}).get("market_cap_percentage", {}).get("btc")
                if v is not None:
                    return {"value": float(v)}
            except Exception:
                pass

        # === Yahoo Finance: S&P500, VIX, Oil
        if "yahoo" in (source or "").lower():
            try:
                result = data.get("chart", {}).get("result", [{}])[0]
                meta = result.get("meta", {})
                v = meta.get("regularMarketPrice")
                if v is not None:
                    return {"value": float(v)}
            except Exception:
                pass

        # === FRED: Interest / Inflation
        if "fred" in (source or "").lower():
            try:
                obs = data.get("observations", [])
                if obs:
                    v = obs[-1].get("value")
                    if v is not None:
                        return {"value": float(v)}
            except Exception:
                pass

        # === TradingView (geen API)
        if "tradingview" in (source or "").lower():
            logger.warning(f"⚠️ TradingView API niet publiek → '{name}' niet opgehaald.")
            return None

        # === Generic fallback
        if isinstance(data, dict):
            for key in ["value", "price"]:
                if key in data:
                    return {"value": float(data[key])}

        logger.warning(f"⚠️ Geen waarde gevonden voor macro '{name}': {str(data)[:200]}")
        return None

    except Exception as e:
        logger.error(f"❌ Fout bij ophalen macro-waarde voor '{name}': {e}", exc_info=True)
        return None


# =====================================================
# 🧠 Macro interpretatie via DB (per user!)
# =====================================================
def interpret_macro_indicator(name: str, value: float, user_id: int) -> dict | None:
    """
    Score, trend, interpretatie, actie via macro_indicator_rules per gebruiker.
    """

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding bij interpretatie macro-indicator.")
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
                f"⚠️ Geen scoreregels gevonden voor macro-indicator '{name}' (user_id={user_id})"
            )
            return None

        rules = [
            {
                "range_min": r[0],
                "range_max": r[1],
                "score": r[2],
                "trend": r[3],
                "interpretation": r[4],
                "action": r[5]
            }
            for r in rows
        ]

        # Score bepalen via helper
        return calculate_score_from_rules(value, rules)

    except Exception as e:
        logger.error(f"❌ Fout bij interpretatie macro '{name}': {e}", exc_info=True)
        return None

    finally:
        conn.close()
