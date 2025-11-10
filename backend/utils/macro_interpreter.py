import requests
import logging
from backend.utils.db import get_db_connection
from backend.utils.scoring_utils import calculate_score_from_rules  # blijft als helper

logger = logging.getLogger(__name__)

# =====================================================
# 🌐 Waarde ophalen van macro-indicator
# =====================================================
async def fetch_macro_value(name: str, source: str = None, link: str = None):
    """
    Haalt actuele waarde op van een macro-indicator via de data_url uit de DB.
    Ondersteunt o.a.:
      - Fear & Greed Index (alternative.me)
      - BTC Dominance (CoinGecko)
      - S&P500, VIX, Oil (Yahoo Finance)
      - Interest / Inflation (FRED API)
      - DXY (TradingView, beperkt)
    """
    try:
        if not link:
            logger.warning(f"⚠️ Geen link opgegeven voor '{name}'")
            return None

        logger.info(f"🌐 Ophalen macro indicator '{name}' via {source} → {link}")
        resp = requests.get(link, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        # === 1️⃣ Fear & Greed Index
        if "alternative" in (source or "").lower():
            try:
                v = data.get("data", [{}])[0].get("value")
                if v is not None:
                    return {"value": float(v)}
            except Exception:
                pass

        # === 2️⃣ BTC Dominance (CoinGecko)
        if "coingecko" in (source or "").lower():
            try:
                v = data.get("data", {}).get("market_cap_percentage", {}).get("btc")
                if v is not None:
                    return {"value": float(v)}
            except Exception:
                pass

        # === 3️⃣ Yahoo Finance (S&P500, VIX, Oil)
        if "yahoo" in (source or "").lower():
            try:
                result = data.get("chart", {}).get("result", [{}])[0]
                meta = result.get("meta", {})
                v = meta.get("regularMarketPrice")
                if v is not None:
                    return {"value": float(v)}
            except Exception:
                pass

        # === 4️⃣ FRED API (Interest rate / Inflation)
        if "fred" in (source or "").lower():
            try:
                obs = data.get("observations", [])
                if obs:
                    v = obs[-1].get("value")
                    if v is not None:
                        return {"value": float(v)}
            except Exception:
                pass

        # === 5️⃣ TradingView – geen publieke API
        if "tradingview" in (source or "").lower():
            logger.warning(f"⚠️ TradingView API is niet publiek – geen waarde opgehaald voor '{name}'")
            return None

        # === 6️⃣ Fallbacks
        if isinstance(data, dict):
            if "price" in data:
                return {"value": float(data["price"])}
            if "value" in data:
                return {"value": float(data["value"])}
            if "data" in data and isinstance(data["data"], dict):
                v = data["data"].get("value") or data["data"].get("price")
                if v:
                    return {"value": float(v)}

        if isinstance(data, list) and len(data) > 0:
            first = data[0]
            if isinstance(first, (int, float)):
                return {"value": float(first)}
            if isinstance(first, dict):
                v = first.get("value") or first.get("price")
                if v:
                    return {"value": float(v)}

        logger.warning(f"⚠️ Geen waarde gevonden voor '{name}' in response: {str(data)[:200]}")
        return None

    except Exception as e:
        logger.error(f"❌ Fout bij ophalen macro-waarde voor '{name}': {e}", exc_info=True)
        return None


# =====================================================
# 📊 Interpretatie via DB-scoreregels
# =====================================================
def interpret_macro_indicator(name: str, value: float) -> dict | None:
    """Vertaal macro-indicatorwaarde naar score, trend, interpretatie en actie via DB-regels."""
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
                ORDER BY range_min ASC;
            """, (name,))
            rows = cur.fetchall()

        if not rows:
            logger.warning(f"⚠️ Geen regels gevonden voor macro-indicator '{name}'")
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

        # ✅ Bereken juiste regel via helper
        return calculate_score_from_rules(value, rules)

    except Exception as e:
        logger.error(f"❌ Fout bij interpretatie '{name}': {e}", exc_info=True)
        return None
    finally:
        conn.close()
