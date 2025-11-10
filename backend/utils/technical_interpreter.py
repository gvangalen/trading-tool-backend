import requests
import logging
from backend.utils.db import get_db_connection
from backend.utils.scoring_utils import calculate_score_from_rules

logger = logging.getLogger(__name__)

# =====================================================
# 🌐 Nieuwe functie: fetch_technical_value
# =====================================================
async def fetch_technical_value(name: str, source: str = None, link: str = None):
    """
    🔍 Haalt actuele waarde op van een technische indicator via data_url in DB.
    Ondersteunt basisindicatoren zoals RSI, MA200, Volume, etc.
    Geeft altijd {"value": float(...)} terug.
    """
    try:
        if not link:
            logger.warning(f"⚠️ Geen data_url voor technische indicator '{name}'")
            return None

        logger.info(f"🌐 Ophalen technische indicator '{name}' via {source} -> {link}")
        resp = requests.get(link, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        # === 1️⃣ RSI (TradingView of API met 'value')
        if "rsi" in name.lower():
            if isinstance(data, dict):
                v = data.get("value") or data.get("rsi")
                if v:
                    return {"value": float(v)}
            if isinstance(data, list) and len(data) > 0:
                first = data[-1] if isinstance(data[-1], dict) else data[0]
                if isinstance(first, dict):
                    v = first.get("rsi") or first.get("value")
                    if v:
                        return {"value": float(v)}

        # === 2️⃣ MA (Moving Average)
        if "ma" in name.lower() or "moving_average" in name.lower():
            if isinstance(data, dict):
                v = data.get("value") or data.get("ma") or data.get("ma200")
                if v:
                    return {"value": float(v)}

        # === 3️⃣ Volume
        if "volume" in name.lower():
            if isinstance(data, dict):
                v = data.get("volume") or data.get("value")
                if v:
                    return {"value": float(v)}

        # === 4️⃣ Fallback – algemene extractie
        if isinstance(data, dict):
            for key in ["value", "score", "price", "close"]:
                if key in data and isinstance(data[key], (int, float)):
                    return {"value": float(data[key])}

        if isinstance(data, list) and len(data) > 0:
            first = data[-1]
            if isinstance(first, (int, float)):
                return {"value": float(first)}
            if isinstance(first, dict):
                v = first.get("value") or first.get("price") or first.get("close")
                if v:
                    return {"value": float(v)}

        logger.warning(f"⚠️ Geen waarde gevonden voor technische indicator '{name}' in response: {str(data)[:200]}")
        return None

    except Exception as e:
        logger.error(f"❌ Fout bij ophalen technische waarde voor '{name}': {e}", exc_info=True)
        return None


# =====================================================
# 📊 Bestaande functie: interpret_technical_indicator
# =====================================================
def interpret_technical_indicator(name: str, value: float) -> dict | None:
    """Vertaal technische indicatorwaarde (RSI, MA200, volume, etc.) via scoreregels in DB."""
    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding bij interpretatie technische indicator.")
        return None

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT range_min, range_max, score, trend, interpretation, action
                FROM technical_indicator_rules
                WHERE indicator = %s
                ORDER BY range_min ASC
            """, (name,))
            rows = cur.fetchall()

        if not rows:
            logger.warning(f"⚠️ Geen regels gevonden voor technische indicator '{name}'")
            return None

        rules = [
            {"range_min": r[0], "range_max": r[1], "score": r[2],
             "trend": r[3], "interpretation": r[4], "action": r[5]}
            for r in rows
        ]
        return calculate_score_from_rules(value, rules)

    except Exception as e:
        logger.error(f"❌ Fout bij technische interpretatie '{name}': {e}", exc_info=True)
        return None
    finally:
        conn.close()
