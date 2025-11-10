import requests
import logging
from backend.utils.db import get_db_connection
from backend.utils.scoring_utils import calculate_score_from_rules

logger = logging.getLogger(__name__)

# =====================================================
# 🌐 Nieuwe functie: fetch_macro_value
# =====================================================
async def fetch_macro_value(name: str, source: str = None, link: str = None):
    """
    🔍 Haalt de actuele waarde op van een macro-indicator via de data_url in de DB.
    Geeft altijd {"value": float(...)} terug.
    Ondersteunt o.a. DXY, S&P500, Fear & Greed Index.
    """
    try:
        if not link:
            logger.warning(f"⚠️ Geen link opgegeven voor '{name}'")
            return None

        logger.info(f"🌐 Ophalen macro indicator '{name}' vanaf {link}")
        resp = requests.get(link, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        # === 1️⃣ Fear & Greed Index (Alternative.me)
        if "data" in data and isinstance(data["data"], list) and len(data["data"]) > 0:
            v = data["data"][0].get("value")
            if v is not None:
                return {"value": float(v)}

        # === 2️⃣ DXY of S&P500 via API
        if isinstance(data, dict):
            if "price" in data:
                return {"value": float(data["price"])}
            if "value" in data:
                return {"value": float(data["value"])}
            if "data" in data and isinstance(data["data"], dict):
                if "value" in data["data"]:
                    return {"value": float(data["data"]["value"])}
                if "price" in data["data"]:
                    return {"value": float(data["data"]["price"])}

        # === 3️⃣ Indien lijst
        if isinstance(data, list) and len(data) > 0:
            first = data[0]
            if isinstance(first, (int, float)):
                return {"value": float(first)}
            if isinstance(first, dict):
                v = first.get("value") or first.get("price")
                if v:
                    return {"value": float(v)}

        logger.warning(f"⚠️ Geen waarde gevonden voor '{name}' in respons: {data}")
        return None

    except Exception as e:
        logger.error(f"❌ Fout bij ophalen macro-waarde voor '{name}': {e}")
        return None


# =====================================================
# 📊 Bestaande functie: interpret_macro_indicator
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
                ORDER BY range_min ASC
            """, (name,))
            rows = cur.fetchall()

        if not rows:
            logger.warning(f"⚠️ Geen regels gevonden voor macro-indicator '{name}'")
            return None

        rules = [
            {"range_min": r[0], "range_max": r[1], "score": r[2],
             "trend": r[3], "interpretation": r[4], "action": r[5]}
            for r in rows
        ]
        return calculate_score_from_rules(value, rules)

    except Exception as e:
        logger.error(f"❌ Fout bij macro-interpretatie '{name}': {e}", exc_info=True)
        return None
    finally:
        conn.close()
