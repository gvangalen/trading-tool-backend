import logging
import requests

from backend.utils.db import get_db_connection
from backend.utils.scoring_utils import (
    normalize_indicator_name,
    select_rule_for_value,            # ✅ nieuwe engine
)

logger = logging.getLogger(__name__)


# =========================================================
# 📈 RSI Berekening
# =========================================================
def calculate_rsi(closes, period=14):
    if len(closes) < period + 1:
        return None

    gains, losses = [], []
    for i in range(1, period + 1):
        delta = closes[-i] - closes[-i - 1]
        gains.append(max(delta, 0))
        losses.append(max(-delta, 0))

    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period

    if avg_loss == 0:
        return 100.0

    rs = avg_gain / avg_loss
    return round(100 - (100 / (1 + rs)), 2)


# =========================================================
# 🌐 Technische indicator waarde ophalen
# =========================================================
def fetch_technical_value(name: str, source: str = None, link: str = None):
    try:
        if not link:
            logger.warning(f"⚠️ Geen link voor '{name}'")
            return None

        logger.info(f"🌐 Ophalen technische indicator '{name}' → {link}")

        resp = requests.get(link, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        # ---------------------------------------------------------
        # 📌 Binance klines
        # ---------------------------------------------------------
        if "binance" in link.lower() and isinstance(data, list):

            closes = [float(k[4]) for k in data]
            volumes = [float(k[5]) for k in data]

            # RSI
            if "rsi" in name.lower():
                return {"value": calculate_rsi(closes)}

            # MA200 ratio
            if "ma200" in name.lower() or "ma_200" in name.lower():
                if len(closes) >= 200:
                    ma = sum(closes[-200:]) / 200
                    ratio = closes[-1] / ma
                    return {"value": round(ratio, 4)}

            # Volume
            if "volume" in name.lower():
                return {"value": sum(volumes[-10:])}

            # Close
            if name.lower() == "close":
                return {"value": closes[-1]}

        # ---------------------------------------------------------
        # Andere API’s
        # ---------------------------------------------------------
        if isinstance(data, dict):
            for key in ["value", "close", "price"]:
                if key in data:
                    return {"value": float(data[key])}

        if isinstance(data, list) and len(data) > 0 and isinstance(data[-1], dict):
            last = data[-1]
            for key in ["value", "close", "price"]:
                if key in last:
                    return {"value": float(last[key])}

        logger.warning(f"⚠️ Geen bruikbare data: {str(data)[:120]}")
        return None

    except Exception as e:
        logger.error(f"❌ Fout bij ophalen '{name}': {e}", exc_info=True)
        return None



# =========================================================
# 🧠 SCOREREGELS VIA DATABASE (PER USER)
# =========================================================
def interpret_technical_indicator_db(indicator: str, value: float, user_id: int):
    """
    Nieuwe veilige scoring op basis van DB-regels + select_rule_for_value()
    """

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding bij interpretatie.")
        return None

    try:
        indicator = normalize_indicator_name(indicator)  # ❗ belangrijk

        with conn.cursor() as cur:
            cur.execute("""
                SELECT range_min, range_max, score, trend, interpretation, action
                FROM technical_indicator_rules
                WHERE indicator = %s AND user_id = %s
                ORDER BY range_min ASC
            """, (indicator, user_id))

            rows = cur.fetchall()

        if not rows:
            logger.warning(f"⚠️ Geen scoreregels gevonden voor '{indicator}' (user_id={user_id})")
            return None

        # Structureren
        rules = [
            {
                "range_min": float(r[0]) if r[0] is not None else None,
                "range_max": float(r[1]) if r[1] is not None else None,
                "score": int(r[2]),
                "trend": r[3],
                "interpretation": r[4],
                "action": r[5],
            }
            for r in rows
        ]

        # Gebruik de universele scoring engine
        matched = select_rule_for_value(value, rules)

        if not matched:
            return {
                "score": 50,
                "interpretation": "Geen matchende scoreregel",
                "action": "–",
                "trend": "neutral",
            }

        return matched

    except Exception as e:
        logger.error(f"❌ interpret_technical_indicator_db fout: {e}", exc_info=True)
        return None

    finally:
        conn.close()



# =========================================================
# 🧹 LEGACY — NIET MEER GEBRUIKT
# =========================================================
def interpret_technical_indicator(name: str, value: float):
    normalized = normalize_indicator_name(name)
    return {
        "indicator": normalized,
        "value": value,
        "score": 50,
        "trend": "",
        "interpretation": "Legacy function — niet meer in gebruik",
        "action": ""
    }
