import logging
import requests

from backend.utils.db import get_db_connection
from backend.utils.scoring_utils import normalize_indicator_name

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

            # Volume (laatste 10 candles)
            if "volume" in name.lower():
                return {"value": sum(volumes[-10:])}

            # Laatste close
            if name.lower() == "close":
                return {"value": closes[-1]}

        # ---------------------------------------------------------
        # 📌 Fallback API’s
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

        logger.warning(f"⚠️ Geen bruikbare data: {str(data)[:150]}")
        return None

    except Exception as e:
        logger.error(f"❌ Fout bij ophalen '{name}': {e}", exc_info=True)
        return None


# =========================================================
# 🧠 SCOREREGELS VIA DATABASE (PER USER)
# =========================================================
def interpret_technical_indicator_db(indicator: str, value: float, user_id: int):
    """
    Nieuwe, zuivere interpretatielaag:
    - Normaliseert indicatornaam
    - Haalt user-specifieke scoreregels op
    - Matcht via min/max ranges
    """

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding bij interpretatie.")
        return None

    try:
        indicator = normalize_indicator_name(indicator)

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

        # Ranges doorlopen
        for (min_v, max_v, score, trend, interp, act) in rows:
            if (min_v is None or value >= min_v) and (max_v is None or value < max_v):
                return {
                    "score": score,
                    "trend": trend,
                    "interpretation": interp,
                    "action": act,
                }

        # Geen match → neutrale fallback
        return {
            "score": 50,
            "trend": "neutral",
            "interpretation": "Geen matchende scoreregel",
            "action": "–"
        }

    except Exception as e:
        logger.error(f"❌ interpret_technical_indicator_db fout: {e}", exc_info=True)
        return None

    finally:
        conn.close()
