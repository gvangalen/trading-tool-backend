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
    """
    RSI, MA200 (ratio!), Volume, Close etc.
    """

    try:
        if not link:
            logger.warning(f"⚠️ Geen link voor '{name}'")
            return None

        logger.info(f"🌐 Ophalen technische indicator '{name}' → {link}")

        resp = requests.get(link, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        # ---------------------------------------------------------
        # 📌 Binance klines — jouw standaard datasource
        # ---------------------------------------------------------
        if "binance" in link.lower() and isinstance(data, list):

            closes = [float(k[4]) for k in data]
            volumes = [float(k[5]) for k in data]

            # -----------------------------------------------------
            # RSI
            # -----------------------------------------------------
            if "rsi" in name.lower():
                value = calculate_rsi(closes)
                return {"value": value}

            # -----------------------------------------------------
            # MA200 → ratio close / MA200
            # -----------------------------------------------------
            if "ma_200" in name.lower() or name.lower() == "ma200":
                if len(closes) >= 200:
                    ma = sum(closes[-200:]) / 200
                    last_close = closes[-1]
                    ratio = last_close / ma
                    return {"value": round(ratio, 4)}

            # -----------------------------------------------------
            # Volume (laatste 10 candles)
            # -----------------------------------------------------
            if "volume" in name.lower():
                return {"value": sum(volumes[-10:])}

            # -----------------------------------------------------
            # Close price
            # -----------------------------------------------------
            if name.lower() == "close":
                return {"value": closes[-1]}

        # ---------------------------------------------------------
        # Andere API's (fallback)
        # ---------------------------------------------------------
        if isinstance(data, dict):
            for key in ["value", "close", "price"]:
                if key in data:
                    return {"value": float(data[key])}

        if isinstance(data, list) and len(data) > 0:
            last = data[-1]
            if isinstance(last, dict):
                for key in ["value", "close", "price"]:
                    if key in last:
                        return {"value": float(last[key])}

        logger.warning(f"⚠️ Geen bruikbare data in response: {str(data)[:120]}")
        return None

    except Exception as e:
        logger.error(f"❌ Fout bij ophalen '{name}': {e}", exc_info=True)
        return None



# =========================================================
# 🧠 NIEUW: interpretatie via SCOREREGELS IN DB (per user)
# =========================================================
def interpret_technical_indicator_db(indicator: str, value: float, user_id: int):
    """
    Gebruikt scoreregels uit `technical_indicator_rules` (per gebruiker).
    Zo werkt jouw nieuwe systeem!
    """

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding bij interpretatie.")
        return None

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT range_min, range_max, score, interpretation, action, trend
                FROM technical_indicator_rules
                WHERE indicator = %s AND user_id = %s
                ORDER BY range_min ASC
            """, (indicator, user_id))

            rows = cur.fetchall()

        if not rows:
            logger.warning(f"⚠️ Geen scoreregels gevonden voor '{indicator}' (user_id={user_id})")
            return None

        # ---------------------------------------------------------
        # Zoek de passende scoreregel
        # ---------------------------------------------------------
        for (min_v, max_v, score, interp, act, trend) in rows:
            if (min_v is None or value >= min_v) and (max_v is None or value < max_v):
                return {
                    "score": score,
                    "interpretation": interp,
                    "action": act,
                    "trend": trend,
                }

        # Geen match → fallback
        return {
            "score": 50,
            "interpretation": "Geen matchende scoreregel",
            "action": "–",
            "trend": "neutral",
        }

    except Exception as e:
        logger.error(f"❌ interpret_technical_indicator_db fout: {e}")
        return None

    finally:
        conn.close()



# =========================================================
# LEGACY FUNCTIE — alleen als fallback (niet meer gebruikt)
# =========================================================
def interpret_technical_indicator(name: str, value: float):
    """
    Deze oude functie werkt niet met users.
    Je Celery-task gebruikt deze niet meer.
    """
    normalized = normalize_indicator_name(name)
    return {
        "indicator": normalized,
        "value": value,
        "score": 50,
        "trend": "",
        "interpretation": "Legacy function — niet meer in gebruik",
        "action": ""
    }
