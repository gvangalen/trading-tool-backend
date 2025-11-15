import logging
import requests
from backend.utils.db import get_db_connection
from backend.utils.scoring_utils import get_score_rule_from_db, normalize_indicator_name

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
# 🌐 Technische data ophalen
# =========================================================
def fetch_technical_value(name: str, source: str = None, link: str = None):
    """
    Haalt technische indicator op (RSI, MA, Volume, etc.).
    Ondersteunt Binance-klines en standaard API responses.
    """
    try:
        if not link:
            logger.warning(f"⚠️ Geen data_url voor '{name}'")
            return None

        logger.info(f"🌐 Ophalen technische indicator '{name}' → {link}")

        resp = requests.get(link, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        # --- Binance-klines ---
        if "binance" in link.lower() and isinstance(data, list):
            closes = [float(k[4]) for k in data if isinstance(k, list) and len(k) >= 5]
            volumes = [float(k[5]) for k in data if isinstance(k, list) and len(k) >= 6]

            if "rsi" in name.lower():
                value = calculate_rsi(closes)
                return {"value": value}

            if "ma" in name.lower():
                period = int(''.join(filter(str.isdigit, name))) or 200
                if len(closes) >= period:
                    ma = sum(closes[-period:]) / period
                    return {"value": round(ma, 2)}

            if "volume" in name.lower():
                return {"value": sum(volumes[-10:])}

        # --- Directe API response ---
        if "value" in data:
            return {"value": float(data["value"])}

        if isinstance(data, dict):
            for key in ["close", "rsi", "ma", "volume"]:
                if key in data and isinstance(data[key], (int, float)):
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
# 📊 Interpretatie via DB-regels
# =========================================================
def interpret_technical_indicator(name: str, value: float):
    """
    Combineert waarde + scoreregels in DB:
    - score
    - trend
    - interpretation
    - action
    """
    if value is None:
        return {
            "indicator": name,
            "value": None,
            "score": 10,
            "trend": "",
            "interpretation": "Geen data",
            "action": ""
        }

    normalized = normalize_indicator_name(name)

    rule = get_score_rule_from_db("technical", normalized, value)

    if not rule:
        return {
            "indicator": normalized,
            "value": value,
            "score": 10,
            "trend": "",
            "interpretation": "Geen scoreregels gevonden",
            "action": ""
        }

    return {
        "indicator": normalized,
        "value": value,
        "score": rule["score"],
        "trend": rule["trend"],
        "interpretation": rule["interpretation"],
        "action": rule["action"]
    }
