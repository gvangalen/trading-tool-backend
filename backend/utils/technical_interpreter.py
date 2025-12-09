import logging
import httpx

from backend.utils.scoring_utils import (
    normalize_indicator_name,
    get_score_rule_from_db,
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
# 🌐 Technische indicator waarde ophalen (ASYNC)
# =========================================================
async def fetch_technical_value(name: str, source: str = None, link: str = None):
    """
    Async versie — voorkomt await errors.
    Wordt aangeroepen via: result = await fetch_technical_value(...)
    """

    try:
        if not link:
            logger.warning(f"⚠️ Geen link opgegeven voor '{name}'")
            return None

        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(link)
        resp.raise_for_status()
        data = resp.json()

        # -----------------------------------------
        # 📊 Binance candles
        # -----------------------------------------
        if "binance" in link.lower() and isinstance(data, list):
            closes = [float(k[4]) for k in data]
            volumes = [float(k[5]) for k in data]

            # RSI
            if "rsi" in name.lower():
                return {"value": calculate_rsi(closes)}

            # MA200
            if "ma200" in name.lower() or "ma_200" in name.lower():
                if len(closes) >= 200:
                    ma = sum(closes[-200:]) / 200
                    return {"value": closes[-1] / ma}

            # Volume
            if "volume" in name.lower():
                return {"value": sum(volumes[-10:])}

            # Close value
            if name.lower() == "close":
                return {"value": closes[-1]}

        # -----------------------------------------
        # 🧩 Fallback JSON parsing
        # -----------------------------------------
        if isinstance(data, dict):
            for key in ("value", "close", "price"):
                if key in data:
                    return {"value": float(data[key])}

        if isinstance(data, list) and data and isinstance(data[-1], dict):
            for key in ("value", "close", "price"):
                if key in data[-1]:
                    return {"value": float(data[-1][key])}

        return None

    except Exception as e:
        logger.error(f"❌ Fout bij ophalen '{name}': {e}", exc_info=True)
        return None


# =========================================================
# 🧠 Interpreteer technische indicator via DB-SCOREREGELS
# =========================================================
def interpret_technical_indicator_db(indicator: str, value: float, user_id: int):
    """
    LET OP:
    scoring_utils.get_score_rule_from_db() verwacht een CATEGORY.
    In dit geval: 'technical'
    """
    try:
        normalized = normalize_indicator_name(indicator)

        rule = get_score_rule_from_db("technical", normalized, value)

        if not rule:
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
        logger.error(
            f"❌ interpret_technical_indicator_db fout: {e}",
            exc_info=True
        )
        return None
