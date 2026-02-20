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
    Async fetch voor technische indicatoren.
    Crasht nooit.
    """

    try:
        if not link:
            logger.warning(f"⚠️ Geen link opgegeven voor '{name}'")
            return None

        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(link)
            resp.raise_for_status()
            data = resp.json()

        lname = name.lower()

        # -------------------------------------------------
        # 📊 Binance candles parsing
        # -------------------------------------------------
        if "binance" in link.lower() and isinstance(data, list):
            try:
                closes = [float(k[4]) for k in data if len(k) > 4]
                volumes = [float(k[5]) for k in data if len(k) > 5]
            except Exception:
                return None

            if not closes:
                return None

            # RSI
            if "rsi" in lname:
                value = calculate_rsi(closes)
                return {"value": value}

            # MA200 ratio
            if "ma200" in lname or "ma_200" in lname:
                if len(closes) >= 200:
                    ma = sum(closes[-200:]) / 200
                    return {"value": closes[-1] / ma}

            # Volume strength
            if "volume" in lname:
                return {"value": sum(volumes[-10:])}

            # Last close
            if lname == "close":
                return {"value": closes[-1]}

        # -------------------------------------------------
        # 🧩 JSON fallback parsing
        # -------------------------------------------------
        if isinstance(data, dict):
            for key in ("value", "close", "price", "last"):
                if key in data:
                    return {"value": float(data[key])}

        if isinstance(data, list) and data:
            last = data[-1]
            if isinstance(last, dict):
                for key in ("value", "close", "price"):
                    if key in last:
                        return {"value": float(last[key])}

        return None

    except Exception as e:
        logger.error(f"❌ fetch_technical_value fout '{name}': {e}", exc_info=True)
        return None


# =========================================================
# 🧠 Interpretatie via DB scoreregels
# =========================================================
def interpret_technical_indicator_db(indicator: str, value: float, user_id: int):
    """
    Interpreteert technische indicator via database scoreregels.
    """

    try:
        normalized = normalize_indicator_name(indicator)

        result = get_score_rule_from_db("technical", normalized, value)

        return {
            "score": result.get("score", 10),
            "trend": result.get("trend") or "neutral",
            "interpretation": result.get("interpretation")
                or "Geen interpretatie beschikbaar",
            "action": result.get("action") or "Geen actie",
        }

    except Exception as e:
        logger.error("❌ interpret_technical_indicator_db fout", exc_info=True)

        return {
            "score": 10,
            "trend": "neutral",
            "interpretation": "Interpretatiefout",
            "action": "Controleer logs",
        }
