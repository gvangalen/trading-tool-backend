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
# 🌐 Technische indicator waarde ophalen (RAW ONLY)
# =========================================================
async def fetch_technical_value(name: str, source: str = None, link: str = None):
    """
    Async fetch voor technische indicatoren.
    Geeft ALLEEN raw values terug.
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

            # RSI (0–100 al correct)
            if "rsi" in lname:
                value = calculate_rsi(closes)
                return {"value": value}

            # MA200 ratio (bijv 1.08 of 0.95)
            if "ma200" in lname or "ma_200" in lname:
                if len(closes) >= 200:
                    ma = sum(closes[-200:]) / 200
                    return {"value": closes[-1] / ma}

            # Volume strength (absolute volume)
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
# 🔹 Technische normalisatie naar 0–100
# =========================================================
def normalize_technical_value(indicator: str, value: float) -> float:
    """
    Zet raw technische waarde om naar genormaliseerde 0–100 schaal.
    """

    try:
        if value is None:
            return 0

        value = float(value)
        indicator = indicator.lower()

        # RSI is al 0–100
        if "rsi" in indicator:
            return max(0, min(100, value))

        # MA200 ratio (1.0 = neutraal)
        if "ma200" in indicator or "ma_200" in indicator:
            deviation = abs(value - 1)
            cap = 0.2  # 20% boven MA = extreem
            return min(100, (deviation / cap) * 100)

        # Volume strength (absolute → schaal)
        if "volume" in indicator:
            cap = 1_000_000_000  # schaal afhankelijk van je timeframe
            return min(100, (value / cap) * 100)

        # Price (optioneel deviation-based)
        if "close" in indicator or "price" in indicator:
            return max(0, min(100, value))

        # Fallback
        return max(0, min(100, value))

    except Exception:
        logger.error("Technische normalisatie fout", exc_info=True)
        return 0


# =========================================================
# 🧠 Interpretatie via DB scoreregels (met normalisatie)
# =========================================================
def interpret_technical_indicator_db(indicator: str, value: float, user_id: int):
    """
    Flow:
    raw_value → normalized_value (0–100) → DB rules → score
    """

    try:
        normalized_name = normalize_indicator_name(indicator)

        # 🔥 eerst normaliseren
        normalized_value = normalize_technical_value(
            normalized_name,
            value,
        )

        result = get_score_rule_from_db(
            "technical",
            normalized_name,
            normalized_value,
        )

        if not result:
            return {
                "score": 10,
                "trend": "neutral",
                "interpretation": "Geen scoreregel match",
                "action": "Geen actie",
            }

        return {
            "score": max(0, min(100, result.get("score", 10))),
            "trend": result.get("trend") or "neutral",
            "interpretation": result.get("interpretation")
                or "Geen interpretatie beschikbaar",
            "action": result.get("action") or "Geen actie",
        }

    except Exception:
        logger.error("❌ interpret_technical_indicator_db fout", exc_info=True)

        return {
            "score": 10,
            "trend": "neutral",
            "interpretation": "Interpretatiefout",
            "action": "Controleer logs",
        }
