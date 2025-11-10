import requests
import logging
from backend.utils.db import get_db_connection
from backend.utils.scoring_utils import calculate_score_from_rules

logger = logging.getLogger(__name__)

# =====================================================
# 📈 RSI Berekening (lokale fallback)
# =====================================================
def calculate_rsi(closes, period=14):
    """Bereken RSI op basis van slotkoersen (standaard 14 candles)."""
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


# =====================================================
# 🌐 Nieuwe functie: fetch_technical_value
# =====================================================
async def fetch_technical_value(name: str, source: str = None, link: str = None):
    """
    🔍 Haalt actuele waarde op van een technische indicator via data_url in DB.
    Ondersteunt RSI, MA, Volume (en berekent RSI zelf bij Binance-klines).
    """
    try:
        if not link:
            logger.warning(f"⚠️ Geen data_url voor technische indicator '{name}'")
            return None

        logger.info(f"🌐 Ophalen technische indicator '{name}' via {source} -> {link}")
        resp = requests.get(link, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        # === 1️⃣ Binance-klines: bereken RSI / MA / Volume zelf
        if "binance" in link.lower() and isinstance(data, list):
            closes = [float(k[4]) for k in data if isinstance(k, list) and len(k) >= 5]
            volumes = [float(k[5]) for k in data if isinstance(k, list) and len(k) >= 6]

            if "rsi" in name.lower():
                value = calculate_rsi(closes)
                if value:
                    logger.info(f"📊 RSI berekend uit Binance candles: {value}")
                    return {"value": value}

            if "ma" in name.lower():
                period = int(''.join(filter(str.isdigit, name))) or 200
                if len(closes) >= period:
                    value = round(sum(closes[-period:]) / period, 2)
                    logger.info(f"📊 MA{period} berekend: {value}")
                    return {"value": value}

            if "volume" in name.lower():
                value = round(sum(volumes[-10:]), 2)
                logger.info(f"📊 Gemiddelde volume berekend: {value}")
                return {"value": value}

        # === 2️⃣ Directe API met RSI / MA waarde
        if "rsi" in name.lower():
            if isinstance(data, dict):
                v = data.get("value") or data.get("rsi")
                if v:
                    return {"value": float(v)}
            if isinstance(data, list) and len(data) > 0:
                last = data[-1]
                if isinstance(last, dict):
                    v = last.get("rsi") or last.get("value")
                    if v:
                        return {"value": float(v)}

        if "ma" in name.lower():
            if isinstance(data, dict):
                v = data.get("ma") or data.get("value")
                if v:
                    return {"value": float(v)}

        if "volume" in name.lower():
            if isinstance(data, dict):
                v = data.get("volume") or data.get("value")
                if v:
                    return {"value": float(v)}

        # === 3️⃣ Fallback – probeer iets bruikbaars te vinden
        if isinstance(data, dict):
            for key in ["value", "price", "close"]:
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
# 📊 Interpretatie van technische waarden
# =====================================================
def interpret_technical_indicator(name: str, value: float) -> dict | None:
    """Vertaal technische indicatorwaarde (RSI, MA200, Volume, etc.) via scoreregels in DB."""
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
