import httpx
import logging
from backend.config.config_loader import load_macro_config

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

YAHOO_BASE_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?range=1d&interval=1d"


async def process_all_macro_indicators():
    """
    ➤ Verwerkt alle macro-indicatoren uit config.
    """
    try:
        macro_config = load_macro_config()
    except Exception as e:
        logger.error(f"❌ [CFG01] Config laden mislukt: {e}")
        return []

    results = []
    for name, config in macro_config.get("indicators", {}).items():
        try:
            result = await process_macro_indicator(name, config)
            results.append(result)
        except Exception as e:
            logger.error(f"❌ [PROC01] Indicator '{name}' verwerken mislukt: {e}")
            results.append({"name": name, "error": str(e)})

    return results


async def process_macro_indicator(name, config):
    """
    ➤ Verwerkt een enkele macro-indicator via Yahoo of andere bron.
    """
    symbol = config.get("symbol")
    source = config.get("source", "yahoo")

    if source == "yahoo":
        url = YAHOO_BASE_URL.format(symbol=symbol)
    else:
        raise ValueError(f"❌ [SRC01] Onbekende macrobron: {source}")

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.get(url)
            response.raise_for_status()
            data = response.json()
    except Exception as e:
        logger.error(f"❌ [API01] API-fout bij ophalen van {name}: {e}")
        raise

    try:
        value = extract_yahoo_value(data)
    except Exception as e:
        logger.error(f"❌ [PARSE01] Fout bij uitlezen waarde voor {name}: {e}")
        raise

    score = calculate_score(value, config["thresholds"], config["positive"])
    interpretation = config.get("explanation", "")
    action = config.get("action", "")

    return {
        "name": name,
        "value": value,
        "score": score,
        "symbol": symbol,
        "source": source,
        "category": config.get("category"),
        "correlation": config.get("correlation"),
        "interpretation": interpretation,
        "action": action,
        "link": generate_chart_link(source, symbol),
    }


def extract_yahoo_value(data):
    """
    ➤ Extract laatste slotkoers uit Yahoo response.
    """
    result = data["chart"]["result"][0]
    close_prices = result["indicators"]["quote"][0]["close"]
    return float(close_prices[-1])


def calculate_score(value, thresholds, positive=True):
    """
    ➤ Bereken score op basis van drempels (laag, neutraal, hoog).
    """
    score = 0
    for threshold in thresholds:
        if value >= threshold:
            score += 1
    return score if positive else 3 - score


def generate_chart_link(source, symbol):
    if not source or not symbol:
        return None
    if source == "yahoo":
        return f"https://finance.yahoo.com/quote/{symbol}"
    elif source == "tradingview":
        return f"https://www.tradingview.com/symbols/{symbol.replace(':', '')}/"
    return None
