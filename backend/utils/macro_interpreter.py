import os
import httpx
import logging
from backend.config.config_loader import load_macro_config

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

YAHOO_BASE_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?range=1d&interval=1d"
ALPHA_VANTAGE_URL = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}"
FEAR_GREED_URL = "https://api.alternative.me/fng/?limit=1"


# ✅ Haalt alle macro-indicatoren op met fallback-logica
async def process_all_macro_indicators():
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
            logger.error(f"❌ [PROC01] {name} verwerken mislukt: {e}")
            results.append({"name": name, "error": str(e)})
    return results


# ✅ Verwerkt één macro-indicator met fallbacklogica
async def process_macro_indicator(name, config):
    name = name.lower()
    symbol = config.get("symbol")
    source = config.get("source", "yahoo")
    value = None

    # 👇 Fear & Greed: oversla Yahoo en Alpha direct
    if name == "fear_greed":
        value = await fetch_fear_greed_value()
        source = "alternative.me"
        symbol = "FearGreedIndex"
    else:
        # 🔁 Stap 1: Yahoo API proberen
        try:
            url = YAHOO_BASE_URL.format(symbol=symbol)
            async with httpx.AsyncClient(timeout=10) as client:
                response = await client.get(url)
                response.raise_for_status()
                data = response.json()
                value = extract_yahoo_value(data)
                logger.info(f"✅ [YAHOO] {name} waarde opgehaald: {value}")
        except httpx.HTTPStatusError as e:
            logger.warning(f"⚠️ [FALLBACK] Yahoo HTTP fout ({e.response.status_code}) voor {name}: {e}")
        except Exception as e:
            logger.warning(f"⚠️ [FALLBACK] Yahoo fout voor {name}: {e}")

        # 🔁 Stap 2: fallback naar Alpha Vantage
        if value is None:
            fallback_symbol = config.get("fallback_symbol", symbol)
            value = await fetch_alpha_vantage_value(fallback_symbol)
            if value:
                source = "alpha_vantage"
                logger.info(f"✅ [AV03] Alpha Vantage waarde voor {fallback_symbol}: {value}")
            else:
                logger.error(f"❌ [AV04] Geen waarde van Yahoo of Alpha voor {name}")

    if value is None:
        raise RuntimeError(f"❌ [FAIL] Geen waarde gevonden voor {name}")

    thresholds = config.get("thresholds", [])
    positive = config.get("positive", True)

    score = calculate_score(value, thresholds, positive)
    trend = determine_trend(value, thresholds, positive)
    interpretation = interpret_value(value, thresholds, positive)

    return {
        "name": name,
        "value": value,
        "score": score,
        "trend": trend,
        "interpretation": interpretation,
        "symbol": symbol,
        "source": source,
        "category": config.get("category"),
        "correlation": config.get("correlation"),
        "explanation": config.get("explanation"),
        "action": config.get("action"),
        "link": generate_chart_link(source, symbol),
    }


# ✅ Yahoo: extract sluitingswaarde
def extract_yahoo_value(data):
    try:
        result = data["chart"]["result"][0]
        close_prices = result["indicators"]["quote"][0]["close"]
        return float(close_prices[-1])
    except Exception as e:
        raise ValueError(f"Fout bij uitlezen Yahoo-data: {e}")


# ✅ Alpha Vantage fallback
async def fetch_alpha_vantage_value(symbol):
    api_key = os.getenv("ALPHA_VANTAGE_API_KEY")
    if not api_key:
        logger.warning("⚠️ [AV01] Geen Alpha Vantage API key ingesteld")
        return None

    url = ALPHA_VANTAGE_URL.format(symbol=symbol, api_key=api_key)
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.get(url)
            response.raise_for_status()
            data = response.json()
            ts = data.get("Time Series (Daily)")
            if not ts:
                logger.warning(f"⚠️ [AV02] Geen time series in Alpha Vantage response voor {symbol}")
                return None
            latest_day = sorted(ts.keys())[-1]
            return float(ts[latest_day]["4. close"])
    except Exception as e:
        logger.warning(f"⚠️ [AV03] Alpha Vantage API-fout voor {symbol}: {e}")
        return None


# ✅ Fear & Greed index
async def fetch_fear_greed_value():
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.get(FEAR_GREED_URL)
            response.raise_for_status()
            data = response.json()
            value = data["data"][0]["value"]
            return float(value)
    except Exception as e:
        logger.warning(f"⚠️ [FG01] Fear & Greed ophalen mislukt: {e}")
        return None


# ✅ Interpretatie-functies
def calculate_score(value, thresholds, positive=True):
    if value is None or not thresholds or len(thresholds) != 3:
        return 0

    v = float(value)
    if positive:
        if v >= thresholds[2]:
            return 90
        elif v >= thresholds[1]:
            return 70
        elif v >= thresholds[0]:
            return 50
        else:
            return 30
    else:
        if v <= thresholds[0]:
            return 90
        elif v <= thresholds[1]:
            return 70
        elif v <= thresholds[2]:
            return 50
        else:
            return 30


def determine_trend(value, thresholds, positive=True):
    if value is None:
        return "Onbekend"

    v = float(value)
    if positive:
        if v >= thresholds[2]:
            return "Zeer sterk"
        elif v >= thresholds[1]:
            return "Sterk"
        elif v >= thresholds[0]:
            return "Neutraal"
        else:
            return "Zwak"
    else:
        if v <= thresholds[0]:
            return "Zeer sterk"
        elif v <= thresholds[1]:
            return "Sterk"
        elif v <= thresholds[2]:
            return "Neutraal"
        else:
            return "Zwak"


def interpret_value(value, thresholds, positive=True):
    if value is None:
        return "Ongeldig"

    v = float(value)
    if positive:
        if v >= thresholds[2]:
            return "Sterke stijging"
        elif v >= thresholds[1]:
            return "Stijgend"
        elif v >= thresholds[0]:
            return "Neutraal"
        else:
            return "Dalend"
    else:
        if v <= thresholds[0]:
            return "Sterke daling"
        elif v <= thresholds[1]:
            return "Dalend"
        elif v <= thresholds[2]:
            return "Neutraal"
        else:
            return "Stijgend"


def generate_chart_link(source, symbol):
    if not source or not symbol:
        return None
    if source == "yahoo":
        return f"https://finance.yahoo.com/quote/{symbol}"
    elif source == "tradingview":
        return f"https://www.tradingview.com/symbols/{symbol.replace(':', '')}/"
    elif source == "alternative.me":
        return "https://alternative.me/crypto/fear-and-greed-index/"
    elif source == "alpha_vantage":
        return f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}"
    return None
