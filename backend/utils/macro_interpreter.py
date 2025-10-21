import os
import httpx
import logging
from backend.config.config_loader import load_macro_config

# ✅ Logging instellen
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ✅ API URL's
YAHOO_BASE_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?range=1d&interval=1d"
ALPHA_VANTAGE_URL = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}"
FEAR_GREED_URL = "https://api.alternative.me/fng/?limit=1"

ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")


# ✅ Verwerkt alle macro-indicatoren
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
    symbol = config.get("symbol")
    source = config.get("source", "yahoo")
    fallback_symbol = config.get("fallback_symbol", symbol)
    value = None

    # ✅ Alternative source (zoals Fear & Greed Index)
    if source == "alternative":
        value = await fetch_fear_greed_value()
        source = "alternative.me"
        symbol = "FearGreedIndex"
    else:
        # 🔁 Stap 1: Yahoo API
        try:
            url = YAHOO_BASE_URL.format(symbol=symbol)
            async with httpx.AsyncClient(timeout=10) as client:
                response = await client.get(url)
                if response.status_code == 429:
                    raise httpx.HTTPStatusError("Too Many Requests", request=response.request, response=response)
                response.raise_for_status()
                data = response.json()
                value = extract_yahoo_value(data)
                logger.info(f"✅ [YAHOO] {name} waarde opgehaald: {value}")
        except Exception as e:
            logger.warning(f"⚠️ [FALLBACK] Yahoo mislukt voor {name} ({symbol}): {e}")

        # 🔁 Stap 2: Fallback naar Alpha Vantage
        if value is None:
            value = await fetch_alpha_vantage_value(fallback_symbol)
            if value:
                source = "alpha_vantage"
                logger.info(f"✅ [AV03] Alpha Vantage waarde voor {fallback_symbol}: {value}")
            else:
                logger.error(f"❌ [AV04] Geen waarde van Yahoo of Alpha voor {name}")

    if value is None:
        raise RuntimeError(f"❌ [FAIL] Geen waarde gevonden voor {name}")

    return {
        "name": name,
        "value": value,
        "symbol": symbol,
        "source": source,
        "category": config.get("category"),
        "correlation": config.get("correlation"),
        "explanation": config.get("explanation"),
        "timeframe": config.get("timeframe"),
        "link": generate_chart_link(source, symbol),
        "thresholds": config.get("thresholds", []),
        "positive": config.get("positive", True),
        "interpretations": config.get("interpretations"),
        "actions": config.get("actions")
    }


# ✅ Yahoo: extract slotkoers
def extract_yahoo_value(data):
    try:
        result = data["chart"]["result"][0]
        close_prices = result["indicators"]["quote"][0]["close"]
        return float(close_prices[-1])
    except Exception as e:
        raise ValueError(f"Fout bij uitlezen Yahoo-data: {e}")


# ✅ Alpha Vantage fallback
async def fetch_alpha_vantage_value(symbol):
    if not ALPHA_VANTAGE_API_KEY:
        logger.warning("⚠️ [AV01] Geen Alpha Vantage API key ingesteld")
        return None

    url = ALPHA_VANTAGE_URL.format(symbol=symbol, api_key=ALPHA_VANTAGE_API_KEY)
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
            return float(data["data"][0]["value"])
    except Exception as e:
        logger.warning(f"⚠️ [FG01] Fear & Greed ophalen mislukt: {e}")
        return None


# ✅ Externe chartlink
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
