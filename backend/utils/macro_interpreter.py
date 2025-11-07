import httpx
import logging
import os

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

FEAR_GREED_URL = "https://api.alternative.me/fng/?limit=1"
ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")

# ============================================
# 🎯 Kernfunctie: waarde ophalen op basis van source
# ============================================

async def fetch_macro_value(name: str, source: str, link: str = None, symbol: str = None) -> dict:
    """
    Haal de actuele waarde op voor een macro-indicator op basis van de DB-config.
    - `source`: bepaalt welke externe API wordt gebruikt.
    - `link`: bevat eventueel het volledige API-endpoint (uit DB).
    - Geeft een dict terug met { name, value, source, link }.
    """
    try:
        logger.info(f"📡 [fetch_macro_value] Ophalen: {name} (source={source})")

        # 🧭 1️⃣ Alternative.me (Fear & Greed Index)
        if source.lower() in ["fear_greed", "alternative.me", "alternative"]:
            async with httpx.AsyncClient(timeout=10) as client:
                response = await client.get(FEAR_GREED_URL)
                response.raise_for_status()
                data = response.json()
                value = float(data["data"][0]["value"])
                return {"name": name, "value": value, "source": "alternative.me", "link": FEAR_GREED_URL}

        # 💹 2️⃣ Yahoo Finance
        elif source.lower() == "yahoo":
            if not symbol:
                raise ValueError("Yahoo-source vereist 'symbol'.")
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?range=1d&interval=1d"
            async with httpx.AsyncClient(timeout=10) as client:
                response = await client.get(url)
                response.raise_for_status()
                data = response.json()
                close_price = float(data["chart"]["result"][0]["indicators"]["quote"][0]["close"][-1])
                return {"name": name, "value": close_price, "source": "yahoo", "link": f"https://finance.yahoo.com/quote/{symbol}"}

        # 🧾 3️⃣ Alpha Vantage (fallback)
        elif source.lower() == "alpha_vantage":
            if not symbol or not ALPHA_VANTAGE_API_KEY:
                raise ValueError("Alpha Vantage vereist 'symbol' en geldige API-key.")
            url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={ALPHA_VANTAGE_API_KEY}"
            async with httpx.AsyncClient(timeout=10) as client:
                response = await client.get(url)
                response.raise_for_status()
                data = response.json()
                ts = data.get("Time Series (Daily)")
                if not ts:
                    raise ValueError("Geen time series in Alpha Vantage response.")
                latest_day = sorted(ts.keys())[-1]
                value = float(ts[latest_day]["4. close"])
                return {"name": name, "value": value, "source": "alpha_vantage", "link": url}

        # 🔗 4️⃣ Direct link uit DB (generiek endpoint)
        elif link:
            async with httpx.AsyncClient(timeout=10) as client:
                response = await client.get(link)
                response.raise_for_status()
                data = response.json()
                # probeer eerste numerieke waarde te vinden
                val = _extract_first_number(data)
                return {"name": name, "value": val, "source": "custom_link", "link": link}

        else:
            raise ValueError(f"Onbekende source '{source}' voor indicator '{name}'")

    except Exception as e:
        logger.error(f"❌ [fetch_macro_value] Fout bij ophalen van '{name}': {e}")
        return {"name": name, "error": str(e), "value": None, "source": source, "link": link}


# ============================================
# 🔍 Helper: eerste numerieke waarde uit dict halen
# ============================================
def _extract_first_number(data):
    """Zoekt de eerste numerieke waarde in een geneste dict of lijst."""
    if isinstance(data, dict):
        for v in data.values():
            num = _extract_first_number(v)
            if num is not None:
                return num
    elif isinstance(data, list):
        for v in data:
            num = _extract_first_number(v)
            if num is not None:
                return num
    elif isinstance(data, (int, float)):
        return float(data)
    return None
