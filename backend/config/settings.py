import os
import json
from dotenv import load_dotenv

# ✅ .env laden vanaf root-directory
load_dotenv()

# === 🔧 Market Data (CoinGecko API) ===
COINGECKO_URL = os.getenv("COINGECKO_URL")  # bv. "https://api.coingecko.com/api/v3/coins/{id}/ohlc?vs_currency=usd&days=1"
VOLUME_URL = os.getenv("VOLUME_URL")        # bv. "https://api.coingecko.com/api/v3/coins/{id}?localization=false"

# ✅ Valideer aanwezigheid
if not COINGECKO_URL or not VOLUME_URL:
    raise ValueError("❌ COINGECKO_URL of VOLUME_URL ontbreekt in .env bestand.")

# ✅ Parse JSON-string naar dict
try:
    ASSETS = json.loads(os.getenv("ASSETS_JSON", "{}"))
    if not ASSETS:
        raise ValueError("❌ ASSETS_JSON is leeg of ongeldig.")
except json.JSONDecodeError as e:
    raise ValueError(f"❌ Ongeldige ASSETS_JSON in .env: {e}")
