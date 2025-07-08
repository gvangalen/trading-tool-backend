import os
import json
from dotenv import load_dotenv

# ✅ .env laden
load_dotenv()

# === 🔧 Market Data (CoinGecko) ===
COINGECKO_URL = os.getenv("COINGECKO_URL")
VOLUME_URL = os.getenv("VOLUME_URL")

# ✅ Zet JSON-string om naar dict
try:
    ASSETS = json.loads(os.getenv("ASSETS_JSON", "{}"))
except json.JSONDecodeError:
    ASSETS = {}
