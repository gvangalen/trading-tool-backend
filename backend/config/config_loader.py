import os
import json
import logging

logger = logging.getLogger(__name__)

# ✅ BASE_DIR wijst naar /backend, ongeacht waar dit bestand staat
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_DIR = os.path.join(BASE_DIR, "config")

def load_config(filename: str):
    path = os.path.join(CONFIG_DIR, filename)
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"❌ [CFG01] Config laden mislukt: {e}")
        raise RuntimeError(f"❌ [CFG01] Configbestand ongeldig of ontbreekt: {e}")

# 🔁 Specifieke loaders (optioneel)
def load_macro_config():
    return load_config("macro_indicators_config.json")

def load_technical_config():
    return load_config("technical_indicators_config.json")

def load_market_config():
    return load_config("market_data_config.json")
