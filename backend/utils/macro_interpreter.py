# utils/macro_interpreter.py

import httpx
import logging

logger = logging.getLogger(__name__)

async def process_macro_indicator(name, config):
    api_url = config.get("api_url")
    extract_key = config.get("extract_key")
    rules = config.get("interpretation_rules", [])

    if not api_url or not extract_key:
        raise ValueError("Ongeldige configuratie")

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.get(api_url)
            response.raise_for_status()
            json_data = response.json()
    except Exception as e:
        logger.error(f"❌ API-fout bij {api_url}: {e}")
        raise

    # Custom DXY
    if extract_key == "custom_dxy_calculation":
        try:
            rates = json_data["rates"]
            basket = ["EUR", "GBP", "JPY", "CAD", "SEK", "CHF"]
            value = sum(rates.get(cur, 1) for cur in basket) / len(basket)
        except Exception as e:
            logger.error(f"❌ DXY berekening mislukt: {e}")
            raise
    else:
        value = extract_nested_value(json_data, extract_key)

    if value is None:
        raise ValueError("Kon waarde niet extraheren")

    interpretation, action = interpret_value(value, rules)

    return {
        "name": name,
        "value": value,
        "interpretation": interpretation,
        "action": action
    }

def extract_nested_value(data, path):
    try:
        keys = path.split(".")
        for key in keys:
            if isinstance(data, list) and key.isdigit():
                data = data[int(key)]
            else:
                data = data.get(key)
        return float(data)
    except Exception as e:
        logger.error(f"❌ Fout bij extractie van '{path}': {e}")
        return None

def interpret_value(value, rules):
    for rule in sorted(rules, key=lambda r: -r["threshold"]):
        if value >= rule["threshold"]:
            return rule["interpretation"], rule["action"]
    return "Unknown", "No action"
