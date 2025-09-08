import httpx
import logging
from backend.config.config_loader import load_macro_config

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


async def process_all_macro_indicators():
    """
    ➤ Laadt macro_config en verwerkt alle indicatoren.
    ➤ Wordt gebruikt in o.a. macro_data_api of Celery-tasks.
    """
    try:
        macro_config = load_macro_config()
    except Exception as e:
        logger.error(f"❌ [CFG01] Config laden mislukt: {e}")
        return []

    results = []
    for name, config in macro_config.items():
        try:
            result = await process_macro_indicator(name, config)
            results.append(result)
        except Exception as e:
            logger.error(f"❌ [PROC01] Indicator '{name}' verwerken mislukt: {e}")
            results.append({"name": name, "error": str(e)})

    return results


async def process_macro_indicator(name, config):
    """
    ➤ Verwerkt een enkele macro-indicator via de config.
    """
    api_url = config.get("api_url")
    extract_key = config.get("extract_key")
    rules = config.get("interpretation_rules", [])

    if not api_url or not extract_key:
        raise ValueError(f"❌ Ongeldige configuratie voor '{name}': ontbrekende api_url of extract_key")

    # ➤ API-request uitvoeren
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.get(api_url)
            response.raise_for_status()
            json_data = response.json()
    except Exception as e:
        logger.error(f"❌ [API01] API-fout bij {api_url}: {e}")
        raise

    # ➤ Speciale DXY-berekening
    if extract_key == "custom_dxy_calculation":
        try:
            rates = json_data["rates"]
            basket = ["EUR", "GBP", "JPY", "CAD", "SEK", "CHF"]
            value = sum(rates.get(cur, 1) for cur in basket) / len(basket)
        except Exception as e:
            logger.error(f"❌ [DXY01] DXY-berekening mislukt: {e}")
            raise
    else:
        value = extract_nested_value(json_data, extract_key)

    if value is None:
        raise ValueError(f"❌ Kon waarde niet extraheren voor '{name}' via key '{extract_key}'")

    interpretation, action = interpret_value(value, rules)
    score = calculate_score(value, rules)

    return {
        "name": name,
        "value": value,
        "interpretation": interpretation,
        "action": action,
        "score": score,
        "symbol": config.get("symbol"),
        "source": config.get("source"),
        "category": config.get("category"),
        "correlation": config.get("correlation"),
        "explanation": config.get("explanation"),
        "link": generate_chart_link(config.get("source"), config.get("symbol")),
    }


def extract_nested_value(data, path):
    """
    ➤ Haalt een geneste waarde uit JSON op via dot-notatie (bv: "data[0].price").
    """
    try:
        keys = path.split(".")
        for key in keys:
            if isinstance(data, list) and key.isdigit():
                data = data[int(key)]
            else:
                data = data.get(key)

        try:
            return float(data)
        except (TypeError, ValueError):
            logger.error(f"❌ Ongeldige numerieke waarde bij extractie van '{path}': {data} (type: {type(data)})")
            return None
    except Exception as e:
        logger.error(f"❌ Fout bij extractie van '{path}': {e}")
        return None


def interpret_value(value, rules):
    """
    ➤ Retourneert interpretatie en actie op basis van thresholdregels.
    """
    for rule in sorted(rules, key=lambda r: -r["threshold"]):
        if value >= rule["threshold"]:
            return rule["interpretation"], rule["action"]
    return "Unknown", "No action"


def calculate_score(value, rules):
    """
    ➤ Berekent score op basis van de positie t.o.v. thresholds.
    """
    for i, rule in enumerate(sorted(rules, key=lambda r: -r["threshold"])):
        if value >= rule["threshold"]:
            return len(rules) - i
    return 0


def generate_chart_link(source, symbol):
    """
    ➤ Genereert een URL naar een grafiek op basis van de bron en het symbool.
    """
    if not source or not symbol:
        return None

    if source == "yahoo":
        return f"https://finance.yahoo.com/quote/{symbol}"
    elif source == "tradingview":
        return f"https://www.tradingview.com/symbols/{symbol.replace(':', '')}/"
    else:
        return None
