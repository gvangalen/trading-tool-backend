import logging
import json
import os
from dotenv import load_dotenv
from openai import OpenAI, OpenAIError

# ✅ .env laden
load_dotenv()
DEFAULT_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# ✅ Logging configureren
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# =====================================================
# 🧰 Helper: veilige JSON/dict conversie
# =====================================================
def ensure_dict(obj, context=""):
    if isinstance(obj, dict):
        return obj
    if isinstance(obj, str):
        try:
            parsed = json.loads(obj)
            if isinstance(parsed, dict):
                return parsed
            else:
                logger.warning(f"⚠️ {context}: JSON geladen maar is geen dict.")
                return {}
        except json.JSONDecodeError:
            logger.error(f"❌ {context}: Kan string niet parsen als JSON:\n{obj}")
            return {}
    logger.warning(f"⚠️ {context}: Ongeldig type ({type(obj)}), verwacht dict of str.")
    return {}


# =====================================================
# 🧠 Strategie genereren via AI
# =====================================================
def generate_strategy_from_setup(setup: dict | str, model: str = DEFAULT_MODEL) -> dict:
    setup = ensure_dict(setup, context="generate_strategy_from_setup")

    try:
        setup_name = setup.get("name", "Onbekende setup")
        trend = setup.get("trend", "?")
        timeframe = setup.get("timeframe", "1D")
        symbol = setup.get("symbol", "BTC")

        indicators = (
            ", ".join(setup.get("indicators", []))
            if isinstance(setup.get("indicators"), list)
            else "Geen"
        )

        # ✅ Scores komen uit database of AI-rapport
        macro_score = setup.get("macro_score", 0)
        technical_score = setup.get("technical_score", 0)
        market_score = setup.get("market_score", 0)

        prompt = f"""
Je bent een professionele crypto swingtrader. Genereer een strategie voor de volgende setup:

Setup naam: {setup_name}
Asset: {symbol} ({timeframe})
Trend: {trend}
Indicatoren: {indicators}
Scores: Macro={macro_score}, Technisch={technical_score}, Markt={market_score}

Format:
- Entry prijs
- Targets (meerdere)
- Stop-loss
- R/R Ratio
- Uitleg (korte AI-analyse over waarom deze strategie werkt)

Antwoord in correct JSON-formaat met deze keys:
entry, targets (lijst), stop_loss, risk_reward, explanation
"""

        logger.info(f"🧠 Strategie-prompt voor {setup_name} (eerste 250 tekens): {prompt[:250]}")

        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": "Je bent een ervaren crypto-analist en trader. Antwoord in JSON."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.7
        )

        raw_content = response.choices[0].message.content.strip()
        logger.info(f"✅ AI-antwoord ontvangen (lengte: {len(raw_content)}): {raw_content[:180]}...")

        try:
            strategy = json.loads(raw_content)
            if not isinstance(strategy, dict):
                raise ValueError("Response is geen dictionary")

            logger.info(f"✅ Strategie succesvol geparsed voor setup '{setup_name}'")
            return strategy

        except (json.JSONDecodeError, ValueError):
            logger.error(f"❌ JSON parse-fout voor setup '{setup_name}':\n{raw_content}")
            return {
                "entry": "n.v.t.",
                "targets": [],
                "stop_loss": "n.v.t.",
                "risk_reward": "n.v.t.",
                "explanation": "AI-output kon niet als JSON worden gelezen."
            }

    except OpenAIError as e:
        logger.error(f"❌ OpenAI-fout bij setup '{setup.get('name', '?')}': {e}")
    except Exception as e:
        logger.error(f"❌ Fout bij strategie-generatie voor setup '{setup.get('name', '?')}': {e}")

    return {
        "entry": "n.v.t.",
        "targets": [],
        "stop_loss": "n.v.t.",
        "risk_reward": "n.v.t.",
        "explanation": "Strategie kon niet worden gegenereerd door een fout."
    }


# =====================================================
# 📊 Strategie-advies voor meerdere setups
# =====================================================
def generate_strategy_advice(setups, macro_score, technical_score, market_data):
    """
    Ontvangt een lijst met setups en vult macro/technical/market scores aan
    (nu DB-driven, niet meer uit config). Genereert per setup een AI-strategie.
    """
    strategies = []

    if not isinstance(setups, list):
        logger.error(f"❌ Ongeldig type voor setups: {type(setups)}. Verwacht lijst.")
        return strategies

    for setup in setups:
        setup = ensure_dict(setup, context="generate_strategy_advice")

        setup["macro_score"] = macro_score
        setup["technical_score"] = technical_score
        setup["market_score"] = market_data.get("change_24h", 0) if isinstance(market_data, dict) else 0

        strategy = generate_strategy_from_setup(setup)

        strategies.append({
            "setup_name": setup.get("name"),
            "symbol": setup.get("symbol", "BTC"),
            "timeframe": setup.get("timeframe", "1D"),
            "trend": setup.get("trend", "?"),
            "strategy": strategy,
        })

    logger.info(f"✅ {len(strategies)} strategieën succesvol gegenereerd.")
    return strategies
