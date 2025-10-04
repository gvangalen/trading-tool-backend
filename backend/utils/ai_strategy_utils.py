# ✅ backend/utils/ai_strategy_utils.py

import logging
import json
import os
from dotenv import load_dotenv
from openai import OpenAI, OpenAIError

# ✅ .env laden
load_dotenv()

# ✅ OpenAI-client instellen (v1+)
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# ✅ Logging configureren
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def generate_strategy_from_setup(setup: dict | str) -> dict:
    """
    Genereert een strategie vanuit een setup met OpenAI.
    Retourneert ALTIJD een dict (ook bij fouten of JSON-problemen).
    """
    if not isinstance(setup, dict):
        logger.error(f"❌ Ongeldig type voor setup: {type(setup)}. Verwacht dict.")
        return {
            "entry": "n.v.t.",
            "targets": [],
            "stop_loss": "n.v.t.",
            "risk_reward": "n.v.t.",
            "explanation": "Strategie kon niet worden gegenereerd – setup is ongeldig."
        }

    try:
        setup_name = setup.get("name", "Onbekende setup")
        trend = setup.get("trend", "?")
        timeframe = setup.get("timeframe", "1D")
        symbol = setup.get("symbol", "BTC")
        indicators = ", ".join(setup.get("indicators", [])) if isinstance(setup.get("indicators"), list) else "Geen"
        macro_score = setup.get("macro_score", "?")
        technical_score = setup.get("technical_score", "?")
        sentiment_score = setup.get("sentiment_score", "?")

        prompt = f"""
Je bent een professionele crypto swingtrader. Genereer een strategie voor de volgende setup:

Setup naam: {setup_name}
Asset: {symbol} ({timeframe})
Trend: {trend}
Indicatoren: {indicators}
Scores: Macro={macro_score}, Technisch={technical_score}, Sentiment={sentiment_score}

Format:
- Entry prijs
- Targets (meerdere)
- Stop-loss
- R/R Ratio
- Uitleg (korte AI-analyse over waarom deze strategie werkt)

Antwoord in correct JSON-formaat met deze keys:
entry, targets (lijst), stop_loss, risk_reward, explanation
"""

        response = client.chat.completions.create(
            model="gpt-4",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.7
        )

        raw_content = response.choices[0].message.content.strip()

        try:
            strategy = json.loads(raw_content)

            if not isinstance(strategy, dict):
                raise ValueError("Response is geen dictionary")

            logger.info(f"✅ Strategie gegenereerd voor setup '{setup_name}'")
            return strategy

        except (json.JSONDecodeError, ValueError) as parse_error:
            logger.error(f"❌ JSON parse-fout voor setup '{setup_name}':\n{raw_content}")
            return {
                "entry": "n.v.t.",
                "targets": [],
                "stop_loss": "n.v.t.",
                "risk_reward": "n.v.t.",
                "explanation": "AI-output kon niet als JSON worden gelezen."
            }

    except OpenAIError as e:
        logger.error(f"❌ OpenAI fout bij setup '{setup.get('name', '?')}': {e}")
    except Exception as e:
        logger.error(f"❌ Fout bij strategie-generatie voor setup '{setup.get('name', '?')}': {e}")

    return {
        "entry": "n.v.t.",
        "targets": [],
        "stop_loss": "n.v.t.",
        "risk_reward": "n.v.t.",
        "explanation": "Strategie kon niet worden gegenereerd door een fout."
    }


def generate_strategy_advice(setups, macro_score, technical_score, market_data):
    """
    Genereert een lijst van AI-strategieën op basis van setups + scores + marktdata.
    Retourneert lijst met strategieën per setup.
    """
    strategies = []

    if not isinstance(setups, list):
        logger.error(f"❌ Ongeldig type voor setups: {type(setups)}. Verwacht lijst.")
        return strategies

    for setup in setups:
        if not isinstance(setup, dict):
            logger.warning(f"⚠️ Setup is geen dict: {type(setup)} → wordt overgeslagen")
            continue

        setup["macro_score"] = macro_score
        setup["technical_score"] = technical_score
        setup["sentiment_score"] = setup.get("score_breakdown", {}).get("sentiment", {}).get("score", 0)

        strategy = generate_strategy_from_setup(setup)

        strategies.append({
            "setup_name": setup.get("name"),
            "symbol": setup.get("symbol"),
            "timeframe": setup.get("timeframe"),
            "trend": setup.get("trend"),
            "strategy": strategy,
        })

    return strategies
