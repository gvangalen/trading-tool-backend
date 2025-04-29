import logging
import openai
import json
import os

# ✅ OpenAI API-sleutel instellen via omgeving
openai.api_key = os.getenv("OPENAI_API_KEY")

# ✅ Logging configureren
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def generate_strategy_from_setup(setup: dict) -> dict | None:
    """
    Genereert automatisch een strategie met GPT op basis van een setup.

    Verwacht een setup-dict met minimaal:
    - name, trend, timeframe, symbol, indicators, macro_score, technical_score, sentiment_score

    Returns:
        dict met keys: entry, targets (list), stop_loss, risk_reward, explanation
    """
    try:
        setup_name = setup.get("name", "Onbekende setup")
        trend = setup.get("trend", "?")
        timeframe = setup.get("timeframe", "1D")
        symbol = setup.get("symbol", "BTC")
        indicators = ", ".join(setup.get("indicators", [])) or "Geen"
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

        response = openai.ChatCompletion.create(
            model="gpt-4",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.7
        )

        raw_content = response["choices"][0]["message"]["content"].strip()

        # ✅ JSON proberen te parsen
        try:
            strategy = json.loads(raw_content)
            logger.info(f"✅ Strategie gegenereerd voor setup '{setup_name}'")
            return strategy
        except json.JSONDecodeError:
            logger.error(f"❌ JSON parse-fout voor setup '{setup_name}':\n{raw_content}")
            return None

    except Exception as e:
        logger.error(f"❌ Fout bij strategie-generatie voor setup '{setup.get('name')}': {e}")
        return None
