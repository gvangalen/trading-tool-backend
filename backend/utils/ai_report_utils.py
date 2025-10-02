# ✅ backend/utils/ai_report_utils.py

import os
import logging
from dotenv import load_dotenv
import openai

from backend.utils.setup_utils import get_latest_setup_for_symbol
from backend.utils.scoring_utils import get_scores_for_symbol
from backend.utils.ai_strategy_utils import generate_strategy_from_setup

# === ✅ Logging ===
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# === ✅ Laad .env en controleer API-key ===
load_dotenv()
api_key = os.getenv("OPENAI_API_KEY")
if not api_key:
    logger.error("❌ OPENAI_API_KEY ontbreekt in .env of omgeving.")
else:
    openai.api_key = api_key


# === ✅ Prompt genereren via OpenAI ===
def generate_section(prompt: str, retries: int = 3, model: str = "gpt-4") -> str | None:
    for attempt in range(1, retries + 1):
        try:
            response = openai.ChatCompletion.create(
                model=model,
                messages=[
                    {"role": "system", "content": "Je bent een professionele crypto-analist. Schrijf in het Nederlands."},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.7
            )
            return response.choices[0].message.content.strip()
        except Exception as e:
            logger.warning(f"⚠️ Fout bij OpenAI-aanroep (poging {attempt}/{retries}): {e}")
    logger.error("❌ Alle pogingen om sectie te genereren zijn mislukt.")
    return None


# === ✅ Prompt-helpers ===
def prompt_for_btc_summary(setup: dict, scores: dict) -> str:
    return f"""
Geef een korte samenvatting van de huidige situatie voor Bitcoin op basis van deze setup:

Setup: {setup.get('name', 'Onbekend')}
Timeframe: {setup.get('timeframe', 'Onbekend')}
Technische score: {scores.get('technical_score', 0)}
Setup score: {scores.get('setup_score', 0)}
Sentiment score: {scores.get('sentiment_score', 0)}

Gebruik duidelijke bewoording en korte zinnen. Maximaal 5 regels.
"""


def prompt_for_macro_summary(scores: dict) -> str:
    return f"""
Vat de macro-economische situatie samen voor vandaag.
Macro-score: {scores.get('macro_score', 0)}
Noem eventueel DXY, rente, inflatie, marktstress of andere belangrijke signalen.
"""


def prompt_for_setup_checklist(setup: dict) -> str:
    return f"""
Controleer of deze setup voldoet aan A+ criteria.

Setup: {setup.get('name', 'Onbekend')}
Timeframe: {setup.get('timeframe', 'Onbekend')}
Indicatoren: {setup.get('indicators', [])}

Geef een checklist-style samenvatting (✓ of ✗ per punt).
"""


def prompt_for_priorities(setup: dict, scores: dict) -> str:
    return f"""
Wat zijn de belangrijkste aandachtspunten voor deze setup vandaag?

Setup: {setup.get('name', 'Onbekend')}
Scores: {scores}
"""


def prompt_for_wyckoff_analysis(setup: dict) -> str:
    return f"""
Geef een Wyckoff-analyse op basis van deze setup.
Fase: {setup.get('wyckoff_phase', 'onbekend')}
Beschrijving: {setup.get('explanation', '')}

Is het distributie of accumulatie? Spring of test? Range of breakout?
"""


def prompt_for_recommendations(strategy: dict | None) -> str:
    if not isinstance(strategy, dict):
        return "Geen strategie beschikbaar door AI-fout."
    return f"""
Wat is het tradingadvies op basis van deze strategie?

Entry: {strategy.get('entry', 'n.v.t.')}
Targets: {strategy.get('targets', 'n.v.t.')}
Stop-loss: {strategy.get('stop_loss', 'n.v.t.')}
Uitleg: {strategy.get('explanation', 'Geen uitleg gegenereerd.')}
"""


def prompt_for_conclusion(scores: dict) -> str:
    return f"""
Vat het dagrapport samen in een slotparagraaf. Noem risico’s, kansen en aanbeveling.
Macro: {scores.get('macro_score', 0)}
Technisch: {scores.get('technical_score', 0)}
Sentiment: {scores.get('sentiment_score', 0)}
"""


def prompt_for_outlook(setup: dict) -> str:
    return f"""
Wat is de verwachting voor de komende 2–5 dagen op basis van deze setup?

Setup: {setup.get('name', 'Onbekend')}
Timeframe: {setup.get('timeframe', 'Onbekend')}
"""


# === ✅ Hoofdfunctie voor dagrapport ===
def generate_daily_report_sections(symbol: str = "BTC") -> dict:
    setup = get_latest_setup_for_symbol(symbol)
    if not isinstance(setup, dict):
        logger.error(f"❌ Setup is geen dict: {type(setup)} → waarde: {setup}")
        return {"error": "Setup data is ongeldig (geen dict)"}

    scores = get_scores_for_symbol(symbol)
    if not isinstance(scores, dict):
        logger.error(f"❌ Scores zijn geen dict: {type(scores)} → waarde: {scores}")
        scores = {}

    strategy = generate_strategy_from_setup(setup)
    if not isinstance(strategy, dict):
        logger.error(f"❌ Strategie is geen dict: {type(strategy)} → waarde: {strategy}")
        strategy = {}

    logger.info(f"📊 Setup: {setup.get('name', 'Onbekend')}, Scores: {scores}")

    return {
        "btc_summary": generate_section(prompt_for_btc_summary(setup, scores)),
        "macro_summary": generate_section(prompt_for_macro_summary(scores)),
        "setup_checklist": generate_section(prompt_for_setup_checklist(setup)),
        "priorities": generate_section(prompt_for_priorities(setup, scores)),
        "wyckoff_analysis": generate_section(prompt_for_wyckoff_analysis(setup)),
        "recommendations": generate_section(prompt_for_recommendations(strategy)),
        "conclusion": generate_section(prompt_for_conclusion(scores)),
        "outlook": generate_section(prompt_for_outlook(setup)),
        "macro_score": scores.get("macro_score", 0),
        "technical_score": scores.get("technical_score", 0),
        "setup_score": scores.get("setup_score", 0),
        "sentiment_score": scores.get("sentiment_score", 0),
    }
