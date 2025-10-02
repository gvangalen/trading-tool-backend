import os
import json
import logging
import openai  # ✅ v1+ correcte import
from backend.utils.setup_utils import get_latest_setup_for_symbol
from backend.utils.scoring_utils import get_scores_for_symbol
from backend.utils.ai_strategy_utils import generate_strategy_from_setup

logger = logging.getLogger(__name__)
openai.api_key = os.getenv("OPENAI_API_KEY")  # ✅ correcte API-key

def generate_section(prompt: str) -> str:
    try:
        response = openai.chat.completions.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": "Je bent een professionele crypto-analist. Schrijf in het Nederlands."},
                {"role": "user", "content": prompt},
            ],
            max_tokens=500,
            temperature=0.7
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        logger.error(f"❌ AI fout bij prompt: {prompt[:100]}...\n➡️ {e}")
        return f"(AI-fout: {e})"

def prompt_for_btc_summary(setup: dict, scores: dict) -> str:
    return f"""
Geef een korte samenvatting van de huidige situatie voor Bitcoin op basis van deze setup:

Setup: {setup.get('name', '')}
Timeframe: {setup.get('timeframe', '')}
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

Setup: {setup.get('name', '')}
Timeframe: {setup.get('timeframe', '')}
Indicatoren: {setup.get('indicators', [])}

Geef een checklist-style samenvatting (✓ of ✗ per punt).
"""

def prompt_for_priorities(setup: dict, scores: dict) -> str:
    return f"""
Wat zijn de belangrijkste aandachtspunten voor deze setup vandaag?

Setup: {setup.get('name', '')}
Scores: {scores}
"""

def prompt_for_wyckoff_analysis(setup: dict) -> str:
    return f"""
Geef een Wyckoff-analyse op basis van deze setup.
Fase: {setup.get('wyckoff_phase', 'onbekend')}
Beschrijving: {setup.get('explanation', '')}

Is het distributie of accumulatie? Spring of test? Range of breakout?
"""

def prompt_for_recommendations(strategy: dict) -> str:
    return f"""
Wat is het tradingadvies op basis van deze strategie?

Entry: {strategy.get('entry')}
Targets: {strategy.get('targets')}
Stop-loss: {strategy.get('stop_loss')}
Uitleg: {strategy.get('explanation')}
"""

def prompt_for_conclusion(scores: dict) -> str:
    return f"""
Vat het dagrapport samen in een slotparagraaf. Noem risico’s, kansen en aanbeveling.
Macro: {scores.get('macro_score')}
Technisch: {scores.get('technical_score')}
Sentiment: {scores.get('sentiment_score')}
"""

def prompt_for_outlook(setup: dict) -> str:
    return f"""
Wat is de verwachting voor de komende 2–5 dagen op basis van deze setup?

Setup: {setup.get('name')}
Timeframe: {setup.get('timeframe')}
"""

def generate_daily_report_sections(symbol: str = "BTC") -> dict:
    setup = get_latest_setup_for_symbol(symbol)
    if not isinstance(setup, dict):
        logger.error(f"❌ Setup is geen dict: {type(setup)} → waarde: {setup}")
        return {"error": "Setup data is ongeldig (geen dict)"}

    strategy = generate_strategy_from_setup(setup)

    # 🔄 Zorg dat strategy een dict is
    if isinstance(strategy, str):
        try:
            strategy = json.loads(strategy)
        except Exception as e:
            logger.error(f"❌ Strategie is geen JSON: {e} → waarde: {strategy}")
            strategy = {}

    scores = get_scores_for_symbol(symbol)

    return {
        "btc_summary": {"text": generate_section(prompt_for_btc_summary(setup, scores))},
        "macro_summary": {"text": generate_section(prompt_for_macro_summary(scores))},
        "setup_checklist": {"text": generate_section(prompt_for_setup_checklist(setup))},
        "priorities": {"text": generate_section(prompt_for_priorities(setup, scores))},
        "wyckoff_analysis": {"text": generate_section(prompt_for_wyckoff_analysis(setup))},
        "recommendations": {"text": generate_section(prompt_for_recommendations(strategy))},
        "conclusion": {"text": generate_section(prompt_for_conclusion(scores))},
        "outlook": {"text": generate_section(prompt_for_outlook(setup))},
        "macro_score": scores.get("macro_score", 0),
        "technical_score": scores.get("technical_score", 0),
        "setup_score": scores.get("setup_score", 0),
        "sentiment_score": scores.get("sentiment_score", 0),
    }
