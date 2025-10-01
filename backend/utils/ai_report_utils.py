import os
from openai import OpenAI
from backend.utils.setup_utils import get_latest_setup_for_symbol
from backend.utils.scoring_utils import get_scores_for_symbol
from backend.utils.ai_strategy_utils import generate_strategy_from_setup

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def generate_section(prompt: str) -> str:
    try:
        response = client.chat.completions.create(
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
        return f"(AI-fout: {e})"

def prompt_for_btc_summary(setup, scores):
    return f"""
Geef een korte samenvatting van de huidige situatie voor Bitcoin op basis van deze setup:

Setup: {setup.get('name', '')}
Timeframe: {setup.get('timeframe')}
Technische score: {scores.get('technical_score', 0)}
Setup score: {scores.get('setup_score', 0)}
Sentiment score: {scores.get('sentiment_score', 0)}

Gebruik duidelijke bewoording en korte zinnen. Maximaal 5 regels.
"""

def prompt_for_macro_summary(scores):
    return f"""
Vat de macro-economische situatie samen voor vandaag.
Macro-score: {scores.get('macro_score', 0)}
Noem eventueel DXY, rente, inflatie, marktstress of andere belangrijke signalen.
"""

def prompt_for_setup_checklist(setup):
    return f"""
Controleer of deze setup voldoet aan A+ criteria.

Setup: {setup.get('name')}
Timeframe: {setup.get('timeframe')}
Indicatoren: {setup.get('indicators', [])}

Geef een checklist-style samenvatting (✓ of ✗ per punt).
"""

def prompt_for_priorities(setup, scores):
    return f"""
Wat zijn de belangrijkste aandachtspunten voor deze setup vandaag?

Setup: {setup.get('name')}
Scores: {scores}
"""

def prompt_for_wyckoff_analysis(setup):
    return f"""
Geef een Wyckoff-analyse op basis van deze setup.
Fase: {setup.get('wyckoff_phase', 'onbekend')}
Beschrijving: {setup.get('explanation', '')}

Is het distributie of accumulatie? Spring of test? Range of breakout?
"""

def prompt_for_recommendations(strategy):
    return f"""
Wat is het tradingadvies op basis van deze strategie?

Entry: {strategy.get('entry')}
Targets: {strategy.get('targets')}
Stop-loss: {strategy.get('stop_loss')}
Uitleg: {strategy.get('explanation')}
"""

def prompt_for_conclusion(scores):
    return f"""
Vat het dagrapport samen in een slotparagraaf. Noem risico’s, kansen en aanbeveling.
Macro: {scores.get('macro_score')}
Technisch: {scores.get('technical_score')}
Sentiment: {scores.get('sentiment_score')}
"""

def prompt_for_outlook(setup):
    return f"""
Wat is de verwachting voor de komende 2–5 dagen op basis van deze setup?

Setup: {setup.get('name')}
Timeframe: {setup.get('timeframe')}
"""

def generate_daily_report_sections(symbol="BTC") -> dict:
    setup = get_latest_setup_for_symbol(symbol)
    strategy = generate_strategy_from_setup(setup)
    scores = get_scores_for_symbol(symbol)

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
