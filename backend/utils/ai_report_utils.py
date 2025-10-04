import os
import logging
from dotenv import load_dotenv
from openai import OpenAI, OpenAIError  # ✅ v1+ import

from backend.utils.setup_utils import get_latest_setup_for_symbol
from backend.utils.scoring_utils import get_scores_for_symbol
from backend.utils.ai_strategy_utils import generate_strategy_from_setup

# === ✅ Logging ===
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# === ✅ Laad .env en initialiseer OpenAI client ===
load_dotenv()
api_key = os.getenv("OPENAI_API_KEY")
if not api_key:
    logger.error("❌ OPENAI_API_KEY ontbreekt in .env of omgeving.")
client = OpenAI(api_key=api_key)

# === ✅ Prompt genereren via OpenAI (v1+ syntax) ===
def generate_section(prompt: str, retries: int = 3, model: str = "gpt-4") -> str | None:
    for attempt in range(1, retries + 1):
        try:
            logger.debug(f"[DEBUG] OpenAI prompt attempt {attempt}: {prompt[:200]}...")
            response = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": "Je bent een professionele crypto-analist. Schrijf in het Nederlands."},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.7
            )
            content = response.choices[0].message.content.strip()
            if not content:
                logger.warning(f"⚠️ Lege response van OpenAI bij poging {attempt}")
                continue
            return content
        except OpenAIError as e:
            logger.warning(f"⚠️ OpenAI fout bij poging {attempt}/{retries}: {e}")
        except Exception as e:
            logger.warning(f"⚠️ Overige fout bij OpenAI-aanroep (poging {attempt}/{retries}): {e}")
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
        logger.warning(f"⚠️ Strategie is geen dict → type={type(strategy)} waarde={strategy}")
        return "Geen strategie beschikbaar door AI-fout."

    entry = strategy.get("entry", "n.v.t.")
    targets = strategy.get("targets", "n.v.t.")
    stop_loss = strategy.get("stop_loss", "n.v.t.")
    explanation = strategy.get("explanation", "Geen uitleg gegenereerd.")

    return f"""
Wat is het tradingadvies op basis van deze strategie?

Entry: {entry}
Targets: {targets}
Stop-loss: {stop_loss}
Uitleg: {explanation}
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
    logger.info(f"[DEBUG] Setup type={type(setup)} value={setup}")
    if not isinstance(setup, dict):
        logger.error(f"❌ Setup is geen dict: {type(setup)} → waarde: {setup}")
        return {"error": "Setup data is ongeldig (geen dict)"}

    scores = get_scores_for_symbol(symbol)
    logger.info(f"[DEBUG] Scores type={type(scores)} value={scores}")
    if not isinstance(scores, dict):
        logger.error(f"❌ Scores zijn geen dict: {type(scores)} → waarde: {scores}")
        scores = {}

    strategy = generate_strategy_from_setup(setup)
    logger.info(f"[DEBUG] Strategy type={type(strategy)} value={strategy}")
    if not isinstance(strategy, dict):
        logger.warning(f"⚠️ Strategie is geen dict, type: {type(strategy)} → waarde: {strategy}")
        strategy = {
            "entry": "n.v.t.",
            "targets": "n.v.t.",
            "stop_loss": "n.v.t.",
            "explanation": "Strategie kon niet gegenereerd worden."
        }

    # Debug: log alle prompts
    btc_prompt = prompt_for_btc_summary(setup, scores)
    macro_prompt = prompt_for_macro_summary(scores)
    checklist_prompt = prompt_for_setup_checklist(setup)
    priorities_prompt = prompt_for_priorities(setup, scores)
    wyckoff_prompt = prompt_for_wyckoff_analysis(setup)
    recommendations_prompt = prompt_for_recommendations(strategy)  # ✅ GEEN OpenAI-call meer hier
    conclusion_prompt = prompt_for_conclusion(scores)
    outlook_prompt = prompt_for_outlook(setup)

    logger.info(f"📊 Setup: {setup.get('name', 'Onbekend')}, Scores: {scores}")

    return {
        "btc_summary": generate_section(btc_prompt) or "Samenvatting niet beschikbaar.",
        "macro_summary": generate_section(macro_prompt) or "Macro-analyse ontbreekt.",
        "setup_checklist": generate_section(checklist_prompt) or "Geen checklist beschikbaar.",
        "priorities": generate_section(priorities_prompt) or "Geen prioriteiten gegenereerd.",
        "wyckoff_analysis": generate_section(wyckoff_prompt) or "Wyckoff-analyse ontbreekt.",
        "recommendations": recommendations_prompt or "Geen aanbevelingen beschikbaar.",  # ✅ FIXED
        "conclusion": generate_section(conclusion_prompt) or "Geen conclusie beschikbaar.",
        "outlook": generate_section(outlook_prompt) or "Geen vooruitblik beschikbaar.",
        "macro_score": scores.get("macro_score", 0),
        "technical_score": scores.get("technical_score", 0),
        "setup_score": scores.get("setup_score", 0),
        "sentiment_score": scores.get("sentiment_score", 0),
    }
