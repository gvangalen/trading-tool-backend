import os
import logging
import json
from dotenv import load_dotenv
from openai import OpenAI, OpenAIError

from backend.utils.setup_utils import get_latest_setup_for_symbol
from backend.utils.scoring_utils import get_scores_for_symbol
from backend.utils.ai_strategy_utils import generate_strategy_from_setup

# === ✅ Logging ===
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# === ✅ OpenAI client initialiseren ===
load_dotenv()
api_key = os.getenv("OPENAI_API_KEY")
if not api_key:
    logger.error("❌ OPENAI_API_KEY ontbreekt in .env of omgeving.")
client = OpenAI(api_key=api_key)

DEFAULT_MODEL = os.getenv("OPENAI_MODEL", "gpt-3.5-turbo")


# === ✅ Helper: veilig casten naar dict ===
def ensure_dict(obj, fallback=None, context=""):
    if isinstance(obj, dict):
        return obj
    try:
        if isinstance(obj, str):
            obj = obj.strip()
            if obj.startswith("{"):
                return json.loads(obj)
            else:
                logger.warning(f"⚠️ {context} was string zonder JSON: '{obj}'")
                return fallback or {"error": obj}
    except Exception as e:
        logger.warning(f"❌ Kon geen dict maken van {context}: {e}")
    logger.warning(f"⚠️ {context} is geen dict: {obj}")
    return fallback or {}


# === ✅ Prompt genereren via OpenAI (met retries en logging) ===
def generate_section(prompt: str, retries: int = 3, model: str = DEFAULT_MODEL) -> str:
    for attempt in range(1, retries + 1):
        try:
            logger.info(f"🔍 [AI Prompt Attempt {attempt}] Prompt (eerste 250 tekens): {prompt[:250]}")
            response = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": "Je bent een professionele crypto-analist. Schrijf in het Nederlands."},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.7
            )
            content = response.choices[0].message.content.strip()
            logger.info(f"✅ [OpenAI] Antwoord gegenereerd (lengte: {len(content)}): {content[:200]}...")
            if not content:
                logger.warning(f"⚠️ Lege response van OpenAI bij poging {attempt}")
                continue
            return content
        except OpenAIError as e:
            logger.warning(f"⚠️ OpenAI fout bij poging {attempt}/{retries}: {e}")
        except Exception as e:
            logger.warning(f"⚠️ Onverwachte fout bij OpenAI-aanroep (poging {attempt}/{retries}): {e}")
    logger.error("❌ Alle pogingen om sectie te genereren zijn mislukt.")
    return "Fout: AI-generatie mislukt. Check limiet of logs."


# === ✅ Prompt templates ===
def prompt_for_btc_summary(setup: dict, scores: dict) -> str:
    return f"""Geef een korte samenvatting van de huidige situatie voor Bitcoin op basis van deze setup:

Setup: {setup.get('name', 'Onbekend')}
Timeframe: {setup.get('timeframe', 'Onbekend')}
Technische score: {scores.get('technical_score', 0)}
Setup score: {scores.get('setup_score', 0)}
Sentiment score: {scores.get('sentiment_score', 0)}

Gebruik duidelijke bewoording en korte zinnen. Maximaal 5 regels."""


def prompt_for_macro_summary(scores: dict) -> str:
    return f"""Vat de macro-economische situatie samen voor vandaag.
Macro-score: {scores.get('macro_score', 0)}
Noem eventueel DXY, rente, inflatie, marktstress of andere belangrijke signalen."""


def prompt_for_setup_checklist(setup: dict) -> str:
    return f"""Controleer of deze setup voldoet aan A+ criteria.
Setup: {setup.get('name', 'Onbekend')}
Timeframe: {setup.get('timeframe', 'Onbekend')}
Indicatoren: {setup.get('indicators', [])}
Geef een checklist-style samenvatting (✓ of ✗ per punt)."""


def prompt_for_priorities(setup: dict, scores: dict) -> str:
    return f"""Wat zijn de belangrijkste aandachtspunten voor deze setup vandaag?
Setup: {setup.get('name', 'Onbekend')}
Scores: {scores}"""


def prompt_for_wyckoff_analysis(setup: dict) -> str:
    return f"""Geef een Wyckoff-analyse op basis van deze setup.
Fase: {setup.get('wyckoff_phase', 'onbekend')}
Beschrijving: {setup.get('explanation', '')}
Is het distributie of accumulatie? Spring of test? Range of breakout?"""


def prompt_for_recommendations(strategy: dict | None) -> str:
    strategy = ensure_dict(strategy, fallback={}, context="strategy (in prompt_for_recommendations)")
    entry = strategy.get("entry", "n.v.t.")
    targets = strategy.get("targets", "n.v.t.")
    stop_loss = strategy.get("stop_loss", "n.v.t.")
    explanation = strategy.get("explanation", "Strategie niet beschikbaar.")
    return f"""Wat is het tradingadvies op basis van deze strategie?

Entry: {entry}
Targets: {targets}
Stop-loss: {stop_loss}
Uitleg: {explanation}"""


def prompt_for_conclusion(scores: dict) -> str:
    return f"""Vat het dagrapport samen in een slotparagraaf. Noem risico’s, kansen en aanbeveling.
Macro: {scores.get('macro_score', 0)}
Technisch: {scores.get('technical_score', 0)}
Sentiment: {scores.get('sentiment_score', 0)}"""


def prompt_for_outlook(setup: dict) -> str:
    return f"""Wat is de verwachting voor de komende 2–5 dagen op basis van deze setup?
Setup: {setup.get('name', 'Onbekend')}
Timeframe: {setup.get('timeframe', 'Onbekend')}"""


# === ✅ Dagrapportgenerator ===
def generate_daily_report_sections(symbol: str = "BTC") -> dict:
    logger.info(f"📥 Start rapportgeneratie voor: {symbol}")

    setup_raw = get_latest_setup_for_symbol(symbol)
    setup = ensure_dict(setup_raw, context="setup")

    scores_raw = get_scores_for_symbol(symbol)
    scores = ensure_dict(scores_raw, context="scores")

    strategy_raw = generate_strategy_from_setup(setup)
    logger.info(f"🧪 Ruwe strategy_raw = {strategy_raw} ({type(strategy_raw)})")
    strategy = ensure_dict(strategy_raw, fallback={
        "entry": "n.v.t.",
        "targets": "n.v.t.",
        "stop_loss": "n.v.t.",
        "explanation": "Strategie kon niet gegenereerd worden."
    }, context="strategy")

    report = {
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

    logger.info("✅ Dagrapport gegenereerd en klaar voor opslag.")
    return report


# === ✅ Test lokaal ===
if __name__ == "__main__":
    report = generate_daily_report_sections("BTC")
    print(json.dumps(report, indent=2, ensure_ascii=False))
