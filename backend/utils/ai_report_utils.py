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


# === ✅ Prompt genereren via OpenAI (met logging + retries) ===
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

            # 🧪 Log de volledige OpenAI response
            logger.info(f"🔍 Volledige OpenAI response: {response}")

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
def prompt_for_macro_summary(scores: dict) -> str:
    return f"""Vat de macro-economische situatie samen voor vandaag.
Macro-score: {scores.get('macro_score', 0)}
Noem eventueel DXY, rente, inflatie, marktstress of andere belangrijke signalen."""


# === ✅ Rapport-generator voor tests (alleen macro_summary actief)
def generate_daily_report_sections(symbol: str = "BTC") -> dict:
    logger.info(f"📥 Start rapportgeneratie voor: {symbol}")

    setup_raw = get_latest_setup_for_symbol(symbol)
    setup = ensure_dict(setup_raw, context="setup")

    scores_raw = get_scores_for_symbol(symbol)
    scores = ensure_dict(scores_raw, context="scores")

    strategy_raw = generate_strategy_from_setup(setup)
    strategy = ensure_dict(strategy_raw, fallback={}, context="strategy")

    # 🧪 Debug log – toon alle ruwe data
    logger.info(f"🧪 setup = {json.dumps(setup, indent=2)}")
    logger.info(f"🧪 scores = {json.dumps(scores, indent=2)}")
    logger.info(f"🧪 strategy = {json.dumps(strategy, indent=2)}")

    # 🔍 TEST: alleen macro_summary actief
    report = {
        "macro_summary": generate_section(prompt_for_macro_summary(scores)),
        # "btc_summary": generate_section(prompt_for_btc_summary(setup, scores)),
        # "setup_checklist": generate_section(prompt_for_setup_checklist(setup)),
        # "priorities": generate_section(prompt_for_priorities(setup, scores)),
        # "wyckoff_analysis": generate_section(prompt_for_wyckoff_analysis(setup)),
        # "recommendations": generate_section(prompt_for_recommendations(strategy)),
        # "conclusion": generate_section(prompt_for_conclusion(scores)),
        # "outlook": generate_section(prompt_for_outlook(setup)),
        "macro_score": scores.get("macro_score", 0),
        "technical_score": scores.get("technical_score", 0),
        "setup_score": scores.get("setup_score", 0),
        "sentiment_score": scores.get("sentiment_score", 0),
    }

    logger.info("✅ Dagrapport gegenereerd met macro_summary.")
    return report


# === ✅ Test handmatig draaien vanaf CLI
if __name__ == "__main__":
    report = generate_daily_report_sections("BTC")
    print(json.dumps(report, indent=2, ensure_ascii=False))
