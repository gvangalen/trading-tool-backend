import os
import logging
import json
from dotenv import load_dotenv
from openai import OpenAI

from backend.utils.setup_utils import get_latest_setup_for_symbol
from backend.utils.ai_strategy_utils import generate_strategy_from_setup
from backend.utils.json_utils import sanitize_json_input
from backend.utils.db import get_db_connection

# === ✅ Logging naar bestand + console
LOG_FILE = "/tmp/daily_report_debug.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def log_and_print(msg: str):
    """Logt én print altijd, onafhankelijk van Celery of PM2."""
    logger.info(msg)
    try:
        with open(LOG_FILE, "a") as f:
            f.write(msg + "\n")
    except Exception:
        pass
    print(msg)


# === ✅ OpenAI client initialiseren
load_dotenv()
api_key = os.getenv("OPENAI_API_KEY")
if not api_key:
    log_and_print("❌ OPENAI_API_KEY ontbreekt in .env of omgeving.")
client = OpenAI(api_key=api_key)

DEFAULT_MODEL = os.getenv("OPENAI_MODEL", "gpt-3.5-turbo")


def safe_get(obj, key, fallback="–"):
    if isinstance(obj, dict):
        return obj.get(key, fallback)
    return fallback


# ✅ Nieuw: Haal scores uit de database
def get_scores_from_db(symbol: str):
    """Haalt de meest recente scores op uit daily_scores-tabel."""
    conn = get_db_connection()
    if not conn:
        log_and_print("❌ Kan geen DB-verbinding maken voor scores.")
        return {}

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT macro_score, technical_score, setup_score, sentiment_score
                FROM daily_scores
                WHERE symbol = %s
                ORDER BY report_date DESC
                LIMIT 1
            """, (symbol,))
            row = cur.fetchone()
            if row:
                return {
                    "macro_score": row[0],
                    "technical_score": row[1],
                    "setup_score": row[2],
                    "sentiment_score": row[3],
                }
            else:
                log_and_print("⚠️ Geen scores gevonden in daily_scores.")
                return {}
    except Exception as e:
        log_and_print(f"❌ Fout bij ophalen van scores uit DB: {e}")
        return {}
    finally:
        conn.close()


def generate_section(prompt: str, retries: int = 3, model: str = DEFAULT_MODEL) -> str:
    for attempt in range(1, retries + 3):
        try:
            log_and_print(f"🔍 [AI Attempt {attempt}] Prompt (eerste 200): {prompt[:200]}")
            response = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": "Je bent een professionele crypto-analist. Schrijf in het Nederlands."},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.7
            )
            content = response.choices[0].message.content.strip()
            log_and_print(f"✅ [AI Response] Lengte: {len(content)} Tekst: {content[:180]}...")
            if not content:
                log_and_print("⚠️ Lege AI-response, probeer opnieuw...")
                continue
            return content
        except Exception as e:
            log_and_print(f"⚠️ Fout bij OpenAI poging {attempt}: {e}")
    log_and_print("❌ Alle AI-pogingen mislukt.")
    return "Fout: AI-generatie mislukt."


# === ✅ Prompt templates
def prompt_for_btc_summary(setup, scores) -> str:
    return f"""Geef een korte samenvatting van de huidige situatie voor Bitcoin:

Setup: {safe_get(setup, 'name')}
Timeframe: {safe_get(setup, 'timeframe')}
Technische score: {safe_get(scores, 'technical_score', 0)}
Setup score: {safe_get(scores, 'setup_score', 0)}
Sentiment score: {safe_get(scores, 'sentiment_score', 0)}"""


def prompt_for_macro_summary(scores) -> str:
    return f"""Vat de macro-economische situatie samen.
Macro-score: {safe_get(scores, 'macro_score', 0)}"""


def prompt_for_setup_checklist(setup) -> str:
    return f"""Controleer A+ criteria.
Setup: {safe_get(setup, 'name')}
Timeframe: {safe_get(setup, 'timeframe')}
Indicatoren: {safe_get(setup, 'indicators', [])}"""


def prompt_for_priorities(setup, scores) -> str:
    return f"""Belangrijkste aandachtspunten vandaag:
Setup: {safe_get(setup, 'name')}
Scores: {scores}"""


def prompt_for_wyckoff_analysis(setup) -> str:
    return f"""Wyckoff-analyse:
Fase: {safe_get(setup, 'wyckoff_phase')}
Beschrijving: {safe_get(setup, 'explanation')}"""


def prompt_for_recommendations(strategy) -> str:
    return f"""Tradingadvies:
Entry: {safe_get(strategy, 'entry')}
Targets: {safe_get(strategy, 'targets')}
Stop-loss: {safe_get(strategy, 'stop_loss')}
Uitleg: {safe_get(strategy, 'explanation')}"""


def prompt_for_conclusion(scores) -> str:
    return f"""Slotconclusie:
Macro: {safe_get(scores, 'macro_score', 0)}
Technisch: {safe_get(scores, 'technical_score', 0)}
Sentiment: {safe_get(scores, 'sentiment_score', 0)}"""


def prompt_for_outlook(setup) -> str:
    return f"""Verwachting 2–5 dagen:
Setup: {safe_get(setup, 'name')}
Timeframe: {safe_get(setup, 'timeframe')}"""


# === ✅ Dagrapportgenerator met diepe debug
def generate_daily_report_sections(symbol: str = "BTC") -> dict:
    log_and_print(f"🚀 Start rapportgeneratie voor: {symbol}")

    # 1️⃣ Data ophalen
    setup_raw = get_latest_setup_for_symbol(symbol)
    scores_raw = get_scores_from_db(symbol)
    log_and_print(f"📦 setup_raw: {repr(setup_raw)[:200]}")
    log_and_print(f"📦 scores_raw: {repr(scores_raw)[:200]}")

    # 2️⃣ Sanitize
    setup = sanitize_json_input(setup_raw, context="setup")
    scores = sanitize_json_input(scores_raw, context="scores")
    log_and_print(f"🧹 setup sanitized ({type(setup)}): {repr(setup)[:200]}")
    log_and_print(f"🧹 scores sanitized ({type(scores)}): {repr(scores)[:200]}")

    # 3️⃣ AI-strategie genereren
    strategy_raw = generate_strategy_from_setup(setup)
    log_and_print(f"📈 strategy_raw: {repr(strategy_raw)[:200]}")
    strategy = sanitize_json_input(strategy_raw, context="strategy")
    log_and_print(f"📈 strategy sanitized ({type(strategy)}): {repr(strategy)[:200]}")

    # 4️⃣ Validatie
    if not isinstance(setup, dict) or not setup:
        log_and_print("❌ Ongeldige setup → stop.")
        return {"error": "Ongeldige setup"}
    if not isinstance(scores, dict) or not scores:
        log_and_print("❌ Ongeldige scores → stop.")
        return {"error": "Ongeldige scores"}
    if not isinstance(strategy, dict) or not strategy:
        log_and_print("❌ Ongeldige strategy → stop.")
        return {"error": "Ongeldige strategy"}

    # 5️⃣ Rapport genereren
    try:
        report = {
            "btc_summary": generate_section(prompt_for_btc_summary(setup, scores)),
            "macro_summary": generate_section(prompt_for_macro_summary(scores)),
            "setup_checklist": generate_section(prompt_for_setup_checklist(setup)),
            "priorities": generate_section(prompt_for_priorities(setup, scores)),
            "wyckoff_analysis": generate_section(prompt_for_wyckoff_analysis(setup)),
            "recommendations": generate_section(prompt_for_recommendations(strategy)),
            "conclusion": generate_section(prompt_for_conclusion(scores)),
            "outlook": generate_section(prompt_for_outlook(setup)),
            "macro_score": safe_get(scores, "macro_score", 0),
            "technical_score": safe_get(scores, "technical_score", 0),
            "setup_score": safe_get(scores, "setup_score", 0),
            "sentiment_score": safe_get(scores, "sentiment_score", 0),
        }

        log_and_print(f"✅ Rapport gegenereerd type: {type(report)} met {len(report)} velden")
        for k, v in report.items():
            log_and_print(f"📌 {k}: {str(v)[:120]}")

        return report

    except Exception as e:
        log_and_print(f"❌ Exception tijdens rapportgeneratie: {e}")
        return {"error": str(e)}


# === ✅ CLI-test
if __name__ == "__main__":
    result = generate_daily_report_sections("BTC")
    print("\n🎯 RESULTAAT:")
    print(json.dumps(result, indent=2, ensure_ascii=False))
