import os
import logging
import json
from dotenv import load_dotenv
from openai import OpenAI

from backend.utils.setup_utils import get_latest_setup_for_symbol
from backend.utils.ai_strategy_utils import generate_strategy_from_setup
from backend.utils.json_utils import sanitize_json_input
from backend.utils.db import get_db_connection
from backend.utils.scoring_utils import get_scores_for_symbol  # ✅ Nieuwe versie gebruikt DB direct

# === ✅ Logging naar bestand + console
LOG_FILE = "/tmp/daily_report_debug.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

def log_and_print(msg: str):
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
DEFAULT_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")


def safe_get(obj, key, fallback="–"):
    if isinstance(obj, dict):
        return obj.get(key, fallback)
    return fallback


# =====================================================
# 📊 Scores ophalen (direct uit database)
# =====================================================
def get_scores_from_db():
    """
    ⚙️ Nieuwe versie: haalt eerst de live berekende scores op
    uit scoring_utils.get_scores_for_symbol() (DB-driven),
    en gebruikt daily_scores alleen als fallback.
    """
    try:
        scores = get_scores_for_symbol(include_metadata=True)
        if scores:
            log_and_print(f"📊 Live scores geladen uit DB: {scores}")
            return scores
    except Exception as e:
        log_and_print(f"⚠️ Live scoreberekening mislukt: {e}")

    # Fallback: oude daily_scores-tabel
    conn = get_db_connection()
    if not conn:
        log_and_print("❌ Geen DB-verbinding voor fallback-scores.")
        return {}

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT macro_score, technical_score, setup_score, market_score
                FROM daily_scores
                ORDER BY report_date DESC
                LIMIT 1
            """)
            row = cur.fetchone()
            if row:
                scores = {
                    "macro_score": row[0],
                    "technical_score": row[1],
                    "setup_score": row[2],
                    "market_score": row[3],
                }
                log_and_print(f"📊 Fallback scores geladen: {scores}")
                return scores
    except Exception as e:
        log_and_print(f"❌ Fout bij ophalen fallback-scores: {e}")
    finally:
        conn.close()
    return {}


# =====================================================
# 📈 Laatste prijs, volume, change ophalen uit market_data
# =====================================================
def get_latest_market_data():
    conn = get_db_connection()
    if not conn:
        return {}
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT price, volume, change_24h
                FROM market_data
                ORDER BY timestamp DESC
                LIMIT 1
            """)
            row = cur.fetchone()
            if row:
                return {
                    "price": round(row[0], 2),
                    "volume": row[1],
                    "change_24h": row[2],
                }
    except Exception as e:
        log_and_print(f"❌ Fout bij ophalen market_data: {e}")
        return {}
    finally:
        conn.close()


# =====================================================
# 🧠 AI-sectie genereren
# =====================================================
def generate_section(prompt: str, retries: int = 3, model: str = DEFAULT_MODEL) -> str:
    for attempt in range(1, retries + 1):
        try:
            log_and_print(f"🔍 [AI Attempt {attempt}] Prompt (eerste 180): {prompt[:180]}")
            response = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": "Je bent een professionele crypto-analist. Schrijf in het Nederlands."},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.7,
            )
            content = response.choices[0].message.content.strip()
            if content:
                log_and_print(f"✅ [AI Response] Lengte: {len(content)} Tekst: {content[:150]}...")
                return content
        except Exception as e:
            log_and_print(f"⚠️ Fout bij OpenAI poging {attempt}: {e}")
    log_and_print("❌ Alle AI-pogingen mislukt.")
    return "Fout: AI-generatie mislukt."


# =====================================================
# 🧩 Prompts per sectie (ongewijzigd)
# =====================================================
def prompt_for_btc_summary(setup, scores, market_data=None) -> str:
    prijsinfo = ""
    if market_data:
        prijsinfo = (
            f"\nPrijs: ${market_data.get('price', '?')}, "
            f"Volume: ${market_data.get('volume', '?')}, "
            f"24u verandering: {market_data.get('change_24h', '?')}%"
        )
    return f"""Geef een korte samenvatting van de huidige situatie voor Bitcoin:
Setup: {safe_get(setup, 'name')}
Timeframe: {safe_get(setup, 'timeframe')}
Technische score: {safe_get(scores, 'technical_score', 0)}
Setup score: {safe_get(scores, 'setup_score', 0)}
Macro score: {safe_get(scores, 'macro_score', 0)}
Market score: {safe_get(scores, 'market_score', 0)}{prijsinfo}"""

def prompt_for_macro_summary(scores) -> str:
    return f"""Vat de macro-economische situatie samen.
Macro-score: {safe_get(scores, 'macro_score', 0)}"""

def prompt_for_setup_checklist(setup) -> str:
    return f"""Controleer A+ criteria voor de setup:
Setup: {safe_get(setup, 'name')}
Timeframe: {safe_get(setup, 'timeframe')}
Indicatoren: {safe_get(setup, 'indicators', [])}"""

def prompt_for_priorities(setup, scores) -> str:
    return f"""Belangrijkste aandachtspunten vandaag:
Setup: {safe_get(setup, 'name')}
Scores: {scores}"""

def prompt_for_wyckoff_analysis(setup) -> str:
    return f"""Wyckoff-analyse van de marktstructuur:
Fase: {safe_get(setup, 'wyckoff_phase')}
Beschrijving: {safe_get(setup, 'explanation')}"""

def prompt_for_recommendations(strategy) -> str:
    return f"""Tradingadvies op basis van strategie:
Entry: {safe_get(strategy, 'entry')}
Targets: {safe_get(strategy, 'targets')}
Stop-loss: {safe_get(strategy, 'stop_loss')}
Uitleg: {safe_get(strategy, 'explanation')}"""

def prompt_for_conclusion(scores) -> str:
    return f"""Slotconclusie van de dag:
Macro: {safe_get(scores, 'macro_score', 0)}
Technisch: {safe_get(scores, 'technical_score', 0)}"""

def prompt_for_outlook(setup) -> str:
    return f"""Verwachting voor de komende 2–5 dagen:
Setup: {safe_get(setup, 'name')}
Timeframe: {safe_get(setup, 'timeframe')}"""


# =====================================================
# 🚀 Hoofdfunctie: Dagrapport genereren
# =====================================================
def generate_daily_report_sections(symbol: str = "BTC") -> dict:
    log_and_print(f"🚀 Start rapportgeneratie voor: {symbol}")

    setup_raw = get_latest_setup_for_symbol(symbol)
    scores_raw = get_scores_from_db()
    market_data = get_latest_market_data()

    log_and_print(f"📦 setup_raw: {repr(setup_raw)[:180]}")
    log_and_print(f"📦 scores_raw: {repr(scores_raw)[:180]}")

    setup = sanitize_json_input(setup_raw, context="setup")
    scores = sanitize_json_input(scores_raw, context="scores")
    strategy_raw = generate_strategy_from_setup(setup)
    strategy = sanitize_json_input(strategy_raw, context="strategy")

    if not isinstance(setup, dict) or not setup:
        return {"error": "Ongeldige setup"}
    if not isinstance(scores, dict) or not scores:
        return {"error": "Ongeldige scores"}
    if not isinstance(strategy, dict) or not strategy:
        return {"error": "Ongeldige strategy"}

    try:
        report = {
            "btc_summary": generate_section(prompt_for_btc_summary(setup, scores, market_data)),
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
            "market_score": safe_get(scores, "market_score", 0),
            "market_data": market_data,
        }
        log_and_print(f"✅ Rapport succesvol gegenereerd ({len(report)} velden)")
        return report

    except Exception as e:
        log_and_print(f"❌ Exception tijdens rapportgeneratie: {e}")
        return {"error": str(e)}


if __name__ == "__main__":
    result = generate_daily_report_sections("BTC")
    print("\n🎯 RESULTAAT:")
    print(json.dumps(result, indent=2, ensure_ascii=False))
