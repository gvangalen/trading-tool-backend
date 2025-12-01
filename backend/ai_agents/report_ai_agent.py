import os
import logging
import json

from backend.utils.setup_utils import get_latest_setup_for_symbol
from backend.ai_agents.strategy_ai_agent import generate_strategy_from_setup
from backend.utils.json_utils import sanitize_json_input
from backend.utils.db import get_db_connection
from backend.utils.scoring_utils import get_scores_for_symbol
from backend.utils.openai_client import ask_gpt_text

# =====================================================
# 🪵 Logging
# =====================================================
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


def safe_get(obj, key, fallback="–"):
    if isinstance(obj, dict):
        return obj.get(key, fallback)
    return fallback


# =====================================================
# 🎨 PREMIUM REPORT STYLE
# =====================================================
REPORT_STYLE_GUIDE = """
Je bent een professionele Bitcoin- en macro-analist.
Schrijf in het Nederlands in de stijl van een premium nieuwsbrief
(een mix van Glassnode, BitcoinStrategy en TIA).
"""


# =====================================================
# 📊 Scores uit DB
# =====================================================
def get_scores_from_db():
    """
    Fix: daily_scores gebruikt nu `report_date` in plaats van `date`
    """
    try:
        scores = get_scores_for_symbol(include_metadata=True)
        if scores:
            return scores
    except Exception as e:
        log_and_print(f"⚠️ Live scoreberekening mislukt: {e}")

    conn = get_db_connection()
    if not conn:
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
                return {
                    "macro_score": row[0],
                    "technical_score": row[1],
                    "setup_score": row[2],
                    "market_score": row[3],
                }
    finally:
        conn.close()

    return {}


# =====================================================
# 🧠 AI insights laden
# =====================================================
def get_ai_insights_from_db():
    """
    ai_category_insights heeft WEL een 'date' kolom
    """
    conn = get_db_connection()
    if not conn:
        return {}

    insights = {}
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT category, avg_score, trend, bias, risk, summary
                FROM ai_category_insights
                WHERE date = CURRENT_DATE;
            """)
            rows = cur.fetchall()

            for category, avg_score, trend, bias, risk, summary in rows:
                insights[category] = {
                    "avg_score": float(avg_score or 0),
                    "trend": trend,
                    "bias": bias,
                    "risk": risk,
                    "summary": summary,
                }

    finally:
        conn.close()

    return insights


# =====================================================
# 📈 Laatste marktdata
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

    finally:
        conn.close()


# =====================================================
# 🧠 Premium text generator
# =====================================================
def generate_section(prompt: str, retries: int = 3) -> str:
    text = ask_gpt_text(prompt, system_role=REPORT_STYLE_GUIDE, retries=retries)
    return text.strip() if text else "AI-generatie mislukt of gaf geen output."


# =====================================================
# PROMPTS
# =====================================================
def prompt_for_btc_summary(setup, scores, market_data=None, ai_insights=None):
    price = safe_get(market_data, "price")
    volume = safe_get(market_data, "volume")
    change = safe_get(market_data, "change_24h")

    macro = safe_get(scores, "macro_score")
    tech = safe_get(scores, "technical_score")
    setup_score = safe_get(scores, "setup_score")
    market_score = safe_get(scores, "market_score")

    master = ai_insights.get("master", {})

    return f"""
Schrijf een krachtige openingssectie (6–8 zinnen).

Data:
- Scores: macro {macro}, technisch {tech}, setup {setup_score}, markt {market_score}
- Master score: {safe_get(master, "avg_score")}, trend {safe_get(master, "trend")}, bias {safe_get(master, "bias")}, risico {safe_get(master, "risk")}
- Markt: prijs ${price}, volume {volume}, verandering {change}%
- Setup: {setup.get('name')} ({setup.get('timeframe')})
"""


def prompt_for_macro_summary(scores, ai_insights):
    macro = ai_insights.get("macro", {})
    return f"""
Maak een compacte macro-update (5–8 zinnen).

Macro score vandaag: {scores['macro_score']}
Trend: {macro.get('trend')}
Bias: {macro.get('bias')}
Risico: {macro.get('risk')}
Samenvatting: {macro.get('summary')}
"""


def prompt_for_setup_checklist(setup):
    return f"""
Schrijf 6–8 bullets met:
- Sterktes
- Zwaktes
- Activatiecondities
- Invalidatie
- Praktische tips

Setup:
Naam: {setup.get('name')}
Timeframe: {setup.get('timeframe')}
"""


def prompt_for_priorities(setup, scores):
    return f"Genereer 3–7 dagelijkse prioriteiten voor traders."


def prompt_for_wyckoff_analysis(setup):
    return f"""
Maak een Wyckoff-analyse (5–10 zinnen).
Setup: {setup.get('name')}
Phase: {setup.get('wyckoff_phase')}
"""


def prompt_for_recommendations(strategy):
    return f"""
Schrijf een premium strategie-uitleg (6–10 zinnen).

Entry: {strategy['entry']}
Targets: {strategy['targets']}
Stop-loss: {strategy['stop_loss']}
"""


def prompt_for_conclusion(scores, ai_insights):
    master = ai_insights.get("master", {})
    return f"""
Schrijf een slotconclusie (4–8 zinnen).
Master Score: {safe_get(master, "avg_score")} | trend {safe_get(master, "trend")}
"""


def prompt_for_outlook(setup):
    return f"Schrijf een 2–5 dagen outlook met bullish, bearish en sideways scenario."


# =====================================================
# 🚀 Main Report Builder
# =====================================================
def generate_daily_report_sections(symbol: str = "BTC") -> dict:
    log_and_print(f"🚀 Start rapportgeneratie voor: {symbol}")

    setup_raw = get_latest_setup_for_symbol(symbol)
    setup = sanitize_json_input(setup_raw, context="setup")

    scores_raw = get_scores_from_db()
    scores = sanitize_json_input(scores_raw, context="scores")

    ai_insights = get_ai_insights_from_db()
    market_data = get_latest_market_data()

    strategy_raw = generate_strategy_from_setup(setup)
    strategy = sanitize_json_input(strategy_raw, context="strategy")

    master = ai_insights.get("master")

    report = {
        "btc_summary": generate_section(prompt_for_btc_summary(setup, scores, market_data, ai_insights)),
        "macro_summary": generate_section(prompt_for_macro_summary(scores, ai_insights)),
        "setup_checklist": generate_section(prompt_for_setup_checklist(setup)),
        "priorities": generate_section(prompt_for_priorities(setup, scores)),
        "wyckoff_analysis": generate_section(prompt_for_wyckoff_analysis(setup)),
        "recommendations": generate_section(prompt_for_recommendations(strategy)),
        "conclusion": generate_section(prompt_for_conclusion(scores, ai_insights)),
        "outlook": generate_section(prompt_for_outlook(setup)),

        # Raw data for API/frontend
        "macro_score": scores["macro_score"],
        "technical_score": scores["technical_score"],
        "setup_score": scores["setup_score"],
        "market_score": scores["market_score"],

        "ai_insights": ai_insights,
        "ai_master_score": master,
        "market_data": market_data,
    }

    log_and_print("✅ Rapport succesvol gegenereerd")
    return report


if __name__ == "__main__":
    result = generate_daily_report_sections("BTC")
    print(json.dumps(result, indent=2, ensure_ascii=False))
