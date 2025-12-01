import os
import logging
import json
from decimal import Decimal

from backend.utils.setup_utils import get_latest_setup_for_symbol
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


# =====================================================
# Helpers
# =====================================================
def log_and_print(msg: str):
    logger.info(msg)
    try:
        with open(LOG_FILE, "a") as f:
            f.write(msg + "\n")
    except Exception:
        pass
    print(msg)


def to_float(v):
    if v is None:
        return None
    if isinstance(v, Decimal):
        return float(v)
    try:
        return float(v)
    except:
        return None


def safe_get(obj, key, fallback="–"):
    if isinstance(obj, dict):
        val = obj.get(key, fallback)
        if isinstance(val, Decimal):
            return to_float(val)
        return val
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
# 1️⃣ Scores ophalen (met report_date)
# =====================================================
def get_scores_from_db():
    try:
        scores = get_scores_for_symbol(include_metadata=True)
        if scores:
            return {k: to_float(v) for k, v in scores.items()}
    except Exception as e:
        log_and_print(f"⚠️ Live scoreberekening mislukt: {e}")

    conn = get_db_connection()
    if not conn:
        return {}

    out = {}
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT macro_score, technical_score, setup_score, market_score
                FROM daily_scores
                ORDER BY report_date DESC
                LIMIT 1;
            """)
            row = cur.fetchone()

            if row:
                out = {
                    "macro_score": to_float(row[0]),
                    "technical_score": to_float(row[1]),
                    "setup_score": to_float(row[2]),
                    "market_score": to_float(row[3]),
                }
    finally:
        conn.close()

    return out


# =====================================================
# 2️⃣ AI insights ophalen
# =====================================================
def get_ai_insights_from_db():
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
                    "avg_score": to_float(avg_score),
                    "trend": trend,
                    "bias": bias,
                    "risk": risk,
                    "summary": summary,
                }
    finally:
        conn.close()

    return insights


# =====================================================
# 3️⃣ Strategy uit database halen (GEEN GPT!)
# =====================================================
def get_latest_strategy_for_setup(setup_id: int):
    conn = get_db_connection()
    if not conn:
        return None

    try:
        with conn.cursor() as cur:
            # Belangrijk: jouw DB heeft kolom "target" (zonder s)
            cur.execute("""
                SELECT entry, target, stop_loss, rr, explanation
                FROM strategies
                WHERE setup_id = %s
                ORDER BY date DESC
                LIMIT 1;
            """, (setup_id,))
            row = cur.fetchone()

        if not row:
            logger.warning(f"⚠️ Geen strategy gevonden voor setup {setup_id}")
            return None

        entry, target, stop_loss, rr, explanation = row

        # Targets normaliseren → altijd list
        targets = []
        if isinstance(target, str):
            targets = [t.strip() for t in target.split(",") if t.strip()]
        elif isinstance(target, list):
            targets = target

        return {
            "entry": entry,
            "targets": targets,
            "stop_loss": stop_loss,
            "risk_reward": rr,
            "explanation": explanation,
        }

    except Exception as e:
        logger.error(f"❌ Fout bij ophalen strategy: {e}", exc_info=True)
        return None

    finally:
        conn.close()


# =====================================================
# 4️⃣ Markt data
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
                LIMIT 1;
            """)
            row = cur.fetchone()

            if row:
                return {
                    "price": to_float(row[0]),
                    "volume": to_float(row[1]),
                    "change_24h": to_float(row[2]),
                }
    finally:
        conn.close()

    return {}


# =====================================================
# 5️⃣ GPT helper
# =====================================================
def generate_section(prompt: str, retries: int = 3) -> str:
    text = ask_gpt_text(prompt, system_role=REPORT_STYLE_GUIDE, retries=retries)
    return text.strip() if text else "AI-generatie mislukt."


# =====================================================
# 6️⃣ PROMPTS
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
Schrijf 6–8 bullets:
- Sterktes
- Zwaktes
- Activatie
- Invalidatie
- Praktische tips

Setup: {setup.get('name')} ({setup.get('timeframe')})
"""


def prompt_for_priorities(setup, scores):
    return "Genereer 3–7 dagelijkse prioriteiten voor traders."


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
    return "Schrijf een 2–5 dagen outlook met bullish, bearish en sideways scenario."


# =====================================================
# 🚀 7️⃣ Main Report Builder (GEEN nieuwe strategy)
# =====================================================
def generate_daily_report_sections(symbol: str = "BTC") -> dict:
    log_and_print(f"🚀 Rapportgeneratie voor {symbol}")

    # Setup
    setup_raw = get_latest_setup_for_symbol(symbol)
    setup = sanitize_json_input(setup_raw, context="setup")

    # Scores
    scores = sanitize_json_input(get_scores_from_db(), context="scores")

    # AI insights
    ai_insights = get_ai_insights_from_db()

    # Market data
    market_data = get_latest_market_data()

    # Strategy (UIT DATABASE!)
    strategy = get_latest_strategy_for_setup(setup["id"])
    if not strategy:
        strategy = {
            "entry": "n.v.t.",
            "targets": [],
            "stop_loss": "n.v.t.",
            "risk_reward": "?",
            "explanation": "Geen strategy beschikbaar voor vandaag."
        }

    # Build report
    report = {
        "btc_summary": generate_section(prompt_for_btc_summary(setup, scores, market_data, ai_insights)),
        "macro_summary": generate_section(prompt_for_macro_summary(scores, ai_insights)),
        "setup_checklist": generate_section(prompt_for_setup_checklist(setup)),
        "priorities": generate_section(prompt_for_priorities(setup, scores)),
        "wyckoff_analysis": generate_section(prompt_for_wyckoff_analysis(setup)),
        "recommendations": generate_section(prompt_for_recommendations(strategy)),
        "conclusion": generate_section(prompt_for_conclusion(scores, ai_insights)),
        "outlook": generate_section(prompt_for_outlook(setup)),

        # Raw data
        "macro_score": scores.get("macro_score"),
        "technical_score": scores.get("technical_score"),
        "setup_score": scores.get("setup_score"),
        "market_score": scores.get("market_score"),

        "ai_insights": ai_insights,
        "ai_master_score": ai_insights.get("master"),
        "market_data": market_data,
        "strategy": strategy,
    }

    log_and_print("✅ Rapport succesvol gegenereerd.")
    return report


if __name__ == "__main__":
    result = generate_daily_report_sections("BTC")
    print(json.dumps(result, indent=2, ensure_ascii=False))
