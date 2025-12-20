import logging
import json
from decimal import Decimal
from datetime import date

from backend.utils.setup_utils import get_latest_setup_for_symbol
from backend.utils.json_utils import sanitize_json_input
from backend.utils.db import get_db_connection
from backend.utils.openai_client import ask_gpt_text

# =====================================================
# Logging
# =====================================================
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# =====================================================
# Helpers
# =====================================================
def to_float(v):
    if v is None:
        return None
    if isinstance(v, Decimal):
        return float(v)
    try:
        return float(v)
    except Exception:
        return None


def nv(v):
    return v if v not in [None, "", "None"] else "–"


# =====================================================
# STYLE
# =====================================================
REPORT_STYLE_GUIDE = """
Je bent een professionele Bitcoin- en macro-analist.
Schrijf in het Nederlands in de stijl van een premium nieuwsbrief
(een mix van Glassnode, BitcoinStrategy en TIA).
"""


# =====================================================
# 1️⃣ DAILY SCORES (SINGLE SOURCE OF TRUTH)
# =====================================================
def get_daily_scores(user_id: int) -> dict:
    conn = get_db_connection()
    if not conn:
        return {}

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    macro_score,
                    technical_score,
                    market_score,
                    setup_score
                FROM daily_scores
                WHERE user_id = %s
                  AND report_date = CURRENT_DATE
                LIMIT 1;
            """, (user_id,))
            row = cur.fetchone()

        if not row:
            logger.warning(f"⚠️ Geen daily_scores gevonden (user_id={user_id})")
            return {}

        return {
            "macro_score": to_float(row[0]),
            "technical_score": to_float(row[1]),
            "market_score": to_float(row[2]),
            "setup_score": to_float(row[3]),
        }

    finally:
        conn.close()


# =====================================================
# 2️⃣ AI CATEGORY INSIGHTS (VALIDATED)
# =====================================================
def get_ai_insights(user_id: int) -> dict:
    conn = get_db_connection()
    if not conn:
        return {}

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT category, avg_score, trend, bias, risk, summary
                FROM ai_category_insights
                WHERE user_id = %s
                  AND date = CURRENT_DATE;
            """, (user_id,))
            rows = cur.fetchall()

        insights = {}
        for cat, avg, trend, bias, risk, summary in rows:
            insights[cat] = {
                "avg_score": to_float(avg),
                "trend": trend or "–",
                "bias": bias or "–",
                "risk": risk or "–",
                "summary": summary or "Geen data.",
            }

        required = ["macro", "market", "technical", "setup"]
        missing = [r for r in required if r not in insights]
        if missing:
            logger.warning(
                f"⚠️ Ontbrekende AI insights {missing} (user_id={user_id})"
            )

        return insights

    finally:
        conn.close()


# =====================================================
# 3️⃣ STRATEGY (LATEST PER SETUP)
# =====================================================
def get_latest_strategy(setup_id: int, user_id: int) -> dict | None:
    if not setup_id:
        return None

    conn = get_db_connection()
    if not conn:
        return None

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT entry, target, stop_loss, explanation, risk_profile, data
                FROM strategies
                WHERE setup_id = %s AND user_id = %s
                ORDER BY created_at DESC
                LIMIT 1;
            """, (setup_id, user_id))
            row = cur.fetchone()

        if not row:
            return None

        entry, target, stop, expl, risk, data = row

        if isinstance(data, dict):
            entry = data.get("entry", entry)
            target = data.get("targets", target)
            stop = data.get("stop_loss", stop)
            expl = data.get("explanation", expl)
            risk = data.get("risk_reward", risk)

        targets = []
        if isinstance(target, str):
            targets = [t.strip() for t in target.split(",") if t.strip()]
        elif isinstance(target, list):
            targets = target

        return {
            "entry": entry or "n.v.t.",
            "targets": targets,
            "stop_loss": stop or "n.v.t.",
            "risk_reward": risk or "?",
            "explanation": expl or "Geen uitleg beschikbaar.",
        }

    finally:
        conn.close()


# =====================================================
# 4️⃣ MARKET DATA (GLOBAL SNAPSHOT)
# =====================================================
def get_latest_market_data() -> dict:
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

        if not row:
            return {}

        return {
            "price": to_float(row[0]),
            "volume": to_float(row[1]),
            "change_24h": to_float(row[2]),
        }

    finally:
        conn.close()


# =====================================================
# 5️⃣ DAILY MARKET INDICATOR SCORES
# =====================================================
def get_market_indicator_scores(user_id: int) -> list:
    conn = get_db_connection()
    if not conn:
        return []

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT indicator, value, score, interpretation
                FROM market_indicator_scores
                WHERE user_id = %s
                  AND timestamp::date = CURRENT_DATE
                ORDER BY timestamp DESC;
            """, (user_id,))
            rows = cur.fetchall()

        return [
            {
                "indicator": r[0],
                "value": to_float(r[1]),
                "score": to_float(r[2]),
                "interpretation": r[3] or "–",
            }
            for r in rows
        ]

    finally:
        conn.close()


# =====================================================
# 6️⃣ GPT HELPER
# =====================================================
def generate_section(prompt: str) -> str:
    text = ask_gpt_text(prompt, system_role=REPORT_STYLE_GUIDE)
    return text.strip() if text else "AI-generatie mislukt."


# =====================================================
# 7️⃣ PROMPTS
# =====================================================
def prompt_btc_summary(setup, scores, market, indicators):
    ind_lines = "\n".join(
        f"- {i['indicator']}: {nv(i['value'])}, score {nv(i['score'])} → {i['interpretation']}"
        for i in indicators[:3]
    ) or "–"

    return f"""
Schrijf een krachtige opening (6–8 zinnen).

Scores:
- Macro: {nv(scores.get('macro_score'))}
- Technisch: {nv(scores.get('technical_score'))}
- Markt: {nv(scores.get('market_score'))}
- Setup: {nv(scores.get('setup_score'))}

Markt:
- Prijs: ${nv(market.get('price'))}
- Volume: {nv(market.get('volume'))}
- 24h: {nv(market.get('change_24h'))}%

Dagelijkse market-indicatoren:
{ind_lines}

Actieve setup:
- {nv(setup.get('name'))} ({nv(setup.get('timeframe'))})
"""


def prompt_macro(ai):
    return f"""
Maak een macro-update (5–8 zinnen).

Trend: {ai.get('trend')}
Bias: {ai.get('bias')}
Risico: {ai.get('risk')}
Samenvatting: {ai.get('summary')}
"""


def prompt_setup_checklist(setup):
    return f"Schrijf 6–8 bullets over de setup {setup.get('name')} ({setup.get('timeframe')})."


def prompt_strategy(strategy):
    return f"""
Schrijf een strategie-uitleg (6–10 zinnen).

Entry: {strategy['entry']}
Targets: {strategy['targets']}
Stop-loss: {strategy['stop_loss']}
"""


def prompt_outlook():
    return "Schrijf een 2–5 dagen outlook met bullish, bearish en sideways scenario."


# =====================================================
# 8️⃣ MAIN REPORT BUILDER
# =====================================================
def generate_daily_report_sections(symbol: str = "BTC", user_id: int = None) -> dict:
    logger.info(f"📄 Rapport genereren | {symbol} | user_id={user_id}")

    setup = sanitize_json_input(
        get_latest_setup_for_symbol(symbol=symbol, user_id=user_id) or {},
        context="setup",
    )

    scores = get_daily_scores(user_id)
    ai = get_ai_insights(user_id)
    market = get_latest_market_data()
    indicators = get_market_indicator_scores(user_id)

    if scores.get("setup_score") is None and ai.get("setup"):
        scores["setup_score"] = ai["setup"].get("avg_score")

    strategy = get_latest_strategy(setup.get("id"), user_id) or {
        "entry": "n.v.t.",
        "targets": [],
        "stop_loss": "n.v.t.",
        "risk_reward": "?",
        "explanation": "Geen strategy beschikbaar.",
    }

    return {
        "btc_summary": generate_section(
            prompt_btc_summary(setup, scores, market, indicators)
        ),
        "macro_summary": generate_section(prompt_macro(ai.get("macro", {}))),
        "setup_checklist": generate_section(prompt_setup_checklist(setup)),
        "recommendations": generate_section(prompt_strategy(strategy)),
        "outlook": generate_section(prompt_outlook()),
        "scores": scores,
        "strategy": strategy,
        "market_data": market,
        "market_indicator_scores": indicators,
    }
