import logging
from decimal import Decimal
from typing import Dict, Any

from backend.utils.db import get_db_connection
from backend.utils.setup_utils import get_latest_setup_for_symbol
from backend.utils.json_utils import sanitize_json_input
from backend.utils.openai_client import ask_gpt_text
from backend.ai_core.system_prompt_builder import build_system_prompt

# =====================================================
# Logging
# =====================================================
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# =====================================================
# REPORT AGENT INSTRUCTIE (HARD CONTRACT)
# =====================================================
REPORT_TASK = """
Rol:
Samenstellen van het DAGELIJKS TRADING RAPPORT.

INPUT:
- daily_scores
- ai_category_insights
- setup (indien aanwezig)
- market snapshot
- market indicator highlights

REGELS:
- Gebruik uitsluitend aangeleverde data
- Geen eigen analyse
- Geen aannames
- Geen uitleg van indicatoren

OUTPUT:
- Altijd geldige JSON
- Geen markdown
- Geen opsmuk

SECTIES:

1. Executive Summary
   - Max 4 zinnen
   - Eindigt met:
     BESLISSING
     CONFIDENCE

2. Macro Context
   - 2–3 zinnen
   - Eindigt met:
     MACRO-IMPACT

3. Setup Validatie
   - Max 4 zinnen
   - Eindigt met:
     SETUP-STATUS
     RELEVANTIE

4. Strategie Implicatie
   - Max 3 zinnen
   - Eindigt met:
     STRATEGIE-STATUS
   - Als data ontbreekt: "ONVOLDOENDE DATA"

5. Vooruitblik
   - Exact 3 zinnen:
     bullish
     bearish
     consolidatie
"""

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


# =====================================================
# 1️⃣ DAILY SCORES (SINGLE SOURCE OF TRUTH)
# =====================================================
def get_daily_scores(user_id: int) -> Dict[str, Any]:
    conn = get_db_connection()
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
# 2️⃣ AI CATEGORY INSIGHTS
# =====================================================
def get_ai_insights(user_id: int) -> Dict[str, Any]:
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT category, avg_score, trend, bias, risk, summary
                FROM ai_category_insights
                WHERE user_id = %s
                  AND date = CURRENT_DATE;
            """, (user_id,))
            rows = cur.fetchall()

        result = {}
        for cat, avg, trend, bias, risk, summary in rows:
            result[cat] = {
                "avg_score": to_float(avg),
                "trend": trend,
                "bias": bias,
                "risk": risk,
                "summary": summary,
            }

        return result
    finally:
        conn.close()


# =====================================================
# 3️⃣ MARKET SNAPSHOT
# =====================================================
def get_latest_market_data() -> Dict[str, Any]:
    conn = get_db_connection()
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
# 4️⃣ MARKET INDICATOR HIGHLIGHTS
# =====================================================
def get_market_indicator_scores(user_id: int) -> list:
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT name, value, score, interpretation
                FROM market_data_indicators
                WHERE user_id = %s
                  AND score IS NOT NULL
                  AND DATE(timestamp) = CURRENT_DATE
                ORDER BY score DESC;
            """, (user_id,))
            rows = cur.fetchall()

        return [
            {
                "indicator": r[0],
                "value": to_float(r[1]),
                "score": to_float(r[2]),
                "interpretation": r[3],
            }
            for r in rows
        ]
    finally:
        conn.close()


# =====================================================
# GPT SECTION GENERATOR
# =====================================================
def generate_section(prompt: str) -> Dict[str, Any]:
    system_prompt = build_system_prompt(agent="report", task=REPORT_TASK)
    text = ask_gpt_text(prompt, system_role=system_prompt)

    if not text:
        return {"text": "ONVOLDOENDE DATA"}

    return {"text": text.strip()}


# =====================================================
# PROMPTS
# =====================================================
def prompt_executive_summary(scores, market):
    return f"""
Scores:
Macro: {scores.get('macro_score')}
Technical: {scores.get('technical_score')}
Market: {scores.get('market_score')}
Setup: {scores.get('setup_score')}

Prijs: {market.get('price')}
24h verandering: {market.get('change_24h')}

Schrijf executive summary.
"""


def prompt_macro_context(macro_ai):
    return f"""
Trend: {macro_ai.get('trend')}
Bias: {macro_ai.get('bias')}
Risico: {macro_ai.get('risk')}

Beschrijf macro-context.
"""


def prompt_setup_validation(setup, scores):
    if not setup:
        return "ONVOLDOENDE DATA"

    return f"""
Setup: {setup.get('name')}
Timeframe: {setup.get('timeframe')}

Scores:
Macro: {scores.get('macro_score')}
Technical: {scores.get('technical_score')}
Market: {scores.get('market_score')}
"""


def prompt_strategy_implication(strategy):
    if not strategy:
        return "ONVOLDOENDE DATA"

    return f"""
Entry: {strategy.get('entry')}
Targets: {strategy.get('targets')}
Stop-loss: {strategy.get('stop_loss')}
"""


def prompt_outlook():
    return """
Geef exact 3 zinnen:
1 bullish
2 bearish
3 consolidatie
"""


# =====================================================
# 🚀 MAIN BUILDER — SINGLE SOURCE OF TRUTH
# =====================================================
def generate_daily_report_sections(user_id: int) -> Dict[str, Any]:
    logger.info(f"📄 Daily report genereren | user_id={user_id}")

    setup = sanitize_json_input(
        get_latest_setup_for_symbol(symbol="BTC", user_id=user_id) or {},
        context="setup",
    )

    scores = get_daily_scores(user_id)
    ai = get_ai_insights(user_id)
    market = get_latest_market_data()
    indicators = get_market_indicator_scores(user_id)

    return {
        # JSONB SECTIES
        "executive_summary": generate_section(
            prompt_executive_summary(scores, market)
        ),
        "macro_context": generate_section(
            prompt_macro_context(ai.get("macro", {}))
        ),
        "setup_validation": generate_section(
            prompt_setup_validation(setup, scores)
        ),
        "strategy_implication": generate_section(
            prompt_strategy_implication({})
        ),
        "outlook": generate_section(
            prompt_outlook()
        ),

        # MARKTDATA
        "price": market.get("price"),
        "change_24h": market.get("change_24h"),
        "volume": market.get("volume"),

        # HIGHLIGHTS & SCORES
        "indicator_highlights": indicators,
        "macro_score": scores.get("macro_score"),
        "technical_score": scores.get("technical_score"),
        "market_score": scores.get("market_score"),
        "setup_score": scores.get("setup_score"),
    }
