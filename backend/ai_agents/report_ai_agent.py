import logging
from decimal import Decimal

from backend.utils.setup_utils import get_latest_setup_for_symbol
from backend.utils.json_utils import sanitize_json_input
from backend.utils.db import get_db_connection
from backend.utils.openai_client import ask_gpt_text
from backend.ai_core.system_prompt_builder import build_system_prompt

# =====================================================
# Logging
# =====================================================
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# =====================================================
# REPORT AGENT TASK (CENTRAAL, GEEN OVERLAP)
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

OUTPUT STRUCTUUR + LIMIETEN:

1. Executive Summary
   - Maximaal 4 zinnen
   - Eindigt met: BESLISSING + CONFIDENCE

2. Macro Context
   - 2–3 zinnen
   - Eindigt met: MACRO-IMPACT

3. Setup Validatie
   - Maximaal 4 zinnen
   - Eindigt met: SETUP-STATUS + RELEVANTIE

4. Strategie Implicatie
   - Maximaal 3 zinnen
   - Eindigt met: STRATEGIE-STATUS
   - Als geen strategie: schrijf letterlijk "ONVOLDOENDE DATA"

5. Vooruitblik
   - Exact 3 zinnen:
     - bullish
     - bearish
     - consolidatie

Als data ontbreekt:
- Schrijf letterlijk: ONVOLDOENDE DATA
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


def nv(v):
    return v if v not in [None, "", "None"] else "–"


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
# 2️⃣ AI CATEGORY INSIGHTS (MACRO / MARKET / TECH)
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
                "summary": summary or "–",
            }

        return insights

    finally:
        conn.close()


# =====================================================
# 3️⃣ MARKET SNAPSHOT
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
# 4️⃣ MARKET INDICATOR HIGHLIGHTS (JUISTE TABEL)
# =====================================================
def get_market_indicator_scores(user_id: int) -> list:
    conn = get_db_connection()
    if not conn:
        return []

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
                "interpretation": r[3] or "–",
            }
            for r in rows
        ]

    finally:
        conn.close()


# =====================================================
# 5️⃣ GPT HELPER
# =====================================================
def generate_section(prompt: str) -> str:
    system_prompt = build_system_prompt(task=REPORT_TASK, agent="report")
    text = ask_gpt_text(prompt, system_role=system_prompt)
    return text.strip() if text else "ONVOLDOENDE DATA"


# =====================================================
# 6️⃣ PROMPTS
# =====================================================
def prompt_executive_summary(scores, market):
    return f"""
Schrijf de executive summary.

Scores:
Macro: {nv(scores.get('macro_score'))}
Technical: {nv(scores.get('technical_score'))}
Market: {nv(scores.get('market_score'))}
Setup: {nv(scores.get('setup_score'))}

Prijs: ${nv(market.get('price'))}
24h verandering: {nv(market.get('change_24h'))}%

Sluit exact af met:
BESLISSING VANDAAG: ACTIE_VANDAAG / GEEN_ACTIE / OBSERVEREN
CONFIDENCE: LAAG / MIDDEL / HOOG
"""


def prompt_macro_context(ai):
    return f"""
Beschrijf de macro-context.

Trend: {ai.get('trend')}
Bias: {ai.get('bias')}
Risico: {ai.get('risk')}

Sluit exact af met:
MACRO-IMPACT: STEUNEND / NEUTRAAL / REMMEND
"""


def prompt_setup_validation(setup, scores):
    if not setup:
        return "ONVOLDOENDE DATA"

    return f"""
Beoordeel de setup.

Setup: {setup.get('name')} ({setup.get('timeframe')})

Scores:
Macro: {nv(scores.get('macro_score'))}
Technical: {nv(scores.get('technical_score'))}
Market: {nv(scores.get('market_score'))}

Sluit exact af met:
SETUP-STATUS: GO / NO-GO / CONDITIONAL
RELEVANTIE: VANDAAG / KOMENDE_DAGEN / LATER
"""


def prompt_strategy_implication(strategy):
    if not strategy:
        return "ONVOLDOENDE DATA"

    return f"""
Analyseer strategie-implicatie.

Entry: {strategy.get('entry')}
Targets: {strategy.get('targets')}
Stop-loss: {strategy.get('stop_loss')}

Sluit exact af met:
STRATEGIE-STATUS: UITVOERBAAR_VANDAAG / WACHT_OP_TRIGGER / NIET_ACTUEEL
"""


def prompt_outlook():
    return """
Geef exact drie zinnen:
1. Bullish scenario
2. Bearish scenario
3. Consolidatie scenario
"""


# =====================================================
# 7️⃣ MAIN REPORT BUILDER
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

    return {
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
        "market_data": market,
        "indicator_highlights": indicators,
        "scores": scores,
    }
