import logging
import json
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
# 🎯 REPORT AGENT — NIEUWE ROL & STIJL
# =====================================================
REPORT_TASK = """
Rol:
Je bent een ervaren Bitcoin market analyst.
Je schrijft een dagelijks briefingdocument voor een ervaren gebruiker.

Context:
- De lezer kent Bitcoin.
- De lezer heeft toegang tot een dashboard met scores en data.
- Jij schrijft de samenvatting NA het bekijken van dat dashboard.

Belangrijk:
- Schrijf in normaal, vloeiend Nederlands.
- Geen labels, geen CAPS, geen AI-achtige termen.
- Geen herhaling van exacte cijfers tenzij functioneel.
- Gebruik data als context, niet als opsomming.
- Klink als een analist, niet als een checklist.

Regels:
- Gebruik uitsluitend aangeleverde data.
- Geen aannames.
- Geen educatieve uitleg.
- Geen opsommingen tenzij expliciet gevraagd.
- Geen markdown.

Output:
- Altijd geldige JSON.
- Elke sectie is één string (geen nested objecten).

Structuur:

1. Executive Summary
   - 3–5 zinnen
   - Samenvatting van de marktsituatie
   - Sluit af met een duidelijke conclusie in normale taal

2. Market Context
   - 1–2 korte alinea’s
   - Beschrijf hoe macro, markt en techniek zich tot elkaar verhouden

3. Setups & Positionering
   - Alleen bespreken als setup-data aanwezig is
   - Anders: benoem expliciet dat er geen valide setup is

4. Strategie & Implicaties
   - Wat betekent dit praktisch voor positionering?
   - Geen trade-instructies, wel richting

5. Vooruitblik
   - Exact 3 zinnen:
     • bullish scenario
     • bearish scenario
     • consolidatie scenario
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
# 1️⃣ DAILY SCORES
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
def generate_section(prompt: str) -> str:
    system_prompt = build_system_prompt(
        agent="report",
        task=REPORT_TASK
    )

    raw = ask_gpt_text(prompt, system_role=system_prompt)
    if not raw:
        return "Onvoldoende data om hier een zinvolle analyse van te maken."

    text = raw.strip()

    try:
        parsed = json.loads(text)
        if isinstance(parsed, dict):
            return "\n\n".join(
                v.strip() for v in parsed.values() if isinstance(v, str)
            )
    except Exception:
        pass

    return text


# =====================================================
# PROMPTS
# =====================================================
def prompt_executive_summary(scores, market):
    return f"""
Macro score: {scores.get('macro_score')}
Technisch score: {scores.get('technical_score')}
Markt score: {scores.get('market_score')}
Setup score: {scores.get('setup_score')}

Prijs: {market.get('price')}
24h verandering: {market.get('change_24h')}

Schrijf een executive summary.
"""


def prompt_market_context(macro_ai):
    return f"""
Trend: {macro_ai.get('trend')}
Bias: {macro_ai.get('bias')}
Risico: {macro_ai.get('risk')}

Beschrijf de bredere marktomgeving.
"""


def prompt_setup_positioning(setup, scores):
    if not setup:
        return "Er is momenteel geen valide setup actief. Beschrijf wat dat betekent voor positionering."

    return f"""
Setup naam: {setup.get('name')}
Timeframe: {setup.get('timeframe')}

Scores:
Macro: {scores.get('macro_score')}
Technisch: {scores.get('technical_score')}
Markt: {scores.get('market_score')}

Beschrijf of deze setup op dit moment relevant is.
"""


def prompt_strategy_implication(scores):
    return f"""
Macro score: {scores.get('macro_score')}
Technisch score: {scores.get('technical_score')}
Markt score: {scores.get('market_score')}
Setup score: {scores.get('setup_score')}

Wat betekent dit praktisch voor strategie en houding?
"""


def prompt_outlook():
    return """
Geef exact drie zinnen:
1. bullish scenario
2. bearish scenario
3. consolidatie scenario
"""


# =====================================================
# 🚀 MAIN BUILDER
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
        "executive_summary": generate_section(
            prompt_executive_summary(scores, market)
        ),
        "macro_context": generate_section(
            prompt_market_context(ai.get("macro", {}))
        ),
        "setup_validation": generate_section(
            prompt_setup_positioning(setup, scores)
        ),
        "strategy_implication": generate_section(
            prompt_strategy_implication(scores)
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
