import logging
import json
from decimal import Decimal
from typing import Dict, Any, List, Optional

from backend.utils.db import get_db_connection
from backend.utils.json_utils import sanitize_json_input
from backend.utils.openai_client import ask_gpt_text
from backend.ai_core.system_prompt_builder import build_system_prompt

# =====================================================
# Logging
# =====================================================
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# =====================================================
# 🎯 REPORT AGENT — DEFINITIEVE ROL
# =====================================================
REPORT_TASK = """
Je bent een ervaren Bitcoin market analyst.
Je schrijft een dagelijks rapport voor een ervaren gebruiker.

Context:
- De lezer kent Bitcoin.
- De lezer heeft een dashboard met scores en indicatoren.
- Jij schrijft de samenvatting NA het bekijken van dat dashboard.

Stijl:
- Normaal, vloeiend Nederlands
- Geen AI-termen, geen labels
- Geen herhaling van exacte cijfers tenzij functioneel
- Geen educatie of uitleg van basisbegrippen
- Klink als een menselijke analist

Regels:
- Gebruik uitsluitend aangeleverde data
- Geen aannames
- Geen markdown
- Elke sectie is één string
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


def generate_text(prompt: str) -> str:
    system_prompt = build_system_prompt(agent="report", task=REPORT_TASK)
    raw = ask_gpt_text(prompt, system_role=system_prompt)

    if not raw:
        return "Onvoldoende data om hier een zinvolle analyse van te maken."

    try:
        parsed = json.loads(raw)
        if isinstance(parsed, dict):
            return "\n\n".join(v.strip() for v in parsed.values() if isinstance(v, str))
    except Exception:
        pass

    return raw.strip()


# =====================================================
# DATA LOADERS — SCORES & MARKET
# =====================================================
def get_daily_scores(user_id: int) -> Dict[str, Any]:
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT macro_score, technical_score, market_score, setup_score
                FROM daily_scores
                WHERE user_id = %s
                  AND report_date = CURRENT_DATE
                LIMIT 1;
            """, (user_id,))
            r = cur.fetchone()

        if not r:
            return {}

        return {
            "macro_score": to_float(r[0]),
            "technical_score": to_float(r[1]),
            "market_score": to_float(r[2]),
            "setup_score": to_float(r[3]),
        }
    finally:
        conn.close()


def get_market_snapshot() -> Dict[str, Any]:
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT price, change_24h, volume
                FROM market_data
                ORDER BY timestamp DESC
                LIMIT 1;
            """)
            r = cur.fetchone()

        if not r:
            return {}

        return {
            "price": to_float(r[0]),
            "change_24h": to_float(r[1]),
            "volume": to_float(r[2]),
        }
    finally:
        conn.close()


# =====================================================
# INDICATOR HIGHLIGHTS
# =====================================================
def get_market_indicator_highlights(user_id: int) -> List[dict]:
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT name, value, score, interpretation
                FROM market_data_indicators
                WHERE user_id = %s
                  AND DATE(timestamp) = CURRENT_DATE
                ORDER BY score DESC;
            """, (user_id,))
            rows = cur.fetchall()

        return [{
            "indicator": r[0],
            "value": to_float(r[1]),
            "score": to_float(r[2]),
            "interpretation": r[3],
        } for r in rows]
    finally:
        conn.close()


def get_macro_indicator_highlights(user_id: int) -> List[dict]:
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT indicator, value, score, interpretation
                FROM macro_data
                WHERE user_id = %s
                  AND DATE(timestamp) = CURRENT_DATE
                ORDER BY score DESC;
            """, (user_id,))
            rows = cur.fetchall()

        return [{
            "indicator": r[0],
            "value": to_float(r[1]),
            "score": to_float(r[2]),
            "interpretation": r[3],
        } for r in rows]
    finally:
        conn.close()


def get_technical_indicator_highlights(user_id: int) -> List[dict]:
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT indicator, value, score, COALESCE(uitleg, advies)
                FROM technical_indicators
                WHERE user_id = %s
                  AND DATE(timestamp) = CURRENT_DATE
                ORDER BY score DESC;
            """, (user_id,))
            rows = cur.fetchall()

        return [{
            "indicator": r[0],
            "value": to_float(r[1]),
            "score": to_float(r[2]),
            "interpretation": r[3],
        } for r in rows]
    finally:
        conn.close()


# =====================================================
# ✅ SETUP SNAPSHOT (NIEUW)
# =====================================================
def get_setup_snapshot(user_id: int) -> Dict[str, Any]:
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT setup_name, symbol, timeframe, score
                FROM daily_setup_scores
                WHERE user_id = %s
                  AND report_date = CURRENT_DATE
                ORDER BY score DESC;
            """, (user_id,))
            rows = cur.fetchall()

        if not rows:
            return {
                "best_setup": None,
                "top_setups": []
            }

        best = rows[0]
        top = rows[:3]

        return {
            "best_setup": {
                "name": best[0],
                "symbol": best[1],
                "timeframe": best[2],
                "score": to_float(best[3]),
            },
            "top_setups": [{
                "name": r[0],
                "score": to_float(r[3]),
            } for r in top]
        }
    finally:
        conn.close()


# =====================================================
# ✅ STRATEGY SNAPSHOT (NIEUW)
# =====================================================
def get_active_strategy(user_id: int) -> Optional[dict]:
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT strategy_name, description, bias, risk_profile
                FROM active_strategy_snapshot
                WHERE user_id = %s
                  AND report_date = CURRENT_DATE
                LIMIT 1;
            """, (user_id,))
            r = cur.fetchone()

        if not r:
            return None

        return {
            "name": r[0],
            "description": r[1],
            "bias": r[2],
            "risk_profile": r[3],
        }
    finally:
        conn.close()


# =====================================================
# PROMPTS
# =====================================================
def p_exec(scores, market):
    return f"""
Macro score: {scores.get('macro_score')}
Technische score: {scores.get('technical_score')}
Markt score: {scores.get('market_score')}
Setup score: {scores.get('setup_score')}

Prijs: {market.get('price')}
24h verandering: {market.get('change_24h')}
"""


def p_market(scores, market):
    return f"""
Prijs: {market.get('price')}
24h verandering: {market.get('change_24h')}
Markt score: {scores.get('market_score')}
"""


def p_macro(scores):
    return f"Macro score: {scores.get('macro_score')}"


def p_technical(scores):
    return f"Technische score: {scores.get('technical_score')}"


def p_setup(best_setup):
    if not best_setup:
        return "Er is vandaag geen setup die voldoende valide is."
    return f"""
Beste setup: {best_setup.get('name')}
Score: {best_setup.get('score')}
"""


def p_strategy(scores):
    return f"""
Macro score: {scores.get('macro_score')}
Technische score: {scores.get('technical_score')}
Markt score: {scores.get('market_score')}
"""


# =====================================================
# 🚀 MAIN BUILDER
# =====================================================
def generate_daily_report_sections(user_id: int) -> Dict[str, Any]:
    logger.info(f"📄 Generating daily report | user_id={user_id}")

    scores = get_daily_scores(user_id)
    market = get_market_snapshot()
    setup_snapshot = get_setup_snapshot(user_id)
    active_strategy = get_active_strategy(user_id)

    return {
        # 🧠 NARRATIVE
        "executive_summary": generate_text(p_exec(scores, market)),
        "market_analysis": generate_text(p_market(scores, market)),
        "macro_context": generate_text(p_macro(scores)),
        "technical_analysis": generate_text(p_technical(scores)),
        "setup_validation": generate_text(p_setup(setup_snapshot.get("best_setup"))),
        "strategy_implication": generate_text(p_strategy(scores)),

        # 📊 MARKET SNAPSHOT
        "price": market.get("price"),
        "change_24h": market.get("change_24h"),
        "volume": market.get("volume"),

        # 📈 SCORES
        **scores,

        # 📋 INDICATORS
        "market_indicator_highlights": get_market_indicator_highlights(user_id),
        "macro_indicator_highlights": get_macro_indicator_highlights(user_id),
        "technical_indicator_highlights": get_technical_indicator_highlights(user_id),

        # 🧩 SETUP & STRATEGY CARDS
        "best_setup": setup_snapshot.get("best_setup"),
        "top_setups": setup_snapshot.get("top_setups"),
        "active_strategy": active_strategy,
    }
