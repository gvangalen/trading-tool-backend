import logging
import json
from decimal import Decimal
from typing import Dict, Any, List, Optional

from backend.utils.db import get_db_connection
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
- De lezer kent Bitcoin
- De lezer ziet scores en indicatoren in een dashboard
- Jij vat dit samen tot een professioneel dagrapport

Stijl:
- Normaal, vloeiend Nederlands
- Volledige zinnen en korte alinea’s
- Geen AI-termen, geen labels, geen bullets
- Geen educatie of uitleg van basisbegrippen
- Geen herhaling van exacte cijfers tenzij functioneel
- Klinkt als een menselijke analist

Regels:
- Gebruik uitsluitend aangeleverde data
- Geen aannames
- Geen markdown
- Elke sectie is één samenhangende tekst

Output = geldige JSON
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


def _flatten_text(obj) -> List[str]:
    """
    Haalt ALLE bruikbare tekst uit nested AI JSON.
    Lost lege Market Analyse en rommel-output definitief op.
    """
    out = []

    if obj is None:
        return out

    if isinstance(obj, str):
        t = obj.strip()
        if t:
            out.append(t)
        return out

    if isinstance(obj, dict):
        for v in obj.values():
            out.extend(_flatten_text(v))
        return out

    if isinstance(obj, list):
        for v in obj:
            out.extend(_flatten_text(v))
        return out

    return out


def generate_text(prompt: str) -> str:
    system_prompt = build_system_prompt(agent="report", task=REPORT_TASK)
    raw = ask_gpt_text(prompt, system_role=system_prompt)

    if not raw:
        return "Onvoldoende data om hier een zinvolle analyse van te maken."

    try:
        parsed = json.loads(raw)
        parts = _flatten_text(parsed)

        blacklist = {
            "GO", "NO-GO", "STATUS", "RISICO", "IMPACT",
            "ACTIE", "ONVOLDOENDE DATA"
        }

        cleaned = [
            p for p in parts
            if p.strip() and p.strip().upper() not in blacklist
        ]

        if cleaned:
            return "\n\n".join(cleaned)

        if parts:
            return "\n\n".join(parts)

    except Exception:
        pass

    return raw.strip()


# =====================================================
# SCORES & MARKET
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
            row = cur.fetchone()

        if not row:
            return {}

        return {
            "price": to_float(row[0]),
            "change_24h": to_float(row[1]),
            "volume": to_float(row[2]),
        }
    finally:
        conn.close()


# =====================================================
# INDICATOR HIGHLIGHTS (UNIFORM STRUCTUUR)
# =====================================================
def get_market_indicator_highlights(user_id: int) -> List[dict]:
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
                  AND score IS NOT NULL
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
                SELECT name, value, score, COALESCE(interpretation, action)
                FROM macro_data
                WHERE user_id = %s
                  AND score IS NOT NULL
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
# SETUP SNAPSHOT
# =====================================================
def get_setup_snapshot(user_id: int) -> Dict[str, Any]:
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT s.id, s.name, s.symbol, s.timeframe, d.score
                FROM daily_setup_scores d
                JOIN setups s ON s.id = d.setup_id
                WHERE d.user_id = %s
                  AND d.report_date = CURRENT_DATE
                ORDER BY d.is_best DESC, d.score DESC
                LIMIT 1;
            """, (user_id,))
            best = cur.fetchone()

            cur.execute("""
                SELECT s.id, s.name, d.score
                FROM daily_setup_scores d
                JOIN setups s ON s.id = d.setup_id
                WHERE d.user_id = %s
                  AND d.report_date = CURRENT_DATE
                ORDER BY d.score DESC
                LIMIT 5;
            """, (user_id,))
            rows = cur.fetchall()

        if not best:
            return {}

        return {
            "best_setup": {
                "id": best[0],
                "name": best[1],
                "symbol": best[2],
                "timeframe": best[3],
                "score": to_float(best[4]),
            },
            "top_setups": [
                {"id": r[0], "name": r[1], "score": to_float(r[2])}
                for r in rows
            ],
        }
    finally:
        conn.close()


# =====================================================
# STRATEGY SNAPSHOT
# =====================================================
def get_active_strategy_snapshot(user_id: int) -> Optional[Dict[str, Any]]:
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    s.name,
                    s.symbol,
                    s.timeframe,
                    a.entry,
                    a.targets,
                    a.stop_loss,
                    a.adjustment_reason,
                    a.confidence_score
                FROM active_strategy_snapshot a
                JOIN setups s ON s.id = a.setup_id
                WHERE a.user_id = %s
                  AND a.snapshot_date = CURRENT_DATE
                ORDER BY a.created_at DESC
                LIMIT 1;
            """, (user_id,))
            row = cur.fetchone()

        if not row:
            return None

        return {
            "setup_name": row[0],
            "symbol": row[1],
            "timeframe": row[2],
            "entry": to_float(row[3]),
            "targets": row[4],
            "stop_loss": to_float(row[5]),
            "adjustment_reason": row[6],
            "confidence_score": to_float(row[7]),
        }
    finally:
        conn.close()


# =====================================================
# PROMPTS (FUNCTIONEEL & INFORMATIEF)
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
Beschrijf wat dit zegt over volatiliteit, richting en activiteit.
"""


def p_macro(scores):
    return f"""
Macro score: {scores.get('macro_score')}
Beschrijf de macro-omgeving en implicaties voor Bitcoin.
"""


def p_technical(scores):
    return f"""
Technische score: {scores.get('technical_score')}
Beschrijf trend, momentum en technische betrouwbaarheid.
"""


def p_setup(best_setup):
    if not best_setup:
        return "Er is vandaag geen setup die voldoende aansluit bij de huidige marktomstandigheden."
    return f"""
Beste setup: {best_setup.get('name')} ({best_setup.get('timeframe')})
Score: {best_setup.get('score')}
Beoordeel de bruikbaarheid van deze setup vandaag.
"""


def p_strategy(scores, active_strategy):
    if not active_strategy:
        return """
Er is geen actieve strategie op deze rapportdatum.
De huidige scores rechtvaardigen geen concrete handelsactie.
"""
    return f"""
Er is een actieve strategie gekoppeld aan de huidige setup.
Beoordeel deze strategie in relatie tot de macro-, markt- en technische scores.
"""


# =====================================================
# MAIN BUILDER
# =====================================================
def generate_daily_report_sections(user_id: int) -> Dict[str, Any]:
    scores = get_daily_scores(user_id)
    market = get_market_snapshot()
    setup_snapshot = get_setup_snapshot(user_id)
    active_strategy = get_active_strategy_snapshot(user_id)

    return {
        # Narrative
        "executive_summary": generate_text(p_exec(scores, market)),
        "market_analysis": generate_text(p_market(scores, market)),
        "macro_context": generate_text(p_macro(scores)),
        "technical_analysis": generate_text(p_technical(scores)),
        "setup_validation": generate_text(
            p_setup(setup_snapshot.get("best_setup"))
        ),
        "strategy_implication": generate_text(
            p_strategy(scores, active_strategy)
        ),

        # Snapshot
        "price": market.get("price"),
        "change_24h": market.get("change_24h"),
        "volume": market.get("volume"),

        # Scores
        "macro_score": scores.get("macro_score"),
        "technical_score": scores.get("technical_score"),
        "market_score": scores.get("market_score"),
        "setup_score": scores.get("setup_score"),

        # Indicator cards
        "market_indicator_highlights": get_market_indicator_highlights(user_id),
        "macro_indicator_highlights": get_macro_indicator_highlights(user_id),
        "technical_indicator_highlights": get_technical_indicator_highlights(user_id),

        # Setup & strategy cards
        "best_setup": setup_snapshot.get("best_setup"),
        "top_setups": setup_snapshot.get("top_setups", []),
        "active_strategy": active_strategy,
    }
