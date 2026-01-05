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
# REPORT AGENT ROLE
# =====================================================
REPORT_TASK = """
Je bent een ervaren Bitcoin market analyst.
Je schrijft een dagelijks rapport voor een ervaren gebruiker.

Context:
- De lezer kent Bitcoin
- De lezer ziet macro-, market-, technical- en setup-scores
- De lezer ziet indicator-cards in een dashboard
- Jij vat dit alles samen tot één professioneel dagrapport

Stijl:
- Normaal, vloeiend Nederlands
- Volledige zinnen, korte alinea’s
- Geen AI-termen, geen labels, geen opsommingen
- Geen uitleg van basisbegrippen
- Geen exacte cijfers herhalen tenzij functioneel
- Schrijf als een menselijke analist

Regels:
- Gebruik uitsluitend aangeleverde data
- Geen aannames
- Geen markdown
- Elke sectie is één doorlopende tekst

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


def generate_text(prompt: str, fallback: str) -> str:
    system_prompt = build_system_prompt(agent="report", task=REPORT_TASK)
    raw = ask_gpt_text(prompt, system_role=system_prompt)

    if not raw:
        return fallback

    try:
        parsed = json.loads(raw)
        parts = _flatten_text(parsed)

        blacklist = {
            "GO", "NO-GO", "STATUS", "RISICO", "IMPACT",
            "ACTIE", "ONVOLDOENDE DATA", "CONDITIONAL"
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

    text = raw.strip()
    return text if len(text) > 5 else fallback


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
                ORDER BY report_date DESC
                LIMIT 1;
            """, (user_id,))
            row = cur.fetchone()

        return {
            "macro_score": to_float(row[0]) if row else None,
            "technical_score": to_float(row[1]) if row else None,
            "market_score": to_float(row[2]) if row else None,
            "setup_score": to_float(row[3]) if row else None,
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

        return {
            "price": to_float(row[0]) if row else None,
            "change_24h": to_float(row[1]) if row else None,
            "volume": to_float(row[2]) if row else None,
        }
    finally:
        conn.close()


def _indicator_list(cur, sql, user_id):
    cur.execute(sql, (user_id,))
    rows = cur.fetchall()
    return [{
        "indicator": r[0],
        "value": to_float(r[1]),
        "score": to_float(r[2]),
        "interpretation": r[3],
    } for r in rows]

# =====================================================
# INDICATOR HIGHLIGHTS (UNIFORM STRUCTUUR – GEEN DUPLICATEN)
# =====================================================

def get_market_indicator_highlights(user_id: int) -> List[dict]:
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            return _indicator_list(cur, """
                SELECT DISTINCT ON (name)
                    name,
                    value,
                    score,
                    interpretation
                FROM market_data_indicators
                WHERE user_id = %s
                  AND score IS NOT NULL
                  AND DATE(timestamp) = CURRENT_DATE
                ORDER BY name, timestamp DESC
                LIMIT 5;
            """, user_id)
    finally:
        conn.close()


def get_macro_indicator_highlights(user_id: int) -> List[dict]:
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            return _indicator_list(cur, """
                SELECT DISTINCT ON (name)
                    name,
                    value,
                    score,
                    COALESCE(interpretation, action)
                FROM macro_data
                WHERE user_id = %s
                  AND score IS NOT NULL
                  AND DATE(timestamp) = CURRENT_DATE
                ORDER BY name, timestamp DESC
                LIMIT 5;
            """, user_id)
    finally:
        conn.close()


def get_technical_indicator_highlights(user_id: int) -> List[dict]:
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            return _indicator_list(cur, """
                SELECT DISTINCT ON (indicator)
                    indicator,
                    value,
                    score,
                    COALESCE(uitleg, advies)
                FROM technical_indicators
                WHERE user_id = %s
                  AND score IS NOT NULL
                  AND DATE(timestamp) = CURRENT_DATE
                ORDER BY indicator, timestamp DESC
                LIMIT 5;
            """, user_id)
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
                ORDER BY d.report_date DESC, d.is_best DESC, d.score DESC
                LIMIT 1;
            """, (user_id,))
            best = cur.fetchone()

            cur.execute("""
                SELECT s.id, s.name, d.score
                FROM daily_setup_scores d
                JOIN setups s ON s.id = d.setup_id
                WHERE d.user_id = %s
                ORDER BY d.report_date DESC, d.score DESC
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
                    s.name, s.symbol, s.timeframe,
                    a.entry, a.targets, a.stop_loss,
                    a.adjustment_reason, a.confidence_score
                FROM active_strategy_snapshot a
                JOIN setups s ON s.id = a.setup_id
                WHERE a.user_id = %s
                ORDER BY a.snapshot_date DESC, a.created_at DESC
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
# PROMPTS
# =====================================================
def p_exec(scores, market):
    return f"""
Macro score {scores.get('macro_score')}, technische score {scores.get('technical_score')},
markt score {scores.get('market_score')} en setup score {scores.get('setup_score')}.
De prijs beweegt recent met beperkte volatiliteit.
Vat de huidige situatie samen en geef een duidelijk handelsoordeel.
"""


def p_market(scores, market, indicators):
    names = ", ".join(i["indicator"] for i in indicators)
    return f"""
De markt toont een markt score van {scores.get('market_score')}.
Belangrijke indicatoren vandaag zijn {names}.
Beschrijf volatiliteit, richting en activiteit.
"""


def p_macro(scores, indicators):
    names = ", ".join(i["indicator"] for i in indicators)
    return f"""
Macro score is {scores.get('macro_score')}.
Belangrijke macrofactoren zijn {names}.
Beschrijf de macro-context en relevantie voor Bitcoin.
"""


def p_technical(scores, indicators):
    weak = ", ".join(i["indicator"] for i in indicators if i["score"] < 40)
    return f"""
Technische score is {scores.get('technical_score')}.
Zwakke indicatoren zijn {weak}.
Beschrijf trend, momentum en betrouwbaarheid.
"""


def p_setup(best_setup):
    if not best_setup:
        return "Er is vandaag geen setup die voldoende aansluit bij de marktomstandigheden."
    return f"""
De beste setup is {best_setup.get('name')} op timeframe {best_setup.get('timeframe')}.
De score ligt op {best_setup.get('score')}.
Beoordeel de praktische inzetbaarheid vandaag.
"""


def p_strategy(scores, active_strategy):
    if not active_strategy:
        return "Er is geen actieve strategie omdat de huidige scorecombinatie geen duidelijke actie ondersteunt."
    return """
Er is een actieve strategie aanwezig.
Beoordeel deze strategie in relatie tot het huidige marktbeeld en de risico’s.
"""


# =====================================================
# MAIN BUILDER
# =====================================================
def generate_daily_report_sections(user_id: int) -> Dict[str, Any]:
    """
    Report Agent 2.0
    - Leest ALLE bestaande data (scores, market, indicators, setups, strategy)
    - Leest bestaande AI-context (ai_category_insights, ai_reflections)
    - Leest vorig rapport
    - Schrijft één samenhangend dagverhaal
    """

    # -------------------------------------------------
    # 1) Data ophalen (ongewijzigd gedrag)
    # -------------------------------------------------
    scores = get_daily_scores(user_id)
    market = get_market_snapshot()

    market_ind = get_market_indicator_highlights(user_id)
    macro_ind = get_macro_indicator_highlights(user_id)
    tech_ind = get_technical_indicator_highlights(user_id)

    setup_snapshot = get_setup_snapshot(user_id)
    best_setup = setup_snapshot.get("best_setup")
    active_strategy = get_active_strategy_snapshot(user_id)

    # -------------------------------------------------
    # 2) Extra context ophalen (AI + gisteren)
    # -------------------------------------------------
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:

            # Laatste report vóór vandaag
            cur.execute("""
                SELECT report_date, executive_summary, market_analysis,
                       macro_context, technical_analysis,
                       setup_validation, strategy_implication, outlook
                FROM daily_reports
                WHERE user_id = %s
                  AND report_date < CURRENT_DATE
                ORDER BY report_date DESC
                LIMIT 1;
            """, (user_id,))
            prev_report = cur.fetchone()

            # AI category insights
            cur.execute("""
                SELECT category, avg_score, trend, bias, risk, summary
                FROM ai_category_insights
                WHERE user_id = %s
                ORDER BY date DESC
                LIMIT 10;
            """, (user_id,))
            ai_insights = cur.fetchall()

            # AI reflections
            cur.execute("""
                SELECT category, indicator, ai_score, comment, recommendation
                FROM ai_reflections
                WHERE user_id = %s
                ORDER BY date DESC
                LIMIT 10;
            """, (user_id,))
            ai_reflections = cur.fetchall()

    finally:
        conn.close()

    # -------------------------------------------------
    # 3) Context blob (DE ENIGE extra intelligentie)
    # -------------------------------------------------
    context_blob = f"""
Je schrijft het rapport voor vandaag.
Gebruik UITSLUITEND onderstaande data.

=== ACTUELE MARKT (enige geldige prijsbron) ===
Prijs: {market.get("price")}
24h verandering: {market.get("change_24h")}
Volume: {market.get("volume")}

=== SCORES ===
Macro: {scores.get("macro_score")}
Technisch: {scores.get("technical_score")}
Market: {scores.get("market_score")}
Setup: {scores.get("setup_score")}

=== MARKET INDICATORS ===
{json.dumps(market_ind, ensure_ascii=False)}

=== MACRO INDICATORS ===
{json.dumps(macro_ind, ensure_ascii=False)}

=== TECHNICAL INDICATORS ===
{json.dumps(tech_ind, ensure_ascii=False)}

=== BESTE SETUP ===
{json.dumps(best_setup, ensure_ascii=False)}

=== ACTIEVE STRATEGIE ===
{json.dumps(active_strategy, ensure_ascii=False)}

=== AI CATEGORY INSIGHTS (context, niet kopiëren) ===
{json.dumps(ai_insights, ensure_ascii=False)}

=== AI REFLECTIONS (context, niet kopiëren) ===
{json.dumps(ai_reflections, ensure_ascii=False)}

=== VORIG RAPPORT ===
{json.dumps(prev_report, ensure_ascii=False)}

BELANGRIJK:
- Geen absolute prijsniveaus noemen
- Verklaar WAAROM indicatoren vandaag hoger/lager zijn
- Bouw logisch voort op gisteren
- Schrijf als één doorlopend verhaal
""".strip()

    # -------------------------------------------------
    # 4) Tekst genereren (zelfde prompts, extra context)
    # -------------------------------------------------
    executive_summary = generate_text(
        context_blob + "\n\n" + p_exec(scores, market),
        "De markt bevindt zich in een afwachtende fase."
    )

    market_analysis = generate_text(
        context_blob + "\n\n" + p_market(scores, market, market_ind),
        "De markt toont beperkte richting."
    )

    macro_context = generate_text(
        context_blob + "\n\n" + p_macro(scores, macro_ind),
        "De macro-omgeving blijft gemengd."
    )

    technical_analysis = generate_text(
        context_blob + "\n\n" + p_technical(scores, tech_ind),
        "Technisch ontbreekt overtuiging."
    )

    setup_validation = generate_text(
        context_blob + "\n\n" + p_setup(best_setup),
        "De huidige setups zijn selectief inzetbaar."
    )

    strategy_implication = generate_text(
        context_blob + "\n\n" + p_strategy(scores, active_strategy),
        "Voorzichtigheid blijft gepast."
    )

    outlook = generate_text(
        context_blob + """
Schrijf een vooruitblik in scenario-vorm (bullish / bearish / range),
zonder absolute prijsniveaus te noemen.
Koppel scenario’s aan indicator-gedrag.
""",
        "Vooruitblik: geduld tot bevestiging."
    )

    # -------------------------------------------------
    # 5) Return payload (frontend blijft exact werken)
    # -------------------------------------------------
    return {
        "executive_summary": executive_summary,
        "market_analysis": market_analysis,
        "macro_context": macro_context,
        "technical_analysis": technical_analysis,
        "setup_validation": setup_validation,
        "strategy_implication": strategy_implication,
        "outlook": outlook,

        "price": market.get("price"),
        "change_24h": market.get("change_24h"),
        "volume": market.get("volume"),

        "macro_score": scores.get("macro_score"),
        "technical_score": scores.get("technical_score"),
        "market_score": scores.get("market_score"),
        "setup_score": scores.get("setup_score"),

        "market_indicator_highlights": market_ind,
        "macro_indicator_highlights": macro_ind,
        "technical_indicator_highlights": tech_ind,

        "best_setup": best_setup,
        "top_setups": setup_snapshot.get("top_setups", []),
        "active_strategy": active_strategy,
    }
