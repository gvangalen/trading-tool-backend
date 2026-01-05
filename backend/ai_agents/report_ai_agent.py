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
- Je krijgt dashboard-data (scores, indicatoren, setups, strategie)
- Je krijgt AI-insights en AI-reflections als extra context
- Je krijgt het rapport van de vorige dag
- Deze informatie gebruik je om samenhang en continuïteit te creëren

Belangrijk:
- Bouw expliciet voort op gisteren
- Verklaar waarom indicatoren vandaag hoger/lager zijn
- Trek geen nieuwe conclusies zonder data
- Gebruik geen absolute prijsniveaus behalve de actuele prijs

Stijl:
- Vloeiend, professioneel Nederlands
- Doorlopend verhaal, geen losse blokken
- Geen AI-termen, geen labels, geen opsommingen
- Geen uitleg van basisbegrippen
- Geen herhaling van cijfers zonder reden
- Klinkt als één analist, niet als meerdere losse modules

Structuur:
- Elke sectie is één logisch doorlopend stuk tekst
- Secties moeten inhoudelijk op elkaar aansluiten

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
# PROMPTS (REPORT AGENT 2.0 — SAMENHANG & VERKLARING)
# =====================================================

def p_exec(scores, market):
    return f"""
Vat de huidige marktsituatie samen tot één helder dagoverzicht.

Gebruik hierbij:
- de combinatie van macro-, market-, technical- en setup-scores
- de actuele prijs en recente marktactiviteit
- de context van gisteren indien relevant

Doel:
- schets het grotere plaatje
- benoem of de markt vandaag uitnodigt tot actie of juist terughoudendheid
- geef één duidelijk, professioneel handelsoordeel
"""


def p_market(scores, market, indicators):
    names = ", ".join(i["indicator"] for i in indicators)

    return f"""
De market score staat vandaag op {scores.get('market_score')}.

Belangrijke marktindicatoren zijn: {names}.

Ga inhoudelijk in op:
- waarom deze indicatoren vandaag hoger of lager scoren
- wat dit zegt over volatiliteit, liquiditeit en richting
- of dit een voortzetting is van gisteren of juist een verandering

Beschrijf dit als één logisch verhaal, geen losse observaties.
"""


def p_macro(scores, indicators):
    names = ", ".join(i["indicator"] for i in indicators)

    return f"""
De macro score staat vandaag op {scores.get('macro_score')}.

Relevante macro-indicatoren zijn: {names}.

Leg uit:
- welke krachten vandaag dominant zijn in de macro-omgeving
- waarom deze indicatoren zo scoren
- hoe dit het bredere speelveld voor Bitcoin beïnvloedt

Koppel expliciet terug naar risico, timing en positionering.
"""


def p_technical(scores, indicators):
    weak = ", ".join(i["indicator"] for i in indicators if i.get("score") is not None and i["score"] < 40)
    strong = ", ".join(i["indicator"] for i in indicators if i.get("score") is not None and i["score"] >= 60)

    return f"""
De technische score komt vandaag uit op {scores.get('technical_score')}.

Analyseer de technische structuur door:
- uit te leggen waarom bepaalde indicatoren zwakker zijn ({weak})
- te verklaren waarom andere indicatoren steun geven ({strong})
- te beschrijven wat dit zegt over trend, momentum en betrouwbaarheid

Vermijd algemene termen en koppel alles aan de huidige marktconditie.
"""


def p_setup(best_setup):
    if not best_setup:
        return """
Er is vandaag geen setup die voldoende aansluit bij de huidige marktomstandigheden.

Leg uit:
- waarom setups momenteel niet goed passen
- wat er zou moeten veranderen voordat dat wel zo is
"""

    return f"""
De best scorende setup vandaag is {best_setup.get('name')}
op timeframe {best_setup.get('timeframe')} met een score van {best_setup.get('score')}.

Beoordeel:
- waarom deze setup relatief beter scoort dan de rest
- of de marktomstandigheden deze setup daadwerkelijk ondersteunen
- of dit een setup is om actief te gebruiken of slechts te monitoren
"""


def p_strategy(scores, active_strategy):
    if not active_strategy:
        return """
Er is momenteel geen actieve strategie.

Licht toe:
- waarom de huidige scorecombinatie geen duidelijke strategie rechtvaardigt
- welke voorwaarden eerst vervuld moeten worden voordat actie logisch wordt
"""

    return """
Er is een actieve strategie aanwezig.

Analyseer deze strategie door:
- haar te plaatsen binnen de huidige macro-, market- en technische context
- de belangrijkste risico’s en aannames te benoemen
- te beoordelen of deze strategie vandaag ongewijzigd geldig blijft
"""


# =====================================================
# MAIN BUILDER — REPORT AGENT 2.0 (SAFE + CONTEXT-AWARE)
# =====================================================
def generate_daily_report_sections(user_id: int) -> Dict[str, Any]:
    """
    Report Agent 2.0
    - Leest ALLE bestaande data
    - Leest AI-insights & reflections
    - Leest vorig rapport
    - Bouwt één samenhangend dagverhaal
    - JSON-safe (Decimal / date proof)
    """

    # -------------------------------------------------
    # 0) JSON-safe helper (CRUCIAAL)
    # -------------------------------------------------
    from datetime import date, datetime

    def _safe_json(obj):
        if obj is None:
            return None
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, (date, datetime)):
            return obj.isoformat()
        if isinstance(obj, dict):
            return {k: _safe_json(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple)):
            return [_safe_json(v) for v in obj]
        return obj

    # -------------------------------------------------
    # 1) Basis data
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
    # 2) Extra context (AI + vorig rapport)
    # -------------------------------------------------
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:

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

            cur.execute("""
                SELECT category, avg_score, trend, bias, risk, summary
                FROM ai_category_insights
                WHERE user_id = %s
                ORDER BY date DESC
                LIMIT 10;
            """, (user_id,))
            ai_insights = cur.fetchall()

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
    # 3) Context blob
    # -------------------------------------------------
    context_blob = f"""
Je schrijft het rapport voor vandaag.
Gebruik UITSLUITEND onderstaande data.

=== ACTUELE MARKT ===
Prijs: {market.get("price")}
24h verandering: {market.get("change_24h")}
Volume: {market.get("volume")}

=== SCORES ===
Macro: {scores.get("macro_score")}
Technisch: {scores.get("technical_score")}
Market: {scores.get("market_score")}
Setup: {scores.get("setup_score")}

=== MARKET INDICATORS ===
{json.dumps(_safe_json(market_ind), ensure_ascii=False)}

=== MACRO INDICATORS ===
{json.dumps(_safe_json(macro_ind), ensure_ascii=False)}

=== TECHNICAL INDICATORS ===
{json.dumps(_safe_json(tech_ind), ensure_ascii=False)}

=== BESTE SETUP ===
{json.dumps(_safe_json(best_setup), ensure_ascii=False)}

=== ACTIEVE STRATEGIE ===
{json.dumps(_safe_json(active_strategy), ensure_ascii=False)}

=== AI INSIGHTS ===
{json.dumps(_safe_json(ai_insights), ensure_ascii=False)}

=== AI REFLECTIONS ===
{json.dumps(_safe_json(ai_reflections), ensure_ascii=False)}

=== VORIG RAPPORT ===
{json.dumps(_safe_json(prev_report), ensure_ascii=False)}

BELANGRIJK:
- Geen absolute prijsniveaus noemen
- Verklaar WAAROM indicatoren vandaag hoger/lager zijn
- Bouw logisch voort op gisteren
""".strip()

    # -------------------------------------------------
    # 4) Tekst genereren
    # -------------------------------------------------
    executive_summary = generate_text(context_blob + "\n\n" + p_exec(scores, market), "Markt in afwachting.")
    market_analysis = generate_text(context_blob + "\n\n" + p_market(scores, market, market_ind), "Beperkte richting.")
    macro_context = generate_text(context_blob + "\n\n" + p_macro(scores, macro_ind), "Macro gemengd.")
    technical_analysis = generate_text(context_blob + "\n\n" + p_technical(scores, tech_ind), "Technisch voorzichtig.")
    setup_validation = generate_text(context_blob + "\n\n" + p_setup(best_setup), "Selectieve setups.")
    strategy_implication = generate_text(context_blob + "\n\n" + p_strategy(scores, active_strategy), "Voorzichtigheid.")
    outlook = generate_text(
        context_blob + "\n\nSchrijf een scenario-vooruitblik zonder prijsniveaus.",
        "Vooruitblik: wachten op bevestiging."
    )

    # -------------------------------------------------
    # 5) RETURN (DIT WAS EERDER NOOIT BEREIKT)
    # -------------------------------------------------
    result = {
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

    logger.info("✅ Report agent OK, return keys=%s", list(result.keys()))
    return result
