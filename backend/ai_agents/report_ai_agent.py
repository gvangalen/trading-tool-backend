import logging
import json
import re
from difflib import SequenceMatcher
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

Output:
- Schrijf ALLEEN doorlopende tekst
- GEEN JSON
- GEEN codeblokken
- GEEN markdown
- GEEN labels of keys
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
    out: List[str] = []
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

def get_daily_deltas(user_id: int) -> Dict[str, Any]:
    """
    Berekent veranderingen t.o.v. gisteren.
    Dit is analytische brandstof voor het rapport.
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    today.macro_score - prev.macro_score AS macro_delta,
                    today.technical_score - prev.technical_score AS technical_delta,
                    today.market_score - prev.market_score AS market_delta,
                    today.setup_score - prev.setup_score AS setup_delta,
                    today.price - prev.price AS price_delta,
                    today.change_24h - prev.change_24h AS change_delta,
                    today.volume - prev.volume AS volume_delta
                FROM (
                    SELECT macro_score, technical_score, market_score, setup_score,
                           price, change_24h, volume
                    FROM daily_scores ds
                    JOIN market_data md ON DATE(md.timestamp) = ds.report_date
                    WHERE ds.user_id = %s
                    ORDER BY ds.report_date DESC
                    LIMIT 1
                ) today
                JOIN (
                    SELECT macro_score, technical_score, market_score, setup_score,
                           price, change_24h, volume
                    FROM daily_scores ds
                    JOIN market_data md ON DATE(md.timestamp) = ds.report_date
                    WHERE ds.user_id = %s
                      AND ds.report_date < CURRENT_DATE
                    ORDER BY ds.report_date DESC
                    LIMIT 1
                ) prev ON TRUE;
            """, (user_id, user_id))
            row = cur.fetchone()

        if not row:
            return {}

        keys = [
            "macro_delta", "technical_delta", "market_delta", "setup_delta",
            "price_delta", "change_delta", "volume_delta"
        ]

        return {k: to_float(v) for k, v in zip(keys, row)}

    finally:
        conn.close()

# =====================================================
# BOT DAILY SNAPSHOT (BACKEND = TRUTH)
# =====================================================
def get_bot_daily_snapshot(user_id: int) -> Dict[str, Any]:
    """
    Leest de botbeslissing van vandaag.

    CONTRACT (frontend + report + pdf):
    {
      bot_name: str,
      action: "buy" | "sell" | "hold",
      confidence: float | str | None,
      amount_eur: float | None,
      setup_match: str | None,
      reason: str | None
    }

    BELANGRIJK:
    - Deze functie retourneert ALTIJD een dict
    - HOLD is een geldige, bewuste beslissing
    - setup_match is ALTIJD string of None (NOOIT object)
    """

    conn = get_db_connection()
    if not conn:
        return {
            "bot_name": "Bot",
            "action": "hold",
            "confidence": None,
            "amount_eur": None,
            "setup_match": None,
            "reason": "Geen databaseverbinding — bot snapshot niet beschikbaar.",
        }

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                  b.name AS bot_name,
                  d.action,
                  d.confidence,
                  d.amount_eur,
                  d.scores_json,
                  d.reason_json
                FROM bot_decisions d
                JOIN bot_configs b ON b.id = d.bot_id
                WHERE d.user_id = %s
                  AND d.decision_date = CURRENT_DATE
                ORDER BY d.updated_at DESC
                LIMIT 1;
            """, (user_id,))
            row = cur.fetchone()

        # ─────────────────────────────────────────────
        # Geen bot decision vandaag → expliciete HOLD
        # ─────────────────────────────────────────────
        if not row:
            return {
                "bot_name": "Bot",
                "action": "hold",
                "confidence": None,
                "amount_eur": None,
                "setup_match": None,
                "reason": "Geen botbeslissing vandaag — drempels of voorwaarden niet gehaald.",
            }

        bot_name, action, confidence, amount_eur, scores_json, reason_json = row

        # ─────────────────────────────────────────────
        # Action normaliseren
        # ─────────────────────────────────────────────
        normalized_action = (action or "hold").lower()
        if normalized_action not in ("buy", "sell", "hold"):
            normalized_action = "hold"

        # ─────────────────────────────────────────────
        # scores_json veilig parsen
        # ─────────────────────────────────────────────
        if scores_json is None:
            scores_json = {}
        elif isinstance(scores_json, str):
            try:
                scores_json = json.loads(scores_json)
            except Exception:
                scores_json = {}

        # ─────────────────────────────────────────────
        # setup_match NORMALISEREN → STRING
        # ─────────────────────────────────────────────
        raw_match = scores_json.get("setup_match")
        setup_match = None

        if isinstance(raw_match, dict):
            setup_match = (
                raw_match.get("name")
                or raw_match.get("label")
                or raw_match.get("id")
            )
        elif isinstance(raw_match, list):
            setup_match = ", ".join(
                str(
                    x.get("name")
                    if isinstance(x, dict) and x.get("name")
                    else x
                )
                for x in raw_match
            )
        elif isinstance(raw_match, (str, int, float)):
            setup_match = str(raw_match)

        # ─────────────────────────────────────────────
        # reason_json → nette tekst
        # ─────────────────────────────────────────────
        reason_text = None

        if reason_json is not None:
            if isinstance(reason_json, str):
                try:
                    parsed = json.loads(reason_json)
                    reason_json = parsed
                except Exception:
                    reason_text = reason_json

            if reason_text is None:
                if isinstance(reason_json, list):
                    reason_text = "; ".join(
                        str(x) for x in reason_json if str(x).strip()
                    )
                elif isinstance(reason_json, dict):
                    if "reason" in reason_json:
                        reason_text = str(reason_json["reason"])
                    elif "reasons" in reason_json and isinstance(reason_json["reasons"], list):
                        reason_text = "; ".join(
                            str(x) for x in reason_json["reasons"] if str(x).strip()
                        )
                    else:
                        reason_text = str(reason_json)

        if normalized_action == "hold" and not reason_text:
            reason_text = "Geen trade: voorwaarden of risicodrempels niet gehaald."

        # ─────────────────────────────────────────────
        # amount / confidence veilig
        # ─────────────────────────────────────────────
        amount_val = None
        try:
            if amount_eur is not None:
                amount_val = float(amount_eur)
        except Exception:
            amount_val = None

        conf_val = confidence
        try:
            if isinstance(confidence, str) and confidence.strip().replace(".", "", 1).isdigit():
                conf_val = float(confidence)
        except Exception:
            conf_val = confidence

        return {
            "bot_name": bot_name or "Bot",
            "action": normalized_action,
            "confidence": conf_val,
            "amount_eur": amount_val,
            "setup_match": setup_match,
            "reason": reason_text,
        }

    finally:
        conn.close()


def generate_text(prompt: str, fallback: str) -> str:
    """
    Verantwoordelijk voor:
    - AI-call
    - opschonen output
    - JSON-defensieve parsing
    GEEN deduplicatie (doen we hogerop)
    """
    system_prompt = build_system_prompt(agent="report", task=REPORT_TASK)
    raw = ask_gpt_text(prompt, system_role=system_prompt)

    if not raw:
        return fallback

    # 1) Strip code fences / markdown
    text = raw.replace("```json", "").replace("```", "").strip()

    # 2) Als het normale tekst is → direct terug
    if not text.lstrip().startswith("{"):
        return text if len(text) > 5 else fallback

    # 3) Defensieve JSON-parse (als AI zich niet houdt aan instructies)
    try:
        parsed = json.loads(text)
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

    return text if len(text) > 5 else fallback


# =====================================================
# Repetition control (cross-section deduplication)
# =====================================================

def _normalize_sentence(s: str) -> str:
    s = s.lower().strip()
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"[^\w\s]", "", s)
    return s


def _is_too_similar(a: str, b: str, threshold: float = 0.82) -> bool:
    return SequenceMatcher(None, a, b).ratio() >= threshold


def reduce_repetition(text: str, seen: list[str]) -> str:
    """
    Verwijdert zinnen die semantisch te sterk lijken
    op eerder geschreven zinnen in andere secties.
    """
    sentences = re.split(r'(?<=[.!?])\s+', text)
    output: List[str] = []

    for s in sentences:
        norm = _normalize_sentence(s)

        if not norm or len(norm) < 20:
            continue

        if any(_is_too_similar(norm, prev) for prev in seen):
            continue

        output.append(s)
        seen.append(norm)

    return " ".join(output)


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
    return """
Formuleer één centrale markthypothese voor vandaag.

Structuur:
- Wat is het belangrijkste verschil t.o.v. gisteren?
- Waarom reageerde de markt juist vandaag?
- Is dit een structurele verschuiving of een reactieve beweging?

Schrijf als één analytisch openingsverhaal.
Geen opsommingen.
"""


def p_market(scores, market, indicators):
    return f"""
Analyseer de marktbeweging van vandaag.

Verplicht:
- Begin met de verandering t.o.v. gisteren
- Verklaar waarom prijs en market score veranderden
- Benoem waarom volume dit wel of niet ondersteunt
- Leg uit wat dit zegt over de duurzaamheid van de beweging

Geen herhaling van cijfers zonder causaliteit.
"""

def p_macro(scores, indicators):
    return f"""
Analyseer de macro-omgeving van vandaag.

Verplicht:
- Benoem welke macro-krachten dominant bleven
- Leg uit waarom macro-indicatoren niet meebewegen met prijs
- Beschrijf de spanning tussen veiligheid (Bitcoin) en risicobereidheid

Focus op krachten, niet op labels.
"""


def p_technical(scores, indicators):
    return f"""
Analyseer de technische structuur van vandaag.

Verplicht:
- Leg uit waarom technische signalen achterblijven of bevestigen
- Benoem welke signalen betrouwbaarheid ONDERMIJNEN
- Beschrijf of dit herstel, consolidatie of ruis is

Geen klassieke TA-uitleg.
Koppel alles aan betrouwbaarheid van de beweging.
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
# BOT STRATEGY PROMPT
# =====================================================
def p_bot_strategy(bot_snapshot: Dict[str, Any]) -> str:
    if bot_snapshot.get("action") == "hold":
        return """
De bot heeft vandaag bewust geen trade geplaatst.

Leg uit:
- welke voorwaarden of drempels niet voldeden
- waarom discipline en risicobeheer vandaag belangrijker waren dan actie
- wat er moet veranderen voordat een trade logisch wordt

Gebruik uitsluitend de aangeleverde botdata.
Voeg geen aannames toe en introduceer geen nieuwe beslissingen.
"""

    return """
Er is vandaag een botbeslissing genomen.

BELANGRIJK:
- De feitelijke botactie, confidence en bedragen worden elders getoond
- Herhaal of parafraseer deze NIET
- Jij geeft uitsluitend context en motivatie

Beschrijf:
- waarom deze beslissing logisch is binnen de huidige scorecombinatie
- hoe discipline en drempels doorslaggevend waren
- waarom dit een geschikt handelsmoment was

Gebruik uitsluitend de aangeleverde botdata.
Voeg geen aannames toe en introduceer geen nieuwe beslissingen.
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
    # 0) JSON-safe helper
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
    bot_snapshot = get_bot_daily_snapshot(user_id)

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
    # 3) Context blob (ENIGE input voor AI)
    # -------------------------------------------------
    deltas = get_daily_deltas(user_id)

context_blob = f"""
Je schrijft het rapport voor vandaag.
Gebruik UITSLUITEND onderstaande data.
Analyseer VERANDERINGEN, geen herhaling.

=== VERANDERINGEN T.O.V. GISTEREN ===
Macro score delta: {deltas.get("macro_delta")}
Market score delta: {deltas.get("market_delta")}
Technical score delta: {deltas.get("technical_delta")}
Setup score delta: {deltas.get("setup_delta")}
Prijs delta: {deltas.get("price_delta")}
24h change delta: {deltas.get("change_delta")}
Volume delta: {deltas.get("volume_delta")}

=== ACTUELE MARKT ===
Prijs: {market.get("price")}
24h verandering: {market.get("change_24h")}
Volume: {market.get("volume")}

=== SCORES ===
Macro: {scores.get("macro_score")}
Market: {scores.get("market_score")}
Technical: {scores.get("technical_score")}
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

=== BOT SNAPSHOT ===
{json.dumps(_safe_json(bot_snapshot), ensure_ascii=False)}

=== VORIG RAPPORT (TER REFERENTIE) ===
{json.dumps(_safe_json(prev_report), ensure_ascii=False)}

BELANGRIJK:
- Begin elke sectie met WAT IS VERANDERD
- Benoem WAAROM die verandering plaatsvond
- Benoem wat ONVERANDERD bleef ondanks beweging
- Geen herhaling van cijfers zonder verklaring
""".strip()

    # -------------------------------------------------
    # 4) Tekst genereren + herhaling reduceren
    # -------------------------------------------------
    seen_sentences: List[str] = []

    executive_summary = reduce_repetition(
        generate_text(context_blob + "\n\n" + p_exec(scores, market), "Markt in afwachting."),
        seen_sentences
    )

    market_analysis = reduce_repetition(
        generate_text(context_blob + "\n\n" + p_market(scores, market, market_ind), "Beperkte richting."),
        seen_sentences
    )

    macro_context = reduce_repetition(
        generate_text(context_blob + "\n\n" + p_macro(scores, macro_ind), "Macro gemengd."),
        seen_sentences
    )

    technical_analysis = reduce_repetition(
        generate_text(context_blob + "\n\n" + p_technical(scores, tech_ind), "Technisch voorzichtig."),
        seen_sentences
    )

    setup_validation = reduce_repetition(
        generate_text(context_blob + "\n\n" + p_setup(best_setup), "Selectieve setups."),
        seen_sentences
    )

    strategy_implication = reduce_repetition(
        generate_text(context_blob + "\n\n" + p_strategy(scores, active_strategy), "Voorzichtigheid."),
        seen_sentences
    )

    bot_strategy = reduce_repetition(
        generate_text(
            context_blob + "\n\n" + p_bot_strategy(bot_snapshot),
            "De bot bleef vandaag afwachtend."
        ),
        seen_sentences
    )

    outlook = reduce_repetition(
        generate_text(
            context_blob + "\n\nSchrijf een scenario-vooruitblik zonder prijsniveaus.",
            "Vooruitblik: wachten op bevestiging."
        ),
        seen_sentences
    )

    # -------------------------------------------------
    # 5) RESULT
    # -------------------------------------------------
    result = {
        "executive_summary": executive_summary,
        "market_analysis": market_analysis,
        "macro_context": macro_context,
        "technical_analysis": technical_analysis,
        "setup_validation": setup_validation,
        "strategy_implication": strategy_implication,
        "bot_strategy": bot_strategy,
        "bot_snapshot": bot_snapshot,
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
