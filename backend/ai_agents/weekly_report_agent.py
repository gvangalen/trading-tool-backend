# backend/ai_agents/weekly_report_agent.py

import logging
import json
import re
from difflib import SequenceMatcher
from decimal import Decimal
from datetime import date, timedelta
from typing import Dict, Any, List, Optional, Tuple

from backend.utils.db import get_db_connection
from backend.utils.openai_client import ask_gpt_text
from backend.ai_core.system_prompt_builder import build_system_prompt

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# =====================================================
# 🧠 WEEKLY REPORT AGENT ROLE (CANONICAL)
# =====================================================
REPORT_TASK = """
Je bent een ervaren Bitcoin market analyst.
Je schrijft een WEEKRAPPORT voor een ervaren gebruiker.

Context:
- Je krijgt meerdere dagrapporten (van dezelfde week)
- Je krijgt scores (macro/technical/market/setup) over de periode
- Je krijgt setup-performance data over de periode
- Je krijgt bot-activiteit (buy/sell/hold) over de periode
- Je gebruikt dit om patronen, verschuivingen en lessen te beschrijven

Belangrijk:
- Beschrijf trends en veranderingen over meerdere dagen
- Trek geen nieuwe conclusies zonder data
- Vermijd absolute prijsniveaus (geen vaste levels); beschrijf regime en gedrag
- Geen basisuitleg of educatie

Stijl:
- Vloeiend, professioneel Nederlands
- Doorlopend verhaal per sectie (geen lijstjes, geen labels)
- Geen AI-termen, geen meta-commentaar
- Geen herhaling van cijfers zonder reden
- Klinkt als één analist met overzicht

Structuur (exact deze output keys):
- executive_summary
- market_overview
- macro_trends
- technical_structure
- setup_performance
- bot_performance
- strategic_lessons
- outlook

Output:
- Schrijf ALLEEN doorlopende tekst (per key)
- GEEN JSON output in de tekstvelden
- GEEN codeblokken
- GEEN markdown
- Geen labels of keys in de tekst zelf
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


def safe_json(value: Any) -> Any:
    """Defensief: jsonb kan dict zijn, of string met json."""
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        try:
            return json.loads(s)
        except Exception:
            return value
    return value


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


def generate_text(prompt: str, fallback: str) -> str:
    """
    - AI-call
    - output opschonen
    - defensief omgaan met JSON-output (als AI tóch json teruggeeft)
    """
    system_prompt = build_system_prompt(agent="report", task=REPORT_TASK)
    raw = ask_gpt_text(prompt, system_role=system_prompt)

    if not raw:
        return fallback

    text = raw.replace("```json", "").replace("```", "").strip()

    # normale tekst
    if not text.lstrip().startswith("{"):
        return text if len(text) > 5 else fallback

    # defensief JSON
    try:
        parsed = json.loads(text)
        parts = _flatten_text(parsed)

        blacklist = {
            "GO", "NO-GO", "STATUS", "RISICO", "IMPACT",
            "ACTIE", "ONVOLDOENDE DATA", "CONDITIONAL"
        }

        cleaned = [p for p in parts if p.strip() and p.strip().upper() not in blacklist]
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
# Data fetchers (weekly window)
# =====================================================

def get_week_window(days: int = 7) -> Tuple[date, date]:
    end_date = date.today()
    start_date = end_date - timedelta(days=days - 1)
    return start_date, end_date


def fetch_daily_reports_window(user_id: int, start: date, end: date) -> List[Dict[str, Any]]:
    """
    Haal de daily_reports van een periode op.
    Verwacht nieuwe daily columns (2.0).
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    report_date,
                    executive_summary,
                    market_analysis,
                    macro_context,
                    technical_analysis,
                    setup_validation,
                    strategy_implication,
                    bot_strategy,
                    bot_snapshot,
                    outlook,
                    macro_score,
                    technical_score,
                    market_score,
                    setup_score
                FROM daily_reports
                WHERE user_id = %s
                  AND report_date BETWEEN %s AND %s
                ORDER BY report_date ASC;
            """, (user_id, start, end))
            rows = cur.fetchall()

        out = []
        for r in rows:
            out.append({
                "report_date": r[0].isoformat() if r[0] else None,
                "executive_summary": r[1],
                "market_analysis": r[2],
                "macro_context": r[3],
                "technical_analysis": r[4],
                "setup_validation": r[5],
                "strategy_implication": r[6],
                "bot_strategy": r[7],
                "bot_snapshot": safe_json(r[8]),
                "outlook": r[9],
                "macro_score": to_float(r[10]),
                "technical_score": to_float(r[11]),
                "market_score": to_float(r[12]),
                "setup_score": to_float(r[13]),
            })
        return out
    finally:
        conn.close()


def fetch_daily_scores_window(user_id: int, start: date, end: date) -> List[Dict[str, Any]]:
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT report_date, macro_score, technical_score, market_score, setup_score
                FROM daily_scores
                WHERE user_id = %s
                  AND report_date BETWEEN %s AND %s
                ORDER BY report_date ASC;
            """, (user_id, start, end))
            rows = cur.fetchall()

        return [{
            "report_date": r[0].isoformat() if r[0] else None,
            "macro_score": to_float(r[1]),
            "technical_score": to_float(r[2]),
            "market_score": to_float(r[3]),
            "setup_score": to_float(r[4]),
        } for r in rows]
    finally:
        conn.close()


def fetch_setup_scores_window(user_id: int, start: date, end: date) -> List[Dict[str, Any]]:
    """
    Dagelijkse setup scores over de periode.
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    d.report_date,
                    s.id,
                    s.name,
                    s.timeframe,
                    d.score,
                    d.is_best
                FROM daily_setup_scores d
                JOIN setups s ON s.id = d.setup_id
                WHERE d.user_id = %s
                  AND d.report_date BETWEEN %s AND %s
                ORDER BY d.report_date ASC, d.score DESC;
            """, (user_id, start, end))
            rows = cur.fetchall()

        return [{
            "report_date": r[0].isoformat() if r[0] else None,
            "setup_id": r[1],
            "name": r[2],
            "timeframe": r[3],
            "score": to_float(r[4]),
            "is_best": bool(r[5]) if r[5] is not None else False,
        } for r in rows]
    finally:
        conn.close()


def fetch_bot_decisions_window(user_id: int, start: date, end: date) -> List[Dict[str, Any]]:
    """
    Bot beslissingen over de periode.
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    d.decision_date,
                    b.name AS bot_name,
                    d.action,
                    d.confidence,
                    d.amount_eur,
                    d.scores_json,
                    d.reason_json
                FROM bot_decisions d
                JOIN bot_configs b ON b.id = d.bot_id
                WHERE d.user_id = %s
                  AND d.decision_date BETWEEN %s AND %s
                ORDER BY d.decision_date ASC, d.updated_at DESC;
            """, (user_id, start, end))
            rows = cur.fetchall()

        out = []
        for r in rows:
            scores_json = safe_json(r[5])
            reason_json = safe_json(r[6])

            # setup_match normaliseren -> string
            setup_match = None
            if isinstance(scores_json, dict):
                raw = scores_json.get("setup_match")
                if isinstance(raw, dict):
                    setup_match = raw.get("name") or raw.get("label") or raw.get("id")
                elif isinstance(raw, list):
                    setup_match = ", ".join(str(x.get("name") if isinstance(x, dict) and x.get("name") else x) for x in raw)
                elif isinstance(raw, (str, int, float)):
                    setup_match = str(raw)

            # reason normaliseren -> string
            reason_text = None
            if isinstance(reason_json, str):
                reason_text = reason_json
            elif isinstance(reason_json, list):
                reason_text = "; ".join(str(x) for x in reason_json if str(x).strip())
            elif isinstance(reason_json, dict):
                if "reason" in reason_json:
                    reason_text = str(reason_json["reason"])
                elif "reasons" in reason_json and isinstance(reason_json["reasons"], list):
                    reason_text = "; ".join(str(x) for x in reason_json["reasons"] if str(x).strip())
                else:
                    reason_text = str(reason_json)

            out.append({
                "date": r[0].isoformat() if r[0] else None,
                "bot_name": r[1] or "Bot",
                "action": (r[2] or "hold").lower(),
                "confidence": r[3],
                "amount_eur": to_float(r[4]),
                "setup_match": setup_match,
                "reason": reason_text,
            })
        return out
    finally:
        conn.close()


def _avg(vals: List[Optional[float]]) -> Optional[float]:
    nums = [v for v in vals if isinstance(v, (int, float))]
    if not nums:
        return None
    return round(sum(nums) / len(nums), 2)


def _trend(vals: List[Optional[float]]) -> str:
    nums = [v for v in vals if isinstance(v, (int, float))]
    if len(nums) < 2:
        return "flat"
    if nums[-1] > nums[0] + 2:
        return "up"
    if nums[-1] < nums[0] - 2:
        return "down"
    return "flat"


def _bot_stats(bot_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    counts = {"buy": 0, "sell": 0, "hold": 0, "other": 0}
    amount_total = 0.0
    amount_count = 0

    for r in bot_rows:
        a = (r.get("action") or "hold").lower()
        if a in counts:
            counts[a] += 1
        else:
            counts["other"] += 1

        if isinstance(r.get("amount_eur"), (int, float)):
            amount_total += float(r["amount_eur"])
            amount_count += 1

    return {
        "counts": counts,
        "amount_total": round(amount_total, 2) if amount_count else 0.0,
        "days_with_decision": len({r.get("date") for r in bot_rows if r.get("date")}),
    }


def _top_setups(setup_rows: List[Dict[str, Any]], limit: int = 5) -> List[Dict[str, Any]]:
    """
    Neem de meest voorkomende 'best setups' in de periode.
    """
    best_only = [r for r in setup_rows if r.get("is_best")]
    freq: Dict[str, Dict[str, Any]] = {}
    for r in best_only:
        key = f"{r.get('name')}|{r.get('timeframe')}"
        if key not in freq:
            freq[key] = {
                "name": r.get("name"),
                "timeframe": r.get("timeframe"),
                "wins": 0,
                "avg_score": [],
            }
        freq[key]["wins"] += 1
        if isinstance(r.get("score"), (int, float)):
            freq[key]["avg_score"].append(float(r["score"]))

    items = []
    for v in freq.values():
        items.append({
            "name": v["name"],
            "timeframe": v["timeframe"],
            "wins": v["wins"],
            "avg_score": _avg(v["avg_score"]),
        })

    items.sort(key=lambda x: (x["wins"], x["avg_score"] or 0), reverse=True)
    return items[:limit]


# =====================================================
# Prompts (weekly)
# =====================================================

def p_exec(meta: Dict[str, Any]) -> str:
    return f"""
Schrijf een executive summary voor de week {meta.get("period_start")} t/m {meta.get("period_end")}.

Focus:
- het dominante marktthema van de week
- hoe de scoremix (macro/market/technical/setup) verschoof
- of de week uitnodigde tot actie of juist terughoudendheid

Blijf compact, beslissingsgericht en zonder prijsniveaus.
"""


def p_market(meta: Dict[str, Any]) -> str:
    return f"""
Schrijf een market_overview over de week.

Gebruik:
- score trends: {json.dumps(meta.get("score_trends"), ensure_ascii=False)}

Doel:
- beschrijf het regime (trend vs range, rust vs volatiliteit, overtuiging vs twijfel)
- benoem verschuivingen door de week heen
- koppel aan handelbaarheid en betrouwbaarheid van signalen
"""


def p_macro(meta: Dict[str, Any], daily_reports: List[Dict[str, Any]]) -> str:
    return f"""
Schrijf macro_trends voor de week.

Je mag uitsluitend steunen op:
- macro score trend: {meta.get("score_trends", {}).get("macro")}
- inhoud uit daily macro_context velden (samengevat)

Doel:
- beschrijf welke macro-invloeden de toon zetten
- benoem of de rugwind/tegenwind toenam of afnam
- link dit aan risk-on/risk-off gedrag, zonder basics uit te leggen
"""


def p_technical(meta: Dict[str, Any]) -> str:
    return f"""
Schrijf technical_structure voor de week.

Gebruik:
- technical trend: {meta.get("score_trends", {}).get("technical")}
- market trend: {meta.get("score_trends", {}).get("market")}

Doel:
- beschrijf de technische structuur als geheel (momentum, trendkwaliteit, betrouwbaarheid)
- benoem of signalen consistenter werden of juist rommeliger
- koppel aan timing (wachten vs meedoen), zonder prijsniveaus
"""


def p_setups(meta: Dict[str, Any]) -> str:
    return f"""
Schrijf setup_performance voor de week.

Gebruik:
- top setups: {json.dumps(meta.get("top_setups"), ensure_ascii=False)}

Doel:
- welke setup-types het beste aansloten op de marktconditie
- of ‘best setups’ consistent waren of steeds wisselden
- wat dat zegt over selectiviteit en filtering
"""


def p_bot(meta: Dict[str, Any]) -> str:
    return f"""
Schrijf bot_performance voor de week.

Gebruik:
- bot stats: {json.dumps(meta.get("bot_stats"), ensure_ascii=False)}

Doel:
- duid of de bot actief of terughoudend was, en waarom dat logisch was in deze weekcontext
- benadruk discipline (geen trade is ook een keuze)
- geen herhaling van exacte bedragen of losse dagregels, wel evaluatie van gedrag
"""


def p_lessons(meta: Dict[str, Any], daily_reports: List[Dict[str, Any]]) -> str:
    return f"""
Schrijf strategic_lessons voor de week.

Doel:
- 2–3 strategische lessen op basis van patronen (niet op basis van één dag)
- wat werkte, wat niet werkte, en wat dat vraagt van selectiviteit/risk management
- koppel aan de scoremix en regime

Geen lijstjes; één doorlopend stuk tekst.
"""


def p_outlook(meta: Dict[str, Any]) -> str:
    return f"""
Schrijf outlook voor de komende week.

Gebruik:
- score trends en regime: {json.dumps(meta.get("score_trends"), ensure_ascii=False)}

Doel:
- schets 2 scenario’s (voortzetting vs omslag/vertraging) zonder prijsniveaus
- benoem welke bevestiging je wil zien in scores/structuur
- eindig met een professioneel handelsoordeel (actief vs geduldig)
"""


# =====================================================
# MAIN
# =====================================================

def generate_weekly_report_sections(user_id: int) -> Dict[str, Any]:
    """
    Weekly report agent (canonical)
    Output keys:
    - executive_summary
    - market_overview
    - macro_trends
    - technical_structure
    - setup_performance
    - bot_performance
    - strategic_lessons
    - outlook
    - meta_json (extra)
    """
    start, end = get_week_window(days=7)

    daily_reports = fetch_daily_reports_window(user_id, start, end)
    daily_scores = fetch_daily_scores_window(user_id, start, end)
    setup_scores = fetch_setup_scores_window(user_id, start, end)
    bot_rows = fetch_bot_decisions_window(user_id, start, end)

    # if no data: minimal safe output
    if not daily_reports and not daily_scores:
        logger.warning("⚠️ Weekly agent: geen daily data gevonden (user=%s)", user_id)
        return {
            "executive_summary": "Er is onvoldoende weekdata beschikbaar om een betrouwbaar weekbeeld te vormen.",
            "market_overview": "De dataset voor deze periode is te beperkt om het markregime goed te duiden.",
            "macro_trends": "Er ontbreken consistente macro-observaties voor deze week.",
            "technical_structure": "Er ontbreken voldoende technische signalen over meerdere dagen.",
            "setup_performance": "Er is te weinig setup-performance data om conclusies te trekken.",
            "bot_performance": "Er is geen bruikbare bot-activiteit over de week gevonden.",
            "strategic_lessons": "Zonder complete weekdata is het niet verstandig om strategische lessen te formuleren.",
            "outlook": "Zodra de weekdata compleet is, kan een scenario-vooruitblik worden gemaakt.",
            "meta_json": {
                "period_start": start.isoformat(),
                "period_end": end.isoformat(),
                "user_id": user_id,
                "status": "no_data",
            },
        }

    macro_vals = [r.get("macro_score") for r in daily_scores] if daily_scores else [r.get("macro_score") for r in daily_reports]
    tech_vals = [r.get("technical_score") for r in daily_scores] if daily_scores else [r.get("technical_score") for r in daily_reports]
    market_vals = [r.get("market_score") for r in daily_scores] if daily_scores else [r.get("market_score") for r in daily_reports]
    setup_vals = [r.get("setup_score") for r in daily_scores] if daily_scores else [r.get("setup_score") for r in daily_reports]

    meta = {
        "user_id": user_id,
        "period_start": start.isoformat(),
        "period_end": end.isoformat(),
        "days_included": [r.get("report_date") for r in daily_reports if r.get("report_date")],
        "score_avgs": {
            "macro": _avg(macro_vals),
            "technical": _avg(tech_vals),
            "market": _avg(market_vals),
            "setup": _avg(setup_vals),
        },
        "score_trends": {
            "macro": _trend(macro_vals),
            "technical": _trend(tech_vals),
            "market": _trend(market_vals),
            "setup": _trend(setup_vals),
        },
        "top_setups": _top_setups(setup_scores, limit=5),
        "bot_stats": _bot_stats(bot_rows),
    }

    # compact daily extracts for context blob (no giant walls)
    daily_extract = []
    for r in daily_reports:
        daily_extract.append({
            "date": r.get("report_date"),
            "exec": r.get("executive_summary"),
            "market": r.get("market_analysis"),
            "macro": r.get("macro_context"),
            "tech": r.get("technical_analysis"),
            "setup": r.get("setup_validation"),
            "bot": r.get("bot_strategy"),
            "outlook": r.get("outlook"),
            "scores": {
                "macro": r.get("macro_score"),
                "technical": r.get("technical_score"),
                "market": r.get("market_score"),
                "setup": r.get("setup_score"),
            }
        })

    context_blob = f"""
Je schrijft een weekrapport voor {start.isoformat()} t/m {end.isoformat()}.
Gebruik UITSLUITEND onderstaande data.

=== WEEK META ===
{json.dumps(meta, ensure_ascii=False)}

=== DAG EXTRACTS (samengevat) ===
{json.dumps(daily_extract, ensure_ascii=False)}

=== BOT BESLISSINGEN (periode) ===
{json.dumps(bot_rows, ensure_ascii=False)}

=== SETUP SCORES (periode) ===
{json.dumps(setup_scores[:200], ensure_ascii=False)}

Belangrijk:
- Geen absolute prijsniveaus
- Beschrijf trends, regime en verschuivingen over de week
- Geen losse opsommingen of labels
""".strip()

    seen: List[str] = []

    executive_summary = reduce_repetition(
        generate_text(context_blob + "\n\n" + p_exec(meta), "Deze week gaf geen helder regime en vroeg om selectiviteit."),
        seen
    )

    market_overview = reduce_repetition(
        generate_text(context_blob + "\n\n" + p_market(meta), "Het marktbeeld bleef wisselend en vroeg om discipline."),
        seen
    )

    macro_trends = reduce_repetition(
        generate_text(context_blob + "\n\n" + p_macro(meta, daily_reports), "Macro was gemengd en bood geen constante rugwind."),
        seen
    )

    technical_structure = reduce_repetition(
        generate_text(context_blob + "\n\n" + p_technical(meta), "Technisch bleef het beeld fragiel en afhankelijk van bevestiging."),
        seen
    )

    setup_performance = reduce_repetition(
        generate_text(context_blob + "\n\n" + p_setups(meta), "Setups vroegen om extra filtering en timingdiscipline."),
        seen
    )

    bot_performance = reduce_repetition(
        generate_text(context_blob + "\n\n" + p_bot(meta), "De bot hield discipline en wachtte op betere voorwaarden."),
        seen
    )

    strategic_lessons = reduce_repetition(
        generate_text(context_blob + "\n\n" + p_lessons(meta, daily_reports), "De belangrijkste les was selectiviteit: niet elke beweging is handelbaar."),
        seen
    )

    outlook = reduce_repetition(
        generate_text(context_blob + "\n\n" + p_outlook(meta), "Vooruitblik: focus op bevestiging in structuur en scoremix voordat je opschaalt."),
        seen
    )

    result = {
        "executive_summary": executive_summary,
        "market_overview": market_overview,
        "macro_trends": macro_trends,
        "technical_structure": technical_structure,
        "setup_performance": setup_performance,
        "bot_performance": bot_performance,
        "strategic_lessons": strategic_lessons,
        "outlook": outlook,
        "meta_json": meta,
    }

    logger.info("✅ Weekly report agent OK, return keys=%s", list(result.keys()))
    return result
