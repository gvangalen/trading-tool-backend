import logging
import json
import re
from difflib import SequenceMatcher
from typing import Dict, Any, List

from backend.utils.db import get_db_connection
from backend.utils.openai_client import ask_gpt_text
from backend.ai_core.system_prompt_builder import build_system_prompt

# =====================================================
# Logging
# =====================================================
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# =====================================================
# 🧠 QUARTERLY REPORT ROLE (CANONICAL)
# =====================================================
REPORT_TASK = """
Je bent een senior Bitcoin macro & cycle strategist.
Je schrijft een KWARTAALRAPPORT voor een ervaren gebruiker.

Context:
- Je krijgt ALLE maandrapporten van het afgelopen kwartaal
- Elk maandrapport is door dezelfde analist geschreven
- Jij analyseert op cyclus-, regime- en risiconiveau

Belangrijk:
- Je vat samen, je herhaalt niet
- Je denkt in fases, overgangen en robuustheid
- Je vergelijkt begin en einde van het kwartaal
- Je beoordeelt consistentie van signalen
- Je doet GEEN trading-, entry- of timing-adviezen
- Je noemt GEEN prijsniveaus

Stijl:
- Strategisch, kalm, overtuigend
- Doorlopend verhaal
- Geen AI-termen
- Geen uitleg van basisbegrippen
- Geen opsommingen of bullets

Output:
- ALLEEN doorlopende tekst per sectie
- GEEN JSON
- GEEN markdown
- GEEN labels in de tekst
"""

# =====================================================
# Helpers
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
    output = []

    for s in sentences:
        norm = _normalize_sentence(s)
        if not norm or len(norm) < 40:
            continue
        if any(_is_too_similar(norm, prev) for prev in seen):
            continue
        output.append(s)
        seen.append(norm)

    return " ".join(output)


def generate_text(prompt: str, fallback: str, seen: list[str]) -> str:
    system_prompt = build_system_prompt(agent="report", task=REPORT_TASK)
    raw = ask_gpt_text(prompt, system_role=system_prompt)

    if not raw or len(raw.strip()) < 10:
        return fallback

    text = raw.replace("```", "").strip()
    return reduce_repetition(text, seen)


# =====================================================
# DATA — MONTHLY REPORTS (LAATSTE KWARTAAL)
# =====================================================

def fetch_monthly_reports_for_quarter(user_id: int) -> List[Dict[str, Any]]:
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    report_date,
                    executive_summary,
                    market_overview,
                    macro_trends,
                    technical_structure,
                    setup_performance,
                    bot_performance,
                    strategic_lessons,
                    outlook,
                    meta_json
                FROM monthly_reports
                WHERE user_id = %s
                  AND report_date >= CURRENT_DATE - INTERVAL '93 days'
                ORDER BY report_date ASC;
            """, (user_id,))
            rows = cur.fetchall()

        reports = []
        for r in rows:
            reports.append({
                "month": r[0].isoformat(),
                "executive_summary": r[1],
                "market_overview": r[2],
                "macro_trends": r[3],
                "technical_structure": r[4],
                "setup_performance": r[5],
                "bot_performance": r[6],
                "strategic_lessons": r[7],
                "outlook": r[8],
                "meta": r[9],
            })

        return reports
    finally:
        conn.close()


# =====================================================
# PROMPTS — QUARTERLY
# =====================================================

def p_exec():
    return """
Schrijf een executive summary voor het afgelopen kwartaal.

Focus:
- het dominante marktregime
- de belangrijkste structurele verschuiving
- of het kwartaal vroeg om geduld of juist exposure
"""


def p_market():
    return """
Schrijf market_overview voor het kwartaal.

Analyseer:
- of het kwartaal trendmatig, range-gebonden of transitioneel was
- of marktgedrag consistenter werd
- hoe robuust richting en volatiliteit waren
"""


def p_macro():
    return """
Schrijf macro_trends voor het kwartaal.

Ga in op:
- of macro structureel steunend of remmend was
- of risico-omgeving verschoof
- hoe dominant macro was t.o.v. andere factoren
"""


def p_technical():
    return """
Schrijf technical_structure voor het kwartaal.

Beschrijf:
- of technische signalen betrouwbaarder werden
- of momentum structureel werd opgebouwd of afgebroken
- hoe dit de handelbaarheid beïnvloedde
"""


def p_setups():
    return """
Schrijf setup_performance voor het kwartaal.

Benoem:
- of setups consistent werkten
- of filtering en selectiviteit doorslaggevend waren
- wat dit zegt over marktfase
"""


def p_bot():
    return """
Schrijf bot_performance voor het kwartaal.

Analyseer:
- of de bot vooral actief of terughoudend was
- of dit gedrag logisch was binnen het kwartaalregime
- hoe discipline en risicodrempels bijdroegen
"""


def p_lessons():
    return """
Schrijf strategic_lessons voor het kwartaal.

Formuleer:
- de belangrijkste structurele lessen
- waar aannames standhielden of faalden
- welke inzichten ook in volgende kwartalen relevant blijven
"""


def p_outlook():
    return """
Schrijf outlook voor het volgende kwartaal.

Zonder voorspellingen:
- benoem welke bevestiging nodig is
- benoem welke risico’s latent blijven
- schets welk type marktgedrag doorslaggevend wordt
"""


# =====================================================
# MAIN BUILDER — QUARTERLY REPORT AGENT
# =====================================================

def generate_quarterly_report_sections(user_id: int) -> Dict[str, Any]:
    monthly_reports = fetch_monthly_reports_for_quarter(user_id)

    if not monthly_reports:
        logger.warning("⚠️ Geen monthly reports gevonden voor quarterly report")
        return {
            "executive_summary": "Er is onvoldoende maanddata beschikbaar om een kwartaalbeeld te vormen.",
            "market_overview": "Het marktregime kon dit kwartaal niet betrouwbaar worden vastgesteld.",
            "macro_trends": "Macro-invloeden waren onvoldoende consistent.",
            "technical_structure": "De technische structuur bood te weinig continuïteit.",
            "setup_performance": "Er is onvoldoende setup-data voor een kwartaalanalyse.",
            "bot_performance": "Bot-activiteit was te beperkt om te evalueren.",
            "strategic_lessons": "Zonder volledige kwartaaldata blijven strategische lessen beperkt.",
            "outlook": "Zodra meer data beschikbaar is kan een vooruitblik worden gemaakt.",
            "meta_json": {
                "user_id": user_id,
                "status": "no_data",
            },
        }

    seen: List[str] = []

    context_blob = f"""
Je schrijft het kwartaalrapport.
Gebruik UITSLUITEND onderstaande maandrapporten.

=== MAANDRAPPORTEN (chronologisch) ===
{json.dumps(monthly_reports, ensure_ascii=False)}

BELANGRIJK:
- Analyseer op kwartaalniveau
- Vat samen, herhaal niet
- Geen nieuwe data introduceren
""".strip()

    executive_summary = generate_text(context_blob + "\n\n" + p_exec(),
                                      "Het kwartaal kende geen eenduidig marktbeeld.", seen)

    market_overview = generate_text(context_blob + "\n\n" + p_market(),
                                    "Het marktregime bleef wisselend.", seen)

    macro_trends = generate_text(context_blob + "\n\n" + p_macro(),
                                 "Macro-invloeden waren gemengd.", seen)

    technical_structure = generate_text(context_blob + "\n\n" + p_technical(),
                                        "De technische structuur bleef fragiel.", seen)

    setup_performance = generate_text(context_blob + "\n\n" + p_setups(),
                                      "Setups vroegen om verhoogde selectiviteit.", seen)

    bot_performance = generate_text(context_blob + "\n\n" + p_bot(),
                                    "De bot handelde vooral disciplinair.", seen)

    strategic_lessons = generate_text(context_blob + "\n\n" + p_lessons(),
                                      "Het kwartaal onderstreepte het belang van robuuste aannames.", seen)

    outlook = generate_text(context_blob + "\n\n" + p_outlook(),
                            "Vooruitblik: focus op bevestiging en risicobeheersing.", seen)

    result = {
        "executive_summary": executive_summary,
        "market_overview": market_overview,
        "macro_trends": macro_trends,
        "technical_structure": technical_structure,
        "setup_performance": setup_performance,
        "bot_performance": bot_performance,
        "strategic_lessons": strategic_lessons,
        "outlook": outlook,
        "meta_json": {
            "user_id": user_id,
            "months_covered": [m["month"] for m in monthly_reports],
        },
    }

    logger.info("✅ Quarterly report agent OK (months=%s)", result["meta_json"]["months_covered"])
    return result
