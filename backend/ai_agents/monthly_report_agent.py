import logging
import json
import re
from difflib import SequenceMatcher
from typing import Dict, Any, List
from datetime import date, timedelta

from backend.utils.db import get_db_connection
from backend.utils.openai_client import ask_gpt_text
from backend.ai_core.system_prompt_builder import build_system_prompt

# =====================================================
# Logging
# =====================================================
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# =====================================================
# 🧠 MONTHLY REPORT ROLE (CANONICAL)
# =====================================================
REPORT_TASK = """
Je bent een senior Bitcoin market strategist.
Je schrijft een MAANDELIJKS rapport voor een ervaren gebruiker.

Context:
- Je krijgt alle weekrapporten van de afgelopen maand
- Elk weekrapport is door dezelfde analist geschreven
- Jij analyseert op regime-, trend- en positioneringsniveau

Belangrijk:
- Je vat samen, je herhaalt niet
- Je benoemt structurele trends en verschuivingen
- Je vergelijkt begin en einde van de maand
- Je beoordeelt betrouwbaarheid van signalen
- Je doet GEEN trades, entries of timing-adviezen
- Je noemt GEEN exacte prijsniveaus

Stijl:
- Strategisch, beheerst, professioneel
- Doorlopend verhaal
- Geen AI-termen
- Geen uitleg van basisbegrippen
- Geen lijstjes of opsommingen

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
        if not norm or len(norm) < 30:
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
# DATA — WEEKLY REPORTS (LAATSTE MAAND)
# =====================================================

def fetch_weekly_reports_for_month(user_id: int) -> List[Dict[str, Any]]:
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
                FROM weekly_reports
                WHERE user_id = %s
                  AND report_date >= CURRENT_DATE - INTERVAL '31 days'
                ORDER BY report_date ASC;
            """, (user_id,))
            rows = cur.fetchall()

        reports = []
        for r in rows:
            reports.append({
                "week": r[0].isoformat(),
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
# PROMPTS — MONTHLY
# =====================================================

def p_exec():
    return """
Schrijf een executive summary voor de afgelopen maand.

Focus:
- het dominante marktregime
- de belangrijkste verschuiving t.o.v. begin van de maand
- of de maand uitnodigde tot actie of juist tot terughoudendheid
"""


def p_market():
    return """
Schrijf market_overview voor de maand.

Beschrijf:
- het overkoepelende marktregime (trend, range, overgang)
- of volatiliteit en richting betrouwbaarder werden
- hoe consistent het marktgedrag was
"""


def p_macro():
    return """
Schrijf macro_trends voor de maand.

Ga in op:
- of macro structureel steunend of remmend was
- of er een verschuiving zichtbaar was in risk-on / risk-off
- hoe dominant macro was t.o.v. andere factoren
"""


def p_technical():
    return """
Schrijf technical_structure voor de maand.

Analyseer:
- of technische signalen betrouwbaarder werden
- of momentum werd opgebouwd of juist afgebroken
- hoe dit de handelbaarheid beïnvloedde
"""


def p_setups():
    return """
Schrijf setup_performance voor de maand.

Benoem:
- of setups consistent werkten of vaak wisselden
- of filtering belangrijker werd
- wat dit zegt over marktfase en selectiviteit
"""


def p_bot():
    return """
Schrijf bot_performance voor de maand.

Analyseer:
- of de bot vooral actief of terughoudend was
- of dit gedrag logisch was binnen het maandregime
- hoe discipline en drempels hebben bijgedragen
"""


def p_lessons():
    return """
Schrijf strategic_lessons voor de maand.

Formuleer:
- de belangrijkste structurele lessen
- wat deze maand duidelijk maakte over risico, timing en geduld
- welke inzichten ook in toekomstige maanden relevant blijven
"""


def p_outlook():
    return """
Schrijf outlook voor de komende maand.

Zonder voorspellingen:
- benoem welke bevestiging nodig is
- benoem waar voorzichtigheid blijft
- schets welk type marktgedrag doorslaggevend wordt
"""


# =====================================================
# MAIN BUILDER — MONTHLY REPORT AGENT
# =====================================================

def generate_monthly_report_sections(user_id: int) -> Dict[str, Any]:
    weekly_reports = fetch_weekly_reports_for_month(user_id)

    if not weekly_reports:
        logger.warning("⚠️ Geen weekly reports gevonden voor monthly report")
        return {
            "executive_summary": "Er is onvoldoende weekdata beschikbaar om een betrouwbaar maandbeeld te vormen.",
            "market_overview": "Het marktregime kon deze maand niet eenduidig worden vastgesteld.",
            "macro_trends": "Macro-invloeden waren onvoldoende consistent om structurele conclusies te trekken.",
            "technical_structure": "De technische structuur bood te weinig continuïteit.",
            "setup_performance": "Er is onvoldoende setup-data voor een maandanalyse.",
            "bot_performance": "Bot-activiteit was te beperkt om te evalueren.",
            "strategic_lessons": "Zonder volledige maanddata zijn strategische lessen beperkt.",
            "outlook": "Zodra meer data beschikbaar is kan een vooruitblik worden gemaakt.",
            "meta_json": {
                "user_id": user_id,
                "status": "no_data",
            },
        }

    seen: List[str] = []

    context_blob = f"""
Je schrijft het maandrapport.
Gebruik UITSLUITEND onderstaande weekrapporten.

=== WEEKRAPPORTEN (chronologisch) ===
{json.dumps(weekly_reports, ensure_ascii=False)}

BELANGRIJK:
- Analyseer op maandniveau
- Vat samen, herhaal niet
- Geen nieuwe data introduceren
""".strip()

    executive_summary = generate_text(context_blob + "\n\n" + p_exec(),
                                      "De maand kende geen eenduidig marktbeeld.", seen)

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
                                      "De maand onderstreepte het belang van geduld.", seen)

    outlook = generate_text(context_blob + "\n\n" + p_outlook(),
                            "Vooruitblik: focus op bevestiging voordat exposure toeneemt.", seen)

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
            "weeks_covered": [w["week"] for w in weekly_reports],
        },
    }

    logger.info("✅ Monthly report agent OK (weeks=%s)", result["meta_json"]["weeks_covered"])
    return result
