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
# MONTHLY REPORT ROLE
# =====================================================
REPORT_TASK = """
Je bent een senior Bitcoin market strategist.
Je schrijft een maandelijks rapport voor een ervaren gebruiker.

Context:
- Je krijgt ALLE weekrapporten van de afgelopen maand
- Elk weekrapport is geschreven door dezelfde analist (jij)
- Jij analyseert op regime-, cyclus- en positioneringsniveau

Belangrijk:
- Je vat samen, je herhaalt niet
- Je benoemt structurele trends en verschuivingen
- Je vergelijkt begin en eind van de maand
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
- ALLEEN doorlopende tekst
- GEEN JSON
- GEEN markdown
- GEEN labels
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

# =====================================================
# DATA — WEEKLY REPORTS VAN DE MAAND
# =====================================================

def get_monthly_weekly_reports(user_id: int) -> List[Dict[str, Any]]:
    """
    Haalt alle weekly reports van de afgelopen maand op.
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    report_date,
                    executive_summary,
                    market_structure,
                    macro_context,
                    setup_evaluation,
                    positioning,
                    outlook
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
                "market_structure": r[2],
                "macro_context": r[3],
                "setup_evaluation": r[4],
                "positioning": r[5],
                "outlook": r[6],
            })

        return reports

    finally:
        conn.close()

# =====================================================
# PROMPTS — MONTHLY
# =====================================================

def p_monthly_regime():
    return """
Beschrijf het marktregime van deze maand.

Ga in op:
- of de maand werd gekenmerkt door trend, range of overgang
- of signalen consistenter of juist instabieler werden
- hoe het sentiment zich structureel ontwikkelde
"""


def p_monthly_structure():
    return """
Analyseer de structurele ontwikkeling door de maand heen.

Leg uit:
- of momentum werd opgebouwd of juist afgebroken
- of technische betrouwbaarheid toenam of afnam
- of bewegingen bevestigd of verworpen werden
"""


def p_monthly_macro():
    return """
Evalueer de macro-omgeving over de maand.

Beantwoord:
- of macro-invloeden structureel steunend of remmend waren
- of er een duidelijke verschuiving in risico-omgeving zichtbaar was
- hoe dominant deze factor was
"""


def p_monthly_positioning():
    return """
Vertaal deze maand naar positioneringslogica.

Beschrijf:
- of deze maand vroeg om agressie of terughoudendheid
- hoe discipline en timing hebben bijgedragen
- of fouten vooral kwamen door te vroeg of te laat handelen
"""


def p_monthly_lessons():
    return """
Reflecteer op deze maand.

Benoem:
- welke aanpak goed werkte
- waar de markt verraste
- welke lessen structureel relevant blijven
"""


def p_monthly_outlook():
    return """
Kijk vooruit naar de komende maand.

Zonder voorspellingen:
- benoem welke bevestiging nodig is
- benoem waar voorzichtigheid blijft
- schets welk type marktontwikkeling doorslaggevend zou zijn
"""

# =====================================================
# MAIN BUILDER — MONTHLY REPORT AGENT
# =====================================================

def generate_monthly_report_sections(user_id: int) -> Dict[str, Any]:
    """
    Monthly Report Agent
    - Leest weekly reports
    - Analyseert structuur & regime
    - Geeft strategische reflectie
    """

    weekly_reports = get_monthly_weekly_reports(user_id)

    if not weekly_reports:
        logger.warning("⚠️ Geen weekly reports gevonden voor monthly report")
        return {
            "summary": "Er zijn onvoldoende weekrapporten beschikbaar om een maandbeeld te vormen."
        }

    # -------------------------------------------------
    # Context blob
    # -------------------------------------------------
    context_blob = f"""
Je schrijft het maandrapport.
Gebruik UITSLUITEND onderstaande weekrapporten.

=== WEEKRAPPORTEN (chronologisch) ===
{json.dumps(weekly_reports, ensure_ascii=False)}

BELANGRIJK:
- Geen nieuwe data introduceren
- Analyseer op regime- en structuurniveau
- Werk op maandhorizon
""".strip()

    seen: List[str] = []

    def gen(prompt, fallback):
        system_prompt = build_system_prompt(agent="report", task=REPORT_TASK)
        raw = ask_gpt_text(context_blob + "\n\n" + prompt, system_role=system_prompt)
        if not raw or len(raw.strip()) < 10:
            return fallback
        return reduce_repetition(raw.strip(), seen)

    # -------------------------------------------------
    # Tekst genereren
    # -------------------------------------------------
    regime = gen(p_monthly_regime(), "De maand kende geen eenduidig marktregime.")
    structure = gen(p_monthly_structure(), "De marktstructuur bleef wisselend.")
    macro = gen(p_monthly_macro(), "Macro-invloeden waren niet dominant.")
    positioning = gen(p_monthly_positioning(), "Selectieve positionering bleef passend.")
    lessons = gen(p_monthly_lessons(), "De maand onderstreepte het belang van discipline.")
    outlook = gen(p_monthly_outlook(), "Bevestiging blijft vereist richting de komende maand.")

    # -------------------------------------------------
    # RESULT
    # -------------------------------------------------
    result = {
        "regime": regime,
        "market_structure": structure,
        "macro_context": macro,
        "positioning": positioning,
        "lessons_learned": lessons,
        "outlook": outlook,

        "weeks_covered": [w["week"] for w in weekly_reports],
        "report_type": "monthly",
    }

    logger.info("✅ Monthly report agent OK, weeks=%s", result["weeks_covered"])
    return result
