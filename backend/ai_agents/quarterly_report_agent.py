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
# QUARTERLY REPORT ROLE
# =====================================================
REPORT_TASK = """
Je bent een senior Bitcoin macro & cycle strategist.
Je schrijft een kwartaalrapport voor een ervaren gebruiker.

Context:
- Je krijgt ALLE maandrapporten van het afgelopen kwartaal
- Elk maandrapport is geschreven door dezelfde analist (jij)
- Jij analyseert op cyclus-, allocatie- en risicofaseniveau

Belangrijk:
- Je vat samen, je herhaalt niet
- Je denkt in regimes, fases en overgangen
- Je vergelijkt begin en einde van het kwartaal
- Je beoordeelt robuustheid van aannames
- Je doet GEEN trading- of timing-adviezen
- Je noemt GEEN prijsniveaus

Stijl:
- Strategisch, kalm, overtuigend
- Doorlopend verhaal
- Geen AI-termen
- Geen uitleg van basisbegrippen
- Geen opsommingen of bullets

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
        if not norm or len(norm) < 35:
            continue

        if any(_is_too_similar(norm, prev) for prev in seen):
            continue

        output.append(s)
        seen.append(norm)

    return " ".join(output)

# =====================================================
# DATA — MONTHLY REPORTS VAN HET KWARTAAL
# =====================================================

def get_quarterly_monthly_reports(user_id: int) -> List[Dict[str, Any]]:
    """
    Haalt alle maandrapporten van het afgelopen kwartaal op.
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    report_date,
                    regime,
                    market_structure,
                    macro_context,
                    positioning,
                    lessons_learned,
                    outlook
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
                "regime": r[1],
                "market_structure": r[2],
                "macro_context": r[3],
                "positioning": r[4],
                "lessons_learned": r[5],
                "outlook": r[6],
            })

        return reports

    finally:
        conn.close()

# =====================================================
# PROMPTS — QUARTERLY
# =====================================================

def p_quarterly_cycle():
    return """
Plaats dit kwartaal binnen de grotere Bitcoin-cyclus.

Beschrijf:
- in welke fase het kwartaal zich bevond
- of dit een bevestiging of overgangsfase was
- of structuur werd opgebouwd of afgebroken
"""


def p_quarterly_risk():
    return """
Analyseer het risicoklimaat over dit kwartaal.

Ga in op:
- of risico-opname werd beloond of afgestraft
- of volatiliteit voorspelbaar of verraderlijk was
- hoe asymmetrie zich ontwikkelde
"""


def p_quarterly_allocation():
    return """
Vertaal dit kwartaal naar allocatie-denken.

Beschrijf:
- of dit kwartaal vroeg om exposure of terughoudendheid
- of schaalbaarheid van posities mogelijk was
- hoe belangrijk geduld bleek
"""


def p_quarterly_mistakes():
    return """
Reflecteer op structurele fouten of valkuilen.

Benoem:
- waar aannames te optimistisch of te defensief waren
- waar marktgedrag structureel werd onderschat
"""


def p_quarterly_forward():
    return """
Kijk vooruit naar het volgende kwartaal.

Zonder voorspellingen:
- benoem welke bevestiging nodig is
- benoem welke risico’s latent blijven
- beschrijf welk type marktontwikkeling doorslaggevend zou zijn
"""

# =====================================================
# MAIN BUILDER — QUARTERLY REPORT AGENT
# =====================================================

def generate_quarterly_report_sections(user_id: int) -> Dict[str, Any]:
    """
    Quarterly Report Agent
    - Leest maandrapporten
    - Analyseert cyclus & allocatie
    - Strategisch overzicht
    """

    monthly_reports = get_quarterly_monthly_reports(user_id)

    if not monthly_reports:
        logger.warning("⚠️ Geen monthly reports gevonden voor quarterly report")
        return {
            "summary": "Er zijn onvoldoende maandrapporten beschikbaar om een kwartaalbeeld te vormen."
        }

    # -------------------------------------------------
    # Context blob
    # -------------------------------------------------
    context_blob = f"""
Je schrijft het kwartaalrapport.
Gebruik UITSLUITEND onderstaande maandrapporten.

=== MAANDRAPPORTEN (chronologisch) ===
{json.dumps(monthly_reports, ensure_ascii=False)}

BELANGRIJK:
- Geen nieuwe data introduceren
- Analyseer op cyclus- en allocatieniveau
- Werk op kwartaalhorizon
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
    cycle = gen(p_quarterly_cycle(), "Het kwartaal liet geen duidelijke cyclusverschuiving zien.")
    risk = gen(p_quarterly_risk(), "Het risicoklimaat bleef wisselend.")
    allocation = gen(p_quarterly_allocation(), "Voorzichtige allocatie bleef passend.")
    mistakes = gen(p_quarterly_mistakes(), "Belangrijke valkuilen werden zichtbaar.")
    forward = gen(p_quarterly_forward(), "Bevestiging blijft vereist richting het volgende kwartaal.")

    # -------------------------------------------------
    # RESULT
    # -------------------------------------------------
    result = {
        "cycle_context": cycle,
        "risk_environment": risk,
        "allocation_logic": allocation,
        "lessons_learned": mistakes,
        "forward_view": forward,

        "months_covered": [m["month"] for m in monthly_reports],
        "report_type": "quarterly",
    }

    logger.info("✅ Quarterly report agent OK, months=%s", result["months_covered"])
    return result
