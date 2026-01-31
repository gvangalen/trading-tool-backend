import io
import logging
import unicodedata
import re
from datetime import datetime
from typing import Dict, Any, Optional

from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
from reportlab.lib.units import cm

logger = logging.getLogger(__name__)

# =====================================================
# 🔒 PDF RENDER CONFIG (LOCKED)
# =====================================================

SECTION_ORDER = [
    ("executive_summary", "Executive Summary"),
    ("market_analysis", "Market Analyse"),
    ("macro_context", "Macro Context"),
    ("technical_analysis", "Technische Analyse"),
    ("setup_validation", "Setup Validatie"),
    ("strategy_implication", "Strategie Implicatie"),
    ("bot_decision", "Botbeslissing"),
    ("outlook", "Vooruitblik"),
]

# =====================================================
# 🧹 Helpers
# =====================================================

def _clean_text(text: Optional[str]) -> str:
    if not text:
        return "–"
    if not isinstance(text, str):
        text = str(text)

    # emoji strip
    text = re.sub(r'[\U00010000-\U0010ffff]', '', text)

    try:
        return unicodedata.normalize("NFKD", text).encode(
            "latin-1", "ignore"
        ).decode("latin-1")
    except Exception:
        return re.sub(r"[^\x00-\x7F]+", "", text)


def _fmt_percent(v):
    if v is None:
        return "–"
    try:
        return f"{float(v):.0f}%"
    except Exception:
        return "–"


def _fmt_eur(v):
    if v is None:
        return "–"
    try:
        return f"€{float(v):,.0f}"
    except Exception:
        return "–"


# =====================================================
# 🖨️ MAIN PDF RENDERER
# =====================================================

def generate_report_pdf(
    report: Dict[str, Any],
    report_type: str = "daily",
) -> io.BytesIO:
    """
    🔒 Definitieve PDF renderer
    - Gebruikt bestaande report data
    - Geen AI
    - Geen interpretatie
    - Exacte inhoud
    """

    buffer = io.BytesIO()
    report_date = report.get("report_date") or datetime.utcnow().strftime("%Y-%m-%d")

    logger.info("🖨️ PDF render | type=%s | date=%s", report_type, report_date)

    doc = SimpleDocTemplate(
        buffer,
        pagesize=A4,
        leftMargin=2 * cm,
        rightMargin=2 * cm,
        topMargin=2 * cm,
        bottomMargin=2 * cm,
        title=f"{report_type.capitalize()} Report {report_date}",
    )

    styles = getSampleStyleSheet()

    styles.add(ParagraphStyle(
        name="Header",
        fontSize=14,
        leading=18,
        spaceAfter=12,
        fontName="Helvetica-Bold",
    ))

    styles.add(ParagraphStyle(
        name="SectionTitle",
        fontSize=12,
        leading=16,
        spaceBefore=14,
        spaceAfter=6,
        fontName="Helvetica-Bold",
    ))

    styles.add(ParagraphStyle(
        name="Body",
        fontSize=11,
        leading=16,
        spaceAfter=10,
        fontName="Helvetica",
    ))

    story = []

    # =====================================================
    # 🧾 HEADER
    # =====================================================
    story.append(Paragraph(
        _clean_text(f"{report_type.capitalize()} Trading Report"),
        styles["Header"]
    ))

    story.append(Paragraph(
        _clean_text(f"Datum: {report_date}"),
        styles["Body"]
    ))

    story.append(Spacer(1, 14))

    # =====================================================
    # 📊 SCORES (exact dashboard waarden)
    # =====================================================
    scores_line = (
        f"Macro: {_fmt_percent(report.get('macro_score'))} · "
        f"Technical: {_fmt_percent(report.get('technical_score'))} · "
        f"Market: {_fmt_percent(report.get('market_score'))} · "
        f"Setup: {_fmt_percent(report.get('setup_score'))}"
    )

    story.append(Paragraph(
        _clean_text(scores_line),
        styles["Body"]
    ))

    story.append(Spacer(1, 16))

    # =====================================================
    # 📄 CONTENT SECTIONS (1-op-1 report)
    # =====================================================
    for key, title in SECTION_ORDER:

        # --- Botbeslissing is speciaal (facts + tekst)
        if key == "bot_decision":
            bot_text = report.get("bot_strategy")
            bot_snapshot = report.get("bot_snapshot")

            if not bot_text and not bot_snapshot:
                continue

            story.append(Paragraph(title, styles["SectionTitle"]))

            if bot_snapshot:
                lines = [
                    f"Bot: {bot_snapshot.get('bot_name', '–')}",
                    f"Actie: {bot_snapshot.get('action', '–')}",
                    f"Confidence: {_fmt_percent(bot_snapshot.get('confidence'))}",
                    f"Bedrag: {_fmt_eur(bot_snapshot.get('amount_eur'))}",
                ]

                if bot_snapshot.get("setup_match"):
                    lines.append(f"Setup match: {bot_snapshot['setup_match']}")

                story.append(Paragraph(
                    _clean_text(" · ".join(lines)),
                    styles["Body"]
                ))

            if bot_text:
                story.append(Paragraph(
                    _clean_text(bot_text),
                    styles["Body"]
                ))

            continue

        # --- Normale secties
        value = report.get(key)
        if not value:
            continue

        story.append(Paragraph(title, styles["SectionTitle"]))

        text = _clean_text(value).replace("\n", "<br/>")
        story.append(Paragraph(text, styles["Body"]))

    # =====================================================
    # 🖨️ BUILD
    # =====================================================
    doc.build(story)
    buffer.seek(0)

    logger.info("✅ PDF render gereed")
    return buffer
