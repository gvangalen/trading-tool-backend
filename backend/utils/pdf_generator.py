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
# 🔒 CANONICAL SECTION ORDER (ALLE REPORT TYPES)
# =====================================================

SECTION_ORDER = [
    ("executive_summary", "Executive Summary"),
    ("market_overview", "Market Overview"),
    ("macro_trends", "Macro Trends"),
    ("technical_structure", "Technische Structuur"),
    ("setup_performance", "Setup Performance"),
    ("bot_performance", "Bot Performance"),
    ("strategic_lessons", "Strategische Lessen"),
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
        return (
            unicodedata.normalize("NFKD", text)
            .encode("latin-1", "ignore")
            .decode("latin-1")
        )
    except Exception:
        return re.sub(r"[^\x00-\x7F]+", "", text)


def _fmt_percent(v) -> str:
    if v is None:
        return "–"
    try:
        return f"{float(v):.0f}%"
    except Exception:
        return "–"


def _as_text(value) -> str:
    """
    Velden kunnen string / dict / list / jsonb zijn.
    PDF wil altijd leesbare tekst.
    """
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, (dict, list)):
        try:
            import json
            return json.dumps(value, ensure_ascii=False, indent=2)
        except Exception:
            return str(value)
    return str(value)

# =====================================================
# 🖨️ MAIN PDF RENDERER
# =====================================================

def generate_pdf_report(
    report: Dict[str, Any],
    report_type: str = "daily",
    save_to_disk: bool = False,  # compat met bestaande calls
) -> io.BytesIO:
    """
    Definitieve PDF renderer

    PRINCIPES:
    - Geen AI
    - Geen interpretatie
    - Geen logica per report-type
    - Rendert exact wat de agent heeft opgeslagen
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
    # HEADER
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
    # SCORES (optioneel)
    # =====================================================
    scores_line = (
        f"Macro: {_fmt_percent(report.get('macro_score'))} · "
        f"Technical: {_fmt_percent(report.get('technical_score'))} · "
        f"Market: {_fmt_percent(report.get('market_score'))} · "
        f"Setup: {_fmt_percent(report.get('setup_score'))}"
    )

    story.append(Paragraph(_clean_text(scores_line), styles["Body"]))
    story.append(Spacer(1, 16))

    # =====================================================
    # CONTENT (CANONICAL SECTIONS)
    # =====================================================
    for key, title in SECTION_ORDER:
        value = _as_text(report.get(key))
        if not value:
            continue

        story.append(Paragraph(_clean_text(title), styles["SectionTitle"]))
        story.append(
            Paragraph(
                _clean_text(value).replace("\n", "<br/>"),
                styles["Body"]
            )
        )

    # =====================================================
    # BUILD
    # =====================================================
    doc.build(story)
    buffer.seek(0)

    logger.info("✅ PDF render gereed")
    return buffer

