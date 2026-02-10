import io
import logging
import unicodedata
import re
from datetime import datetime
from typing import Dict, Any, Optional, List

from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import (
    SimpleDocTemplate,
    Paragraph,
    Spacer,
    Table,
    TableStyle,
)
from reportlab.lib.units import cm
from reportlab.lib import colors

logger = logging.getLogger(__name__)

# =====================================================
# CANONICAL SECTIONS
# =====================================================

SECTION_ORDER = [
    ("executive_summary", "Executive Summary"),
    ("market_analysis", "Market Analysis"),
    ("technical_analysis", "Technical Analysis"),
    ("strategy_implication", "Strategy Implications"),
    ("setup_validation", "Setup Validation"),
    ("outlook", "Outlook"),
]

# =====================================================
# HELPERS
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


def _json_to_text(value) -> str:
    if value is None:
        return ""

    if isinstance(value, str):
        return value

    if isinstance(value, dict):
        return "\n".join([f"{k}: {v}" for k, v in value.items()])

    if isinstance(value, list):
        return "\n".join([str(v) for v in value])

    return str(value)


# =====================================================
# STYLES
# =====================================================

def build_styles():
    styles = getSampleStyleSheet()

    styles.add(ParagraphStyle(
        name="Header",
        fontSize=18,
        leading=22,
        spaceAfter=16,
        fontName="Helvetica-Bold",
    ))

    styles.add(ParagraphStyle(
        name="SectionTitle",
        fontSize=13,
        leading=16,
        spaceBefore=14,
        spaceAfter=6,
        fontName="Helvetica-Bold",
    ))

    styles.add(ParagraphStyle(
        name="Body",
        fontSize=10.5,
        leading=15,
        spaceAfter=8,
        fontName="Helvetica",
    ))

    styles.add(ParagraphStyle(
        name="Small",
        fontSize=9,
        leading=12,
        textColor=colors.grey,
    ))

    return styles


# =====================================================
# SCORE BLOCK
# =====================================================

def render_scores(report, styles):
    data = [
        ["Macro", _fmt_percent(report.get("macro_score"))],
        ["Technical", _fmt_percent(report.get("technical_score"))],
        ["Market", _fmt_percent(report.get("market_score"))],
        ["Setup", _fmt_percent(report.get("setup_score"))],
    ]

    table = Table(data, colWidths=[5 * cm, 3 * cm])

    table.setStyle(TableStyle([
        ("BACKGROUND", (0,0), (-1,0), colors.whitesmoke),
        ("GRID", (0,0), (-1,-1), 0.25, colors.grey),
        ("FONTNAME", (0,0), (-1,-1), "Helvetica"),
        ("FONTSIZE", (0,0), (-1,-1), 10),
        ("ROWBACKGROUNDS", (0,0), (-1,-1),
         [colors.whitesmoke, colors.transparent]),
    ]))

    return [
        Paragraph("Market Scores", styles["SectionTitle"]),
        table,
        Spacer(1, 14),
    ]


# =====================================================
# INDICATOR TABLE
# =====================================================

def render_indicator_table(data: List[Dict], title, styles):
    if not data:
        return []

    table_data = [["Indicator", "Score", "Value", "Interpretation"]]

    for row in data:
        table_data.append([
            _clean_text(row.get("indicator")),
            str(round(row.get("score", 0), 1)),
            str(round(row.get("value", 0), 2)),
            _clean_text(row.get("interpretation")),
        ])

    table = Table(
        table_data,
        colWidths=[4 * cm, 2 * cm, 3 * cm, 7 * cm],
        repeatRows=1
    )

    table.setStyle(TableStyle([
        ("BACKGROUND", (0,0), (-1,0), colors.black),
        ("TEXTCOLOR", (0,0), (-1,0), colors.white),
        ("GRID", (0,0), (-1,-1), 0.25, colors.grey),
        ("FONTNAME", (0,0), (-1,0), "Helvetica-Bold"),
        ("FONTSIZE", (0,0), (-1,-1), 9),
        ("ROWBACKGROUNDS", (0,1), (-1,-1),
         [colors.whitesmoke, colors.transparent]),
    ]))

    return [
        Paragraph(title, styles["SectionTitle"]),
        table,
        Spacer(1, 12),
    ]


# =====================================================
# BOT CARD
# =====================================================

def render_bot_snapshot(bot, styles):
    if not bot:
        return []

    content = f"""
    <b>Bot:</b> {bot.get("bot_name","–")}<br/>
    <b>Action:</b> {bot.get("action","–")}<br/>
    <b>Confidence:</b> {bot.get("confidence","–")}<br/>
    <b>Amount:</b> €{bot.get("amount_eur",0)}<br/>
    """

    return [
        Paragraph("Bot Decision", styles["SectionTitle"]),
        Paragraph(_clean_text(content), styles["Body"]),
        Spacer(1, 12),
    ]


# =====================================================
# TEXT SECTIONS
# =====================================================

def render_text_sections(report, styles):
    story = []

    for key, title in SECTION_ORDER:
        value = report.get(key)

        if not value:
            continue

        text = _json_to_text(value)

        story.append(Paragraph(title, styles["SectionTitle"]))
        story.append(
            Paragraph(
                _clean_text(text).replace("\n", "<br/>"),
                styles["Body"]
            )
        )

    return story


# =====================================================
# MAIN RENDERER
# =====================================================

def generate_pdf_report(
    report: Dict[str, Any],
    report_type: str = "daily",
    save_to_disk: bool = False,
) -> io.BytesIO:

    buffer = io.BytesIO()

    report_date = report.get("report_date") \
        or datetime.utcnow().strftime("%Y-%m-%d")

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

    styles = build_styles()
    story = []

    # HEADER
    story.append(
        Paragraph(
            f"{report_type.capitalize()} Trading Report",
            styles["Header"]
        )
    )

    story.append(
        Paragraph(f"Date: {report_date}", styles["Small"])
    )

    story.append(Spacer(1, 12))

    # SCORES
    story.extend(render_scores(report, styles))

    # INDICATORS
    story.extend(
        render_indicator_table(
            report.get("macro_indicator_highlights"),
            "Macro Indicator Highlights",
            styles,
        )
    )

    story.extend(
        render_indicator_table(
            report.get("technical_indicator_highlights"),
            "Technical Indicator Highlights",
            styles,
        )
    )

    # BOT
    story.extend(
        render_bot_snapshot(
            report.get("bot_snapshot"),
            styles,
        )
    )

    # TEXT
    story.extend(render_text_sections(report, styles))

    # BUILD
    doc.build(story)
    buffer.seek(0)

    logger.info("✅ PDF render gereed")
    return buffer
