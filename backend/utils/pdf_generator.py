import os
import io
import json
import logging
import unicodedata
import re
from datetime import datetime

from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
from reportlab.lib.units import cm
from reportlab.lib.colors import HexColor

logger = logging.getLogger(__name__)

# =====================================================
# 🎨 Sectiekleuren (nieuw, aligned met report agent)
# =====================================================
SECTION_COLORS = {
    "executive_summary":    "#1F4FD8",  # blauw
    "macro_context":        "#4B5563",  # grijs
    "setup_validation":     "#B45309",  # amber
    "strategy_implication": "#7C3AED",  # paars
    "outlook":              "#065F46",  # groen
    "indicator_highlights": "#0F766E",  # teal
}

# =====================================================
# 🧩 Sectielabels (exacte report structuur)
# =====================================================
SECTION_LABELS = {
    "executive_summary":    "📌 Executive Summary",
    "macro_context":        "🌍 Macro Context",
    "setup_validation":     "✅ Setup Validatie",
    "strategy_implication": "🎯 Strategie Implicatie",
    "outlook":              "🔮 Vooruitblik",
    "indicator_highlights": "📊 Indicator Highlights",
}

# =====================================================
# 🧹 Helpers
# =====================================================
def strip_emoji(text: str) -> str:
    if not isinstance(text, str):
        return str(text)
    return re.sub(r'[\U00010000-\U0010ffff]', '', text)


def clean_text(text: str) -> str:
    if not isinstance(text, str):
        return str(text)
    try:
        text = strip_emoji(text)
        return unicodedata.normalize("NFKD", text).encode(
            "latin-1", "ignore"
        ).decode("latin-1")
    except Exception:
        return re.sub(r"[^\x00-\x7F]+", "", text)


def render_json(value) -> str:
    """
    Render JSONB (dict/list) of string netjes naar tekst.
    """
    if value is None:
        return "–"
    if isinstance(value, (dict, list)):
        return json.dumps(value, indent=2, ensure_ascii=False)
    return str(value)

# =====================================================
# 🖨️ PDF GENERATOR — DAILY / WEEKLY / MONTHLY
# =====================================================
def generate_pdf_report(
    report_row: dict,
    report_type: str = "daily",
    save_to_disk: bool = False,
) -> io.BytesIO:
    """
    Render PDF op basis van 1 rij uit daily_reports / weekly_reports / etc.
    """

    buffer = io.BytesIO()
    today_str = report_row.get("report_date") or datetime.utcnow().strftime("%Y-%m-%d")

    # Alleen opslaan als expliciet gevraagd
    if save_to_disk:
        base_folder = os.path.abspath("static/pdf")
        folder = os.path.join(base_folder, report_type)
        os.makedirs(folder, exist_ok=True)
        pdf_path = os.path.join(folder, f"{report_type}_{today_str}.pdf")
    else:
        pdf_path = None

    logger.info(f"🖨️ PDF genereren | type={report_type} | date={today_str}")

    doc = SimpleDocTemplate(
        buffer,
        pagesize=A4,
        rightMargin=2 * cm,
        leftMargin=2 * cm,
        topMargin=2 * cm,
        bottomMargin=2 * cm,
        title=f"{report_type.capitalize()} Trading Report ({today_str})",
    )

    styles = getSampleStyleSheet()

    styles.add(ParagraphStyle(
        name="SectionHeader",
        fontSize=13,
        leading=18,
        spaceAfter=10,
        spaceBefore=14,
        fontName="Helvetica-Bold",
        textColor=HexColor("#111827"),
    ))

    styles.add(ParagraphStyle(
        name="Content",
        fontSize=11,
        leading=16,
        spaceAfter=8,
        fontName="Helvetica",
    ))

    story = []

    # =====================================================
    # 🧾 HEADER
    # =====================================================
    story.append(Paragraph(
        clean_text(f"{report_type.capitalize()} Trading Report"),
        styles["Title"]
    ))
    story.append(Paragraph(
        clean_text(f"Datum: {today_str}"),
        styles["Normal"]
    ))
    story.append(Spacer(1, 14))

    # =====================================================
    # 📈 SCORES OVERVIEW
    # =====================================================
    scores_line = (
        f"Macro: {report_row.get('macro_score', '–')} | "
        f"Technical: {report_row.get('technical_score', '–')} | "
        f"Market: {report_row.get('market_score', '–')} | "
        f"Setup: {report_row.get('setup_score', '–')}"
    )
    story.append(Paragraph(
        clean_text(scores_line),
        styles["Content"]
    ))
    story.append(Spacer(1, 12))

    # =====================================================
    # 💰 MARKET SNAPSHOT
    # =====================================================
    price = report_row.get("price", "–")
    volume = report_row.get("volume", "–")
    change = report_row.get("change_24h", "–")

    if isinstance(volume, (int, float)):
        volume = f"{volume/1e9:.1f}B" if volume > 1e9 else f"{volume/1e6:.1f}M"

    market_line = f"Prijs: ${price} | Volume: {volume} | 24h verandering: {change}%"
    story.append(Paragraph(
        clean_text(market_line),
        styles["Content"]
    ))
    story.append(Spacer(1, 16))

    # =====================================================
    # 📄 SECTIES (EXACT DAILY_REPORTS STRUCTUUR)
    # =====================================================
    for key, label in SECTION_LABELS.items():
        value = report_row.get(key)
        if not value:
            continue

        color = HexColor(SECTION_COLORS.get(key, "#374151"))

        header_style = ParagraphStyle(
            name=f"{key}_header",
            fontName="Helvetica-Bold",
            fontSize=12,
            leading=16,
            textColor=color,
            spaceBefore=12,
            spaceAfter=6,
        )

        story.append(Paragraph(clean_text(label), header_style))

        body = render_json(value)
        body = clean_text(body).replace("\n", "<br/>")

        story.append(Paragraph(body, styles["Content"]))
        story.append(Spacer(1, 8))

    # =====================================================
    # 🖨️ BUILD PDF
    # =====================================================
    try:
        doc.build(story)
        buffer.seek(0)

        if pdf_path:
            with open(pdf_path, "wb") as f:
                f.write(buffer.getvalue())
            logger.info(f"✅ PDF opgeslagen: {pdf_path}")

        return buffer

    except Exception as e:
        logger.error("❌ PDF-generatie mislukt", exc_info=True)
        raise
