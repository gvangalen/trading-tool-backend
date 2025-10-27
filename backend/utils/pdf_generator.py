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

# 🎨 Sectiekleuren
SECTION_COLORS = {
    "btc_summary": "#4682B4",       # steelblue
    "macro_summary": "#696969",     # dimgray
    "setup_checklist": "#DAA520",   # goldenrod
    "priorities": "#FF8C00",        # darkorange
    "wyckoff_analysis": "#008080",  # teal
    "recommendations": "#B22222",   # firebrick
    "conclusion": "#006400",        # darkgreen
    "outlook": "#708090",           # slategray,
}

# 🧩 Sectielabels met emoji’s (worden opgeschoond)
SECTION_LABELS = {
    "btc_summary": "📊 Bitcoin Samenvatting",
    "macro_summary": "🌍 Macro Overzicht",
    "setup_checklist": "✅ Setup Checklist",
    "priorities": "🎯 Dagelijkse Prioriteiten",
    "wyckoff_analysis": "🌀 Wyckoff Analyse",
    "recommendations": "💡 Aanbevelingen",
    "conclusion": "🧠 Conclusie",
    "outlook": "🔮 Vooruitblik",
}

# 🧹 Helper om emoji’s te strippen
def strip_emoji(text: str) -> str:
    if not isinstance(text, str):
        return str(text)
    return re.sub(r'[\U00010000-\U0010ffff]', '', text)

# 🧹 Helper om niet‑printbare tekens te verwijderen
def clean_text(text: str) -> str:
    if not isinstance(text, str):
        return str(text)
    try:
        text = strip_emoji(text)
        return unicodedata.normalize("NFKD", text).encode("latin-1", "ignore").decode("latin-1")
    except Exception:
        return re.sub(r"[^\x00-\x7F]+", "", text)

# 🧾 PDF generator
def generate_pdf_report(data: dict, report_type: str = "daily", save_to_disk: bool = True) -> io.BytesIO:
    buffer = io.BytesIO()
    today_str = datetime.now().strftime("%Y-%m-%d")

    base_folder = os.path.abspath("static/pdf")
    folder = os.path.join(base_folder, report_type)
    os.makedirs(folder, exist_ok=True)
    pdf_path = os.path.join(folder, f"{report_type}_{today_str}.pdf")

    logger.info(f"⏳ Genereren van PDF gestart voor type '{report_type}' op {pdf_path}")

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
        name='SectionHeader',
        fontSize=13,
        leading=18,
        spaceAfter=12,
        spaceBefore=16,
        fontName='Helvetica-Bold',
        textColor=HexColor("#333333"),
    ))
    styles.add(ParagraphStyle(
        name='Content',
        fontSize=11,
        leading=16,
        spaceAfter=10,
        spaceBefore=6,
        fontName='Helvetica',
    ))

    story = []

    # === 🧾 Header
    story.append(Paragraph(clean_text(strip_emoji("📈 Daily Trading Report (BTC)")), styles["Title"]))
    story.append(Paragraph(datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"), styles["Normal"]))
    story.append(Spacer(1, 12))

    # === 💰 Marktdata blok (optioneel)
    market_data = data.get("market_data")
    if isinstance(market_data, dict):
        story.append(Paragraph(clean_text("💰 Marktgegevens"), styles["SectionHeader"]))
        price = market_data.get("price", "–")
        volume = market_data.get("volume", "–")
        change = market_data.get("change_24h", "–")

        # Optioneel: format volume als bijv. 1.2B
        if isinstance(volume, (int, float)):
            volume = f"{volume/1e9:.1f}B" if volume > 1e9 else f"{volume/1e6:.1f}M"
        change_str = f"{change}%" if isinstance(change, (int, float)) else change

        price_line = f"Prijs: ${price} | Volume: {volume} | 24h Verandering: {change_str}"
        story.append(Paragraph(price_line, styles["Content"]))
        story.append(Spacer(1, 12))

    # === 🧱 Secties
    for key, label in SECTION_LABELS.items():
        value = data.get(key)
        if not value:
            continue

        color = HexColor(SECTION_COLORS.get(key, "#808080"))
        header_style = ParagraphStyle(
            name=f"{key}_header",
            fontName="Helvetica-Bold",
            fontSize=12,
            leading=16,
            textColor=color,
            spaceBefore=12,
            spaceAfter=8,
        )

        # 🔤 Titel (emoji gestript)
        story.append(Paragraph(clean_text(strip_emoji(label)), header_style))

        # 📄 Inhoud
        try:
            if isinstance(value, (dict, list)):
                body = json.dumps(value, indent=2, ensure_ascii=False)
            else:
                body = str(value)
        except Exception as e:
            logger.warning(f"⚠️ Fout bij converteren van sectie '{key}': {e}")
            body = f"[Fout bij renderen van deze sectie: {e}]"

        body = clean_text(strip_emoji(body)).replace("\n", "<br/>")
        story.append(Paragraph(body, styles["Content"]))
        story.append(Spacer(1, 6))

    # === 🖨️ PDF bouwen
    try:
        doc.build(story)
        buffer.seek(0)

        if save_to_disk:
            with open(pdf_path, "wb") as f:
                f.write(buffer.getvalue())
            logger.info(f"✅ PDF opgeslagen op: {pdf_path}")
            if pdf_path.startswith(os.path.abspath("static")):
                logger.info(f"🌐 PDF beschikbaar via URL: /{os.path.relpath(pdf_path, 'static')}")
            else:
                logger.warning("❗ PDF buiten /static map opgeslagen – niet direct downloadbaar via frontend.")

        return buffer

    except Exception as e:
        logger.error(f"❌ PDF-generatie mislukt: {e}", exc_info=True)
        raise
