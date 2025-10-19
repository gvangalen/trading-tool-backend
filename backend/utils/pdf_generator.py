import os
import io
import json
import logging
from datetime import datetime
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
from reportlab.lib.units import cm
from reportlab.lib.colors import HexColor

logger = logging.getLogger(__name__)

# === 🎨 Sectiekleuren (RGB in HEX)
SECTION_COLORS = {
    "btc_summary": "#4682B4",       # steelblue
    "macro_summary": "#696969",     # dimgray
    "setup_checklist": "#DAA520",   # goldenrod
    "priorities": "#FF8C00",        # darkorange
    "wyckoff_analysis": "#008080",  # teal
    "recommendations": "#B22222",   # firebrick
    "conclusion": "#006400",        # darkgreen
    "outlook": "#708090",           # slategray
}

# === 🧩 Sectielabels
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


def generate_pdf_report(data: dict, report_type: str = "daily", save_to_disk: bool = True) -> io.BytesIO:
    """
    ✅ Unicode-proof PDF generator met kleuren, emoji-ondersteuning en veilige opslag
    """
    buffer = io.BytesIO()
    today_str = datetime.now().strftime("%Y-%m-%d")
    folder = f"reports/pdf/{report_type}"
    os.makedirs(folder, exist_ok=True)
    pdf_path = f"{folder}/{today_str}.pdf"

    # === 📄 Documentinstellingen
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
    styles.add(ParagraphStyle(name='SectionHeader', fontSize=13, leading=16, spaceAfter=10, spaceBefore=14, fontName='Helvetica-Bold'))
    styles.add(ParagraphStyle(name='Content', fontSize=10.5, leading=14, spaceAfter=8, fontName='Helvetica'))

    story = []

    # === 🧾 Header
    story.append(Paragraph("📈 Daily Trading Report (BTC)", styles["Title"]))
    story.append(Paragraph(datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"), styles["Normal"]))
    story.append(Spacer(1, 12))

    # === 🧱 Secties genereren
    for key, label in SECTION_LABELS.items():
        value = data.get(key)
        if not value:
            continue

        # Titelblok met kleur
        color = HexColor(SECTION_COLORS.get(key, "#808080"))
        header_style = ParagraphStyle(
            name=f"{key}_header",
            fontName="Helvetica-Bold",
            fontSize=12,
            leading=14,
            textColor=color,
            spaceBefore=10,
            spaceAfter=6,
        )
        story.append(Paragraph(label, header_style))

        # Tekstinhoud
        try:
            if isinstance(value, (dict, list)):
                body = json.dumps(value, indent=2, ensure_ascii=False)
            else:
                body = str(value)
        except Exception as e:
            logger.warning(f"⚠️ Fout bij converteren van sectie '{key}': {e}")
            body = f"[Fout bij renderen van deze sectie: {e}]"

        # Emoji & unicode veilig
        body = body.replace("\n", "<br/>")
        story.append(Paragraph(body, styles["Content"]))
        story.append(Spacer(1, 6))

    # === 📦 PDF genereren
    try:
        doc.build(story)
        buffer.seek(0)

        if save_to_disk:
            with open(pdf_path, "wb") as f:
                f.write(buffer.getvalue())
            logger.info(f"✅ PDF opgeslagen op: {pdf_path}")

        return buffer

    except Exception as e:
        logger.error(f"❌ PDF-generatie mislukt: {e}")
        raise
