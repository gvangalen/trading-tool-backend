import io
import json
import logging
from datetime import datetime
from fpdf import FPDF

logger = logging.getLogger(__name__)

# 🎨 Kleuren per sectie (lichtgrijze achtergrond + accentkleur)
SECTION_COLORS = {
    "summary": (70, 130, 180),         # Steel Blue
    "macro": (105, 105, 105),          # Dim Gray
    "technical": (34, 139, 34),        # Forest Green
    "setups": (218, 165, 32),          # Goldenrod
    "strategy": (65, 105, 225),        # Royal Blue
    "recommendation": (178, 34, 34),   # Firebrick
    "conclusion": (0, 128, 0),         # Green
    "outlook": (112, 128, 144),        # Slate Gray
}

# 📄 PDF Klasse met custom layout
class PDF(FPDF):
    def header(self):
        self.set_font("Helvetica", "B", 14)
        self.cell(0, 10, "📊 Daily Trading Report", ln=True, align="C")
        self.set_font("Helvetica", "", 10)
        self.cell(0, 10, datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"), ln=True, align="C")
        self.ln(5)

    def section_title(self, title, rgb=(200, 200, 200)):
        self.set_fill_color(*rgb)
        self.set_font("Helvetica", "B", 12)
        self.set_text_color(255, 255, 255)
        self.cell(0, 8, f" {title}", ln=True, fill=True)
        self.set_text_color(0, 0, 0)
        self.ln(2)

    def section_body(self, text):
        self.set_font("Helvetica", "", 10)
        self.multi_cell(0, 6, text)
        self.ln(2)


# 🧠 Hooffunctie om JSON-data om te zetten naar PDF
def generate_pdf_report(data: dict) -> io.BytesIO:
    pdf = PDF()
    pdf.add_page()

    for section in [
        "summary",
        "macro",
        "technical",
        "setups",
        "strategy",
        "recommendation",
        "conclusion",
        "outlook"
    ]:
        if section in data and data[section]:
            title = section.replace("_", " ").title()
            color = SECTION_COLORS.get(section, (128, 128, 128))  # fallback kleur
            pdf.section_title(title, rgb=color)

            try:
                body = (
                    json.dumps(data[section], indent=2, ensure_ascii=False)
                    if isinstance(data[section], dict)
                    else str(data[section])
                )
            except Exception as e:
                logger.warning(f"⚠️ Fout bij converteren van sectie '{section}': {e}")
                body = f"[Fout bij renderen van deze sectie: {e}]"

            pdf.section_body(body)

    # Genereer PDF in geheugen
    output = io.BytesIO()
    try:
        pdf_bytes = pdf.output(dest='S').encode('latin-1')  # FPDF returns Latin-1 encoded string
        output.write(pdf_bytes)
        output.seek(0)
        return output
    except Exception as e:
        logger.error(f"❌ PDF-generatie mislukt: {e}")
        raise
