import io
import json
import logging
from datetime import datetime
from fpdf import FPDF

logger = logging.getLogger(__name__)

# 🎨 Kleuren per sectie (matcht databasekolommen)
SECTION_COLORS = {
    "btc_summary": (70, 130, 180),         # Steel Blue
    "macro_summary": (105, 105, 105),      # Dim Gray
    "setup_checklist": (218, 165, 32),     # Goldenrod
    "priorities": (255, 140, 0),           # Dark Orange
    "wyckoff_analysis": (0, 128, 128),     # Teal
    "recommendations": (178, 34, 34),      # Firebrick
    "conclusion": (0, 100, 0),             # Dark Green
    "outlook": (112, 128, 144),            # Slate Gray
}

# 🧾 Vertalingen / titels voor de PDF-secties
SECTION_LABELS = {
    "btc_summary": "🧠 Bitcoin Samenvatting",
    "macro_summary": "📉 Macro Overzicht",
    "setup_checklist": "📋 Setup Checklist",
    "priorities": "🎯 Dagelijkse Prioriteiten",
    "wyckoff_analysis": "🔍 Wyckoff Analyse",
    "recommendations": "📈 Aanbevelingen",
    "conclusion": "✅ Conclusie",
    "outlook": "🔮 Vooruitblik",
}

# 📄 PDF Klasse met custom layout
class PDF(FPDF):
    def header(self):
        self.set_font("Helvetica", "B", 14)
        self.cell(0, 10, "📊 Daily Trading Report (BTC)", ln=True, align="C")
        self.set_font("Helvetica", "", 10)
        self.cell(0, 10, datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"), ln=True, align="C")
        self.ln(5)

    def section_title(self, title, rgb=(200, 200, 200)):
        """Maak sectietitel met achtergrondkleur"""
        self.set_fill_color(*rgb)
        self.set_font("Helvetica", "B", 12)
        self.set_text_color(255, 255, 255)
        self.cell(0, 8, f" {title}", ln=True, fill=True)
        self.set_text_color(0, 0, 0)
        self.ln(2)

    def section_body(self, text):
        """Voeg de tekst van een sectie toe"""
        self.set_font("Helvetica", "", 10)
        self.multi_cell(0, 6, text)
        self.ln(2)


# 🧠 Hooffunctie om rapportdata (dict) om te zetten naar PDF
def generate_pdf_report(data: dict) -> io.BytesIO:
    pdf = PDF()
    pdf.add_page()

    # Doorloop alle bekende secties op basis van databasekolommen
    for key, label in SECTION_LABELS.items():
        value = data.get(key)
        if value:
            color = SECTION_COLORS.get(key, (128, 128, 128))
            pdf.section_title(label, rgb=color)

            try:
                body = (
                    json.dumps(value, indent=2, ensure_ascii=False)
                    if isinstance(value, dict)
                    else str(value)
                )
            except Exception as e:
                logger.warning(f"⚠️ Fout bij converteren van sectie '{key}': {e}")
                body = f"[Fout bij renderen van deze sectie: {e}]"

            pdf.section_body(body)

    # Genereer PDF in geheugen
    output = io.BytesIO()
    try:
        pdf_bytes = pdf.output(dest='S').encode('latin-1')  # FPDF geeft tekst terug in Latin-1
        output.write(pdf_bytes)
        output.seek(0)
        return output
    except Exception as e:
        logger.error(f"❌ PDF-generatie mislukt: {e}")
        raise
