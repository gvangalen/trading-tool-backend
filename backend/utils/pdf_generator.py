import os
import json
import logging
from datetime import datetime
from fpdf import FPDF

logger = logging.getLogger(__name__)

# 📄 PDF Klassestructuur
class PDF(FPDF):
    def header(self):
        self.set_font("Helvetica", "B", 14)
        self.cell(0, 10, "📊 Daily Trading Report", ln=1, align="C")
        self.set_font("Helvetica", "", 10)
        self.cell(0, 10, datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"), ln=1, align="C")
        self.ln(5)

    def section_title(self, title):
        self.set_font("Helvetica", "B", 12)
        self.set_fill_color(240, 240, 240)
        self.cell(0, 8, title, ln=1, fill=True)
        self.ln(1)

    def section_body(self, text):
        self.set_font("Helvetica", "", 10)
        self.multi_cell(0, 6, text)
        self.ln(2)


# 🧠 Functie om JSON in PDF te zetten
def generate_pdf_report(json_data, output_path="daily_report.pdf"):
    pdf = PDF()
    pdf.add_page()

    # 🔄 JSON data verwerken per sectie
    for section in ["summary", "macro", "technical", "setups", "strategy", "recommendation"]:
        if section in json_data:
            title = section.replace("_", " ").title()
            pdf.section_title(f"{title}")
            body = json.dumps(json_data[section], indent=2) if isinstance(json_data[section], dict) else str(json_data[section])
            pdf.section_body(body)

    try:
        pdf.output(output_path)
        logger.info(f"✅ PDF gegenereerd: {output_path}")
        return output_path
    except Exception as e:
        logger.error(f"❌ PDF genereren mislukt: {e}")
        return None


# 🔧 Test-run
if __name__ == "__main__":
    with open("daily_report.json") as f:
        data = json.load(f)
    generate_pdf_report(data)
