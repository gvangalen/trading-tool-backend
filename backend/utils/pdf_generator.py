import os
import io
import json
import logging
from datetime import datetime
from fpdf import FPDF

logger = logging.getLogger(__name__)

SECTION_COLORS = {
    "btc_summary": (70, 130, 180),
    "macro_summary": (105, 105, 105),
    "setup_checklist": (218, 165, 32),
    "priorities": (255, 140, 0),
    "wyckoff_analysis": (0, 128, 128),
    "recommendations": (178, 34, 34),
    "conclusion": (0, 100, 0),
    "outlook": (112, 128, 144),
}

SECTION_LABELS = {
    "btc_summary": "Bitcoin Samenvatting",
    "macro_summary": "Macro Overzicht",
    "setup_checklist": "Setup Checklist",
    "priorities": "Dagelijkse Prioriteiten",
    "wyckoff_analysis": "Wyckoff Analyse",
    "recommendations": "Aanbevelingen",
    "conclusion": "Conclusie",
    "outlook": "Vooruitblik",
}


class PDF(FPDF):
    def header(self):
        self.set_font("Helvetica", "B", 14)
        self.cell(0, 10, "Daily Trading Report (BTC)", ln=True, align="C")
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


def generate_pdf_report(data: dict, report_type: str = "daily", save_to_disk: bool = True) -> io.BytesIO:
    pdf = PDF()
    pdf.add_page()

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
                logger.warning(f"Fout bij converteren van sectie '{key}': {e}")
                body = f"[Fout bij renderen van deze sectie: {e}]"
            pdf.section_body(body)

    output = io.BytesIO()
    try:
        pdf_output = pdf.output(dest='S').encode('latin-1', errors='ignore')  # 🔧 emoji's of rare karakters worden genegeerd
        output.write(pdf_output)
        output.seek(0)

        if save_to_disk:
            today_str = datetime.now().strftime("%Y-%m-%d")
            folder = f"reports/pdf/{report_type}"
            os.makedirs(folder, exist_ok=True)
            path = f"{folder}/{today_str}.pdf"
            with open(path, "wb") as f:
                f.write(pdf_output)
            logger.info(f"PDF opgeslagen op {path}")

        return output

    except Exception as e:
        logger.error(f"PDF-generatie mislukt: {e}")
        raise
