import os
import logging
from datetime import datetime

from celery import shared_task
from dotenv import load_dotenv

from backend.utils.db import get_db_connection
from backend.utils.report_sections import generate_daily_report_sections

# === ✅ Logging instellen
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# === ✅ Task: Dagrapport genereren
@shared_task
def generate_daily_report(symbol: str = "BTC"):
    logger.info("🔄 Dagrapport-task versie 6-OCT-21:20 live")
    load_dotenv()

    try:
        logger.info("📝 Genereren van dagelijks rapport gestart...")
        full_report = generate_daily_report_sections(symbol)

        # ✅ Extracteer tekstuele secties voor rapport
        sections = [
            {"title": "Samenvatting", "data": full_report.get("btc_summary")},
            {"title": "Macro", "data": full_report.get("macro_summary")},
            {"title": "Checklist", "data": full_report.get("setup_checklist")},
            {"title": "Prioriteiten", "data": full_report.get("priorities")},
            {"title": "Wyckoff", "data": full_report.get("wyckoff_analysis")},
            {"title": "Advies", "data": full_report.get("recommendations")},
            {"title": "Conclusie", "data": full_report.get("conclusion")},
            {"title": "Vooruitblik", "data": full_report.get("outlook")},
        ]

        # ✅ Scores
        macro_score = full_report.get("macro_score", 0)
        technical_score = full_report.get("technical_score", 0)
        setup_score = full_report.get("setup_score", 0)
        sentiment_score = full_report.get("sentiment_score", 0)

        # ✅ Database connectie
        conn = get_db_connection()
        if not conn:
            logger.error("❌ Geen databaseverbinding. Rapport niet opgeslagen.")
            return

        cursor = conn.cursor()
        today = datetime.now().strftime("%Y-%m-%d")

        # ✅ Opslaan van rapportregels
        for section in sections:
            title = section.get("title")
            data = section.get("data")
            if not data:
                logger.warning(f"⚠️ Lege sectie: {title}")
                continue
            cursor.execute(
                "INSERT INTO daily_reports (symbol, date, section_title, section_text) VALUES (%s, %s, %s, %s)",
                (symbol, today, title, data)
            )

        # ✅ Opslaan van scores
        cursor.execute(
            "INSERT INTO daily_scores (symbol, date, macro_score, technical_score, setup_score, sentiment_score) VALUES (%s, %s, %s, %s, %s, %s)",
            (symbol, today, macro_score, technical_score, setup_score, sentiment_score)
        )

        conn.commit()
        conn.close()
        logger.info("✅ Dagrapport succesvol opgeslagen in database.")

    except Exception as e:
        logger.error(f"❌ Fout bij genereren rapportsecties: {e}")
