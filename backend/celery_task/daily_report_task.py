# ✅ backend/celery_task/daily_report_task.py
import os
import logging
from datetime import datetime
from celery import shared_task
from dotenv import load_dotenv
from backend.utils.db import get_db_connection
from backend.utils.ai_report_utils import generate_daily_report_sections

# === ✅ Logging instellen
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


@shared_task(name="backend.celery_task.daily_report_task.generate_daily_report")
def generate_daily_report(symbol: str = "BTC"):
    """
    Dagelijks AI-rapport genereren en opslaan in database.
    """
    logger.info("🔄 Dagrapport-task gestart")
    load_dotenv()

    try:
        logger.info("📝 Rapportgeneratie gestart...")
        full_report = generate_daily_report_sections(symbol)

        if not isinstance(full_report, dict):
            logger.error("❌ Ongeldige rapportstructuur (geen dict). Afgebroken.")
            return

        # ✅ Rapportsecties voorbereiden
        sections = [
            {"title": "Samenvatting", "data": full_report.get("btc_summary", "")},
            {"title": "Macro", "data": full_report.get("macro_summary", "")},
            {"title": "Checklist", "data": full_report.get("setup_checklist", "")},
            {"title": "Prioriteiten", "data": full_report.get("priorities", "")},
            {"title": "Wyckoff", "data": full_report.get("wyckoff_analysis", "")},
            {"title": "Advies", "data": full_report.get("recommendations", "")},
            {"title": "Conclusie", "data": full_report.get("conclusion", "")},
            {"title": "Vooruitblik", "data": full_report.get("outlook", "")},
        ]

        # ✅ Scores ophalen met fallback
        macro_score = full_report.get("macro_score", 0)
        technical_score = full_report.get("technical_score", 0)
        setup_score = full_report.get("setup_score", 0)
        sentiment_score = full_report.get("sentiment_score", 0)

        # ✅ Databaseverbinding openen
        conn = get_db_connection()
        if not conn:
            logger.error("❌ Geen databaseverbinding. Rapport niet opgeslagen.")
            return

        cursor = conn.cursor()
        today = datetime.now().strftime("%Y-%m-%d")

        logger.info(f"🚀 Start opslag van dagrapport ({symbol}) voor {today}")

        # ✅ Secties opslaan in daily_reports
        for section in sections:
            title = section["title"]
            text = section["data"]
            if not text:
                logger.warning(f"⚠️ Lege sectie: {title}")
                continue

            cursor.execute(
                """
                INSERT INTO daily_reports (symbol, report_date, section_title, section_text)
                VALUES (%s, %s, %s, %s)
                """,
                (symbol, today, title, text)
            )

        # ✅ Scores opslaan in daily_scores
        cursor.execute(
            """
            INSERT INTO daily_scores (symbol, report_date, macro_score, technical_score, setup_score, sentiment_score)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (symbol, today, macro_score, technical_score, setup_score, sentiment_score)
        )

        conn.commit()
        conn.close()

        logger.info(f"✅ Dagrapport en scores opgeslagen in database voor {symbol} ({today})")

    except Exception as e:
        logger.error(f"❌ Fout bij genereren rapportsecties: {e}", exc_info=True)
