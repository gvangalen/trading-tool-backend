# backend/celery_task/daily_report_task.py

import os
import logging
from datetime import datetime
from celery import shared_task
from dotenv import load_dotenv

from backend.utils.db import get_db_connection
from backend.utils.ai_report_utils import generate_daily_report_sections
from backend.utils.scoring_utils import calculate_combined_score
from backend.utils.pdf_generator import generate_pdf_report

# === Logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
load_dotenv()

@shared_task(name="backend.celery_task.daily_report_task.generate_daily_report")
def generate_daily_report():
    logger.info("🔄 Dagrapport-task gestart")

    try:
        logger.info("📝 Rapportgeneratie gestart...")
        full_report = generate_daily_report_sections("BTC")  # nog altijd BTC-gebaseerd

        if not isinstance(full_report, dict):
            logger.error("❌ Ongeldige rapportstructuur (geen dict). Afgebroken.")
            return

        macro_score = full_report.get("macro_score")
        technical_score = full_report.get("technical_score")
        setup_score = full_report.get("setup_score")
        sentiment_score = full_report.get("sentiment_score")

        if not all(isinstance(s, (int, float)) for s in [macro_score, technical_score, sentiment_score]):
            logger.warning("⚠️ Scores missen of ongeldig – fallback naar calculate_combined_score()")
            combined = calculate_combined_score("BTC")
            macro_score = macro_score if isinstance(macro_score, (int, float)) else combined.get("macro_score", 0)
            technical_score = technical_score if isinstance(technical_score, (int, float)) else combined.get("technical_score", 0)
            sentiment_score = sentiment_score if isinstance(sentiment_score, (int, float)) else combined.get("sentiment_score", 0)

        if not isinstance(setup_score, (int, float)):
            setup_score = round((macro_score + technical_score) / 2, 2)

        today = datetime.now().strftime("%Y-%m-%d")
        conn = get_db_connection()
        cursor = conn.cursor()

        logger.info(f"🚀 Opslaan dagrapport voor {today}")
        cursor.execute(
            """
            INSERT INTO daily_reports (
                report_date, btc_summary, macro_summary,
                setup_checklist, priorities, wyckoff_analysis,
                recommendations, conclusion, outlook,
                macro_score, technical_score, setup_score, sentiment_score
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (report_date) DO UPDATE
            SET btc_summary = EXCLUDED.btc_summary,
                macro_summary = EXCLUDED.macro_summary,
                setup_checklist = EXCLUDED.setup_checklist,
                priorities = EXCLUDED.priorities,
                wyckoff_analysis = EXCLUDED.wyckoff_analysis,
                recommendations = EXCLUDED.recommendations,
                conclusion = EXCLUDED.conclusion,
                outlook = EXCLUDED.outlook,
                macro_score = EXCLUDED.macro_score,
                technical_score = EXCLUDED.technical_score,
                setup_score = EXCLUDED.setup_score,
                sentiment_score = EXCLUDED.sentiment_score
            """,
            (
                today,
                full_report.get("btc_summary", ""),
                full_report.get("macro_summary", ""),
                full_report.get("setup_checklist", ""),
                full_report.get("priorities", ""),
                full_report.get("wyckoff_analysis", ""),
                full_report.get("recommendations", ""),
                full_report.get("conclusion", ""),
                full_report.get("outlook", ""),
                macro_score, technical_score, setup_score, sentiment_score
            )
        )

        # ✅ Scores apart loggen (voor grafieken)
        cursor.execute(
            """
            INSERT INTO daily_scores (
                report_date, macro_score, technical_score, setup_score, sentiment_score
            ) VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (report_date) DO UPDATE
            SET macro_score = EXCLUDED.macro_score,
                technical_score = EXCLUDED.technical_score,
                setup_score = EXCLUDED.setup_score,
                sentiment_score = EXCLUDED.sentiment_score
            """,
            (today, macro_score, technical_score, setup_score, sentiment_score)
        )

        conn.commit()

        # ✅ PDF genereren
        cursor.execute("SELECT * FROM daily_reports WHERE report_date = %s LIMIT 1;", (today,))
        row = cursor.fetchone()
        if row:
            cols = [desc[0] for desc in cursor.description]
            report_dict = dict(zip(cols, row))
            generate_pdf_report(report_dict, report_type="daily")
            logger.info(f"🖨️ PDF gegenereerd voor {today}")
        else:
            logger.warning(f"⚠️ Geen rapport gevonden voor PDF voor {today}")

        conn.close()
        logger.info(f"✅ Dagrapport succesvol opgeslagen en verwerkt ({today})")

    except Exception as e:
        logger.error(f"❌ Fout tijdens rapportgeneratie: {e}", exc_info=True)
