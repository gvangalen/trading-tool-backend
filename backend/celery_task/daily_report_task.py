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
    Dagelijks AI-rapport genereren en veilig opslaan in database.
    Dubbele entries worden overschreven met de nieuwste data.
    """
    logger.info("🔄 Dagrapport-task gestart")
    load_dotenv()

    try:
        logger.info("📝 Rapportgeneratie gestart...")
        full_report = generate_daily_report_sections(symbol)

        if not isinstance(full_report, dict):
            logger.error("❌ Ongeldige rapportstructuur (geen dict). Afgebroken.")
            return

        # ✅ Scores ophalen met fallback
        macro_score = full_report.get("macro_score")
        technical_score = full_report.get("technical_score")
        setup_score = full_report.get("setup_score")
        sentiment_score = full_report.get("sentiment_score")

        # ✅ Als één van de scores ontbreekt → fallback naar setup_scores
        if None in (macro_score, technical_score, sentiment_score):
            logger.warning("⚠️ Ontbrekende scores in full_report. Fallback naar setup_scores.")
            from backend.utils.scoring_utils import calculate_combined_score
            combined = calculate_combined_score(symbol)
            macro_score = macro_score or combined.get("macro_score", 0)
            technical_score = technical_score or combined.get("technical_score", 0)
            sentiment_score = sentiment_score or combined.get("sentiment_score", 0)

        # ✅ Setup score = gemiddelde macro + technische score (indien None)
        if setup_score is None:
            setup_score = round((macro_score + technical_score) / 2, 2)

        # ✅ Databaseverbinding openen
        conn = get_db_connection()
        if not conn:
            logger.error("❌ Geen databaseverbinding. Rapport niet opgeslagen.")
            return

        cursor = conn.cursor()
        today = datetime.now().strftime("%Y-%m-%d")

        logger.info(f"🚀 Start opslag van dagrapport ({symbol}) voor {today}")
        logger.info("🧪 INSERT INTO daily_reports ... ON CONFLICT DO UPDATE")

        # ✅ Conflict-safe insert voor daily_reports
        cursor.execute(
            """
            INSERT INTO daily_reports (
                report_date, symbol, btc_summary, macro_summary,
                setup_checklist, priorities, wyckoff_analysis,
                recommendations, conclusion, outlook,
                macro_score, technical_score, setup_score, sentiment_score
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (report_date, symbol) DO UPDATE
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
                today, symbol,
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

        # ✅ Conflict-safe insert voor daily_scores
        cursor.execute(
            """
            INSERT INTO daily_scores (
                symbol, report_date,
                macro_score, technical_score, setup_score, sentiment_score
            ) VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, report_date) DO UPDATE
            SET macro_score = EXCLUDED.macro_score,
                technical_score = EXCLUDED.technical_score,
                setup_score = EXCLUDED.setup_score,
                sentiment_score = EXCLUDED.sentiment_score
            """,
            (symbol, today, macro_score, technical_score, setup_score, sentiment_score)
        )

        conn.commit()
        conn.close()
        logger.info(f"✅ Dagrapport en scores succesvol opgeslagen of bijgewerkt voor {symbol} ({today})")

    except Exception as e:
        logger.error(f"❌ Fout bij genereren rapportsecties: {e}", exc_info=True)
