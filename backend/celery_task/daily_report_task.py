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

        # ✅ Scores ophalen met fallback
        macro_score = full_report.get("macro_score")
        technical_score = full_report.get("technical_score")
        setup_score = full_report.get("setup_score")
        sentiment_score = full_report.get("sentiment_score")

        # ✅ Als één van de scores ontbreekt, gebruik setup_scores als fallback
        if None in (macro_score, technical_score, sentiment_score):
            logger.warning("⚠️ Ontbrekende scores in full_report. Fallback naar setup_scores.")
            from backend.utils.scoring_utils import calculate_combined_score
            combined = calculate_combined_score(symbol)
            macro_score = macro_score if macro_score is not None else combined.get("macro_score", 0)
            technical_score = technical_score if technical_score is not None else combined.get("technical_score", 0)
            sentiment_score = sentiment_score if sentiment_score is not None else combined.get("sentiment_score", 0)

        # ✅ Setup score blijft het gemiddelde van macro en technical (bij ontbreken)
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

        # ✅ Debuglog vóór INSERT
        logger.info("🧪 INSERT INTO daily_reports met kolommen: report_date, symbol, btc_summary, macro_summary, ...")

        # ✅ Volledig rapport opslaan in één rij
        cursor.execute(
            """
            INSERT INTO daily_reports (
                report_date, symbol, btc_summary, macro_summary,
                setup_checklist, priorities, wyckoff_analysis,
                recommendations, conclusion, outlook,
                macro_score, technical_score, setup_score, sentiment_score
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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

        # ✅ Scores apart opslaan in daily_scores
        cursor.execute(
            """
            INSERT INTO daily_scores (
                symbol, report_date,
                macro_score, technical_score, setup_score, sentiment_score
            ) VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (symbol, today, macro_score, technical_score, setup_score, sentiment_score)
        )

        conn.commit()
        conn.close()
        logger.info(f"✅ Dagrapport en scores opgeslagen voor {symbol} ({today})")

    except Exception as e:
        logger.error(f"❌ Fout bij genereren rapportsecties: {e}", exc_info=True)
