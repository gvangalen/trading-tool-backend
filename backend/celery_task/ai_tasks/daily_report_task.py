from celery import shared_task
from backend.utils.db import get_db_connection
from backend.utils.ai_report_utils import generate_daily_report_sections
import logging

logger = logging.getLogger(__name__)

@shared_task
def generate_daily_report_task(symbol="BTC"):
    logger.info(f"📅 START daily report task voor {symbol}")
    conn = get_db_connection()
    if not conn:
        logger.error("❌ DB-verbinding mislukt.")
        return

    try:
        report = generate_daily_report_sections(symbol)

        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO daily_reports (
                    report_date, btc_summary, macro_summary, setup_checklist,
                    priorities, wyckoff_analysis, recommendations, conclusion, outlook,
                    macro_score, technical_score, setup_score, sentiment_score
                ) VALUES (CURRENT_DATE, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                    sentiment_score = EXCLUDED.sentiment_score;
            """, (
                report["btc_summary"],
                report["macro_summary"],
                report["setup_checklist"],
                report["priorities"],
                report["wyckoff_analysis"],
                report["recommendations"],
                report["conclusion"],
                report["outlook"],
                report["macro_score"],
                report["technical_score"],
                report["setup_score"],
                report["sentiment_score"]
            ))

            conn.commit()
            logger.info("✅ Dagrapport succesvol opgeslagen.")

    except Exception as e:
        logger.error(f"❌ Fout bij opslaan dagrapport: {e}")

    finally:
        conn.close()
