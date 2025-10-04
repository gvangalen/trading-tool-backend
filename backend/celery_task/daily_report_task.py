import os
import json
import logging
from dotenv import load_dotenv
from datetime import datetime
from pytz import timezone
from celery import shared_task

from backend.utils.db import get_db_connection
from backend.utils.ai_report_utils import generate_daily_report_sections  # <-- AI-rapport

# ✅ .env laden (voor consistentie, niet strikt nodig hier)
load_dotenv()

# ✅ Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def save_report_to_db(date, report_data):
    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen databaseverbinding beschikbaar.")
        return False

    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO daily_reports (
                    report_date,
                    btc_summary,
                    macro_summary,
                    setup_checklist,
                    priorities,
                    wyckoff_analysis,
                    recommendations,
                    conclusion,
                    outlook,
                    macro_score,
                    technical_score,
                    setup_score,
                    sentiment_score
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (report_date) DO UPDATE SET
                    btc_summary = EXCLUDED.btc_summary,
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
            """, (
                date,
                report_data.get("btc_summary"),
                report_data.get("macro_summary"),
                report_data.get("setup_checklist"),
                report_data.get("priorities"),
                report_data.get("wyckoff_analysis"),
                report_data.get("recommendations"),
                report_data.get("conclusion"),
                report_data.get("outlook"),
                report_data.get("macro_score"),
                report_data.get("technical_score"),
                report_data.get("setup_score"),
                report_data.get("sentiment_score"),
            ))
            conn.commit()
        logger.info("✅ Dagrapport succesvol opgeslagen in de database.")
        return True
    except Exception as e:
        logger.error(f"❌ Fout bij opslaan rapport: {e}")
        return False
    finally:
        conn.close()


@shared_task(name="backend.celery_task.daily_report_task.generate_daily_report")
def generate_daily_report():
    logger.info("📝 Genereren van dagelijks rapport gestart...")

    if not get_db_connection():
        logger.error("❌ Dagrapport geannuleerd: databaseverbinding faalt.")
        return

    today = datetime.now(timezone("UTC")).date()

    try:
        report_data = generate_daily_report_sections(symbol="BTC")
    except Exception as e:
        logger.error(f"❌ Fout bij genereren rapportsecties: {e}")
        return

    # ✅ Fix: controleer of report_data een dict is
    if not isinstance(report_data, dict):
        logger.error(f"❌ Ongeldig rapportformaat ontvangen: {type(report_data)} → {report_data}")
        return

    if not report_data.get("btc_summary"):
        logger.error("❌ Lege of onvolledige rapportinhoud ontvangen (btc_summary ontbreekt)")
        return

    try:
        backup_path = f"./backups/daily_report_{today}.json"
        os.makedirs(os.path.dirname(backup_path), exist_ok=True)
        with open(backup_path, "w") as f:
            json.dump(report_data, f, indent=2, default=str)
        logger.info(f"🧾 Backup opgeslagen als {backup_path}")
    except Exception as e:
        logger.warning(f"⚠️ Backup json maken mislukt: {e}")

    logger.info("💾 Rapportinhoud gegenereerd. Opslaan...")
    save_report_to_db(today, report_data)
    return report_data
