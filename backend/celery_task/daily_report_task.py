import os
import json
import logging
from dotenv import load_dotenv
from datetime import datetime
from pytz import timezone
from celery import shared_task

from backend.utils.db import get_db_connection
from backend.utils.ai_report_utils import generate_daily_report_sections
from backend.utils.json_utils import sanitize_json_input  # ✅ Consistente validatie

# ✅ .env laden (voor consistentie, niet strikt nodig hier)
load_dotenv()

# ✅ Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
dit werkt allemaal voor geen meter man ;x

def save_report_to_db(date, report_data, conn):
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

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Dagrapport geannuleerd: databaseverbinding faalt.")
        return

    today = datetime.now(timezone("UTC")).date()

    try:
        raw_data = generate_daily_report_sections(symbol="BTC")
        logger.info("🧠 AI-rapportsecties gegenereerd, begin met validatie...")
        report_data = sanitize_json_input(raw_data, context="generate_daily_report")  # ✅
    except Exception as e:
        logger.error(f"❌ Fout bij genereren rapportsecties: {e}")
        return

    if not report_data.get("btc_summary", "").strip():
        logger.error("❌ Lege of onvolledige rapportinhoud ontvangen (btc_summary ontbreekt of is leeg)")
        return

    try:
        backup_dir = os.path.join(os.getcwd(), "backups")
        os.makedirs(backup_dir, exist_ok=True)
        backup_path = os.path.join(backup_dir, f"daily_report_{today}.json")
        with open(backup_path, "w") as f:
            json.dump(report_data, f, indent=2, ensure_ascii=False, default=str)
        logger.info(f"🧾 Backup opgeslagen als {backup_path}")
    except Exception as e:
        logger.warning(f"⚠️ Backup json maken mislukt: {e}")

    logger.info("💾 Rapportinhoud gevalideerd. Start met opslaan...")
    save_success = save_report_to_db(today, report_data, conn)

    if save_success:
        logger.info("🎉 Dagrapport task succesvol afgerond.")
        return report_data
    else:
        logger.error("❌ Dagrapport kon niet worden opgeslagen.
