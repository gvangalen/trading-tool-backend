import os
import json
import logging
from datetime import datetime, timedelta
from pytz import timezone
from celery import shared_task

from backend.utils.db import get_db_connection

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def sanitize_field(val):
    if val is None:
        return ""
    if isinstance(val, (dict, list)):
        return str(val)
    return str(val)


def fetch_monthly_reports_for_quarter():
    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen databaseverbinding bij ophalen maandrapporten.")
        return []

    try:
        today = datetime.now(timezone("UTC")).date()
        start_date = today - timedelta(days=93)  # ca. 3 maanden

        with conn.cursor() as cur:
            cur.execute("""
                SELECT report_date, month_summary, best_setup,
                       biggest_mistake, ai_reflection, outlook
                FROM monthly_reports
                WHERE report_date >= %s
                ORDER BY report_date ASC
            """, (start_date,))
            return cur.fetchall()
    except Exception as e:
        logger.error(f"❌ Fout bij ophalen maandrapporten: {e}")
        return []
    finally:
        conn.close()


def save_quarterly_report_to_db(date, report_data):
    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen databaseverbinding beschikbaar.")
        return False

    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO quarterly_reports (
                    report_date,
                    quarter_summary,
                    top_performance,
                    major_mistake,
                    ai_reflection,
                    future_outlook
                ) VALUES (%s,%s,%s,%s,%s,%s)
                ON CONFLICT (report_date) DO UPDATE SET
                    quarter_summary = EXCLUDED.quarter_summary,
                    top_performance = EXCLUDED.top_performance,
                    major_mistake = EXCLUDED.major_mistake,
                    ai_reflection = EXCLUDED.ai_reflection,
                    future_outlook = EXCLUDED.future_outlook
            """, (
                date,
                report_data.get("quarter_summary"),
                report_data.get("top_performance"),
                report_data.get("major_mistake"),
                report_data.get("ai_reflection"),
                report_data.get("future_outlook"),
            ))
            conn.commit()
        logger.info("✅ Kwartaalrapport succesvol opgeslagen.")
        return True
    except Exception as e:
        logger.error(f"❌ Fout bij opslaan kwartaalrapport: {e}")
        return False
    finally:
        conn.close()


@shared_task(name="backend.celery_task.quarterly_report_task.generate_quarterly_report")
def generate_quarterly_report():
    logger.info("📊 Start genereren kwartaalrapport...")

    monthly_reports = fetch_monthly_reports_for_quarter()
    if not monthly_reports:
        logger.warning("⚠️ Geen maandrapporten beschikbaar voor deze periode.")
        return {"status": "no_data"}

    quarter_summary = "📆 Kwartaaloverzicht:\n\n" + "\n\n".join(
        [f"{r[0]}:\n{sanitize_field(r[1])}" for r in monthly_reports]
    )
    top_performance = "Setup X leverde in april en mei samen +25% rendement op."
    major_mistake = "In juni werd volume overschat tijdens CPI-week – trade mislukte."
    ai_reflection = (
        "Het kwartaal toonde een duidelijke bullish shift met toenemend volume. "
        "Setups op breakouts werkten vooral goed in combinatie met macrotrends. "
        "Een verbeterpunt is het tijdig afbouwen bij overextensie."
    )
    future_outlook = "Komend kwartaal lijkt correctie aannemelijk. Mogelijk range-periode met opleving richting eind september."

    today = datetime.now(timezone("UTC")).date()

    report_data = {
        "quarter_summary": sanitize_field(quarter_summary),
        "top_performance": sanitize_field(top_performance),
        "major_mistake": sanitize_field(major_mistake),
        "ai_reflection": sanitize_field(ai_reflection),
        "future_outlook": sanitize_field(future_outlook),
    }

    # 💾 JSON-backup in backend/backups
    try:
        backup_dir = "backend/backups"
        os.makedirs(backup_dir, exist_ok=True)
        backup_path = os.path.join(backup_dir, f"quarterly_report_{today}.json")
        with open(backup_path, "w") as f:
            json.dump(report_data, f, indent=2)
        logger.info(f"🧾 Backup opgeslagen: {backup_path}")
    except Exception as e:
        logger.warning(f"⚠️ Backup json maken mislukt: {e}")

    success = save_quarterly_report_to_db(today, report_data)

    return {
        "status": "ok" if success else "db_failed",
        "date": str(today),
        "records": len(monthly_reports),
        "report_data": report_data
    }
