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


def fetch_weekly_reports_for_month():
    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen databaseverbinding bij ophalen weekly reports.")
        return []

    try:
        today = datetime.now(timezone("UTC")).date()
        start_date = today - timedelta(days=31)

        with conn.cursor() as cur:
            cur.execute("""
                SELECT report_date, week_summary, best_setup,
                       missed_opportunity, ai_reflection, outlook,
                       macro_score, technical_score, setup_score, sentiment_score
                FROM weekly_reports
                WHERE report_date >= %s
                ORDER BY report_date ASC
            """, (start_date,))
            return cur.fetchall()
    except Exception as e:
        logger.error(f"❌ Fout bij ophalen weekly reports: {e}")
        return []
    finally:
        conn.close()


def save_monthly_report_to_db(date, report_data):
    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen databaseverbinding beschikbaar.")
        return False

    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO monthly_reports (
                    report_date,
                    month_summary,
                    best_setup,
                    biggest_mistake,
                    ai_reflection,
                    outlook,
                    macro_score,
                    technical_score,
                    setup_score,
                    sentiment_score
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (report_date) DO UPDATE SET
                    month_summary = EXCLUDED.month_summary,
                    best_setup = EXCLUDED.best_setup,
                    biggest_mistake = EXCLUDED.biggest_mistake,
                    ai_reflection = EXCLUDED.ai_reflection,
                    outlook = EXCLUDED.outlook,
                    macro_score = EXCLUDED.macro_score,
                    technical_score = EXCLUDED.technical_score,
                    setup_score = EXCLUDED.setup_score,
                    sentiment_score = EXCLUDED.sentiment_score
            """, (
                date,
                report_data.get("month_summary"),
                report_data.get("best_setup"),
                report_data.get("biggest_mistake"),
                report_data.get("ai_reflection"),
                report_data.get("outlook"),
                report_data.get("macro_score"),
                report_data.get("technical_score"),
                report_data.get("setup_score"),
                report_data.get("sentiment_score"),
            ))
            conn.commit()
        logger.info("✅ Maandrapport succesvol opgeslagen.")
        return True
    except Exception as e:
        logger.error(f"❌ Fout bij opslaan maandrapport: {e}")
        return False
    finally:
        conn.close()


@shared_task(name="backend.celery_task.monthly_report_task.generate_monthly_report")
def generate_monthly_report():
    logger.info("📆 Start genereren maandrapport...")

    weekly_reports = fetch_weekly_reports_for_month()
    if not weekly_reports:
        logger.warning("⚠️ Geen weekly reports beschikbaar.")
        return {"status": "no_data"}

    month_summary = "📅 Samenvatting van de maand:\n\n" + "\n\n".join(
        [f"{r[0]}:\n{sanitize_field(r[1])}" for r in weekly_reports]
    )

    best_setup = "Setup B werkte meerdere keren goed op momentum reversal."
    biggest_mistake = "Verkeerde inschatting van macro-data tijdens FOMC week zorgde voor verlies."

    ai_reflection = (
        "De maand toonde hoge volatiliteit met sterke bullish ondertoon. "
        "Het combineren van technische breakouts met macro-data leverde de beste resultaten op. "
        "Toekomstige optimalisatie ligt in nauwkeurigere exit-strategieën en setupfiltering bij low volume."
    )

    outlook = "Volgende maand mogelijk consolidatie na sterke stijging – waakzaam voor omslag macro."
    today = datetime.now(timezone("UTC")).date()

    # 🎯 Gemiddelde scores berekenen
    def avg(index):
        values = [r[index] for r in weekly_reports if r[index] is not None]
        return round(sum(values) / len(values)) if values else None

    report_data = {
        "month_summary": sanitize_field(month_summary),
        "best_setup": sanitize_field(best_setup),
        "biggest_mistake": sanitize_field(biggest_mistake),
        "ai_reflection": sanitize_field(ai_reflection),
        "outlook": sanitize_field(outlook),
        "macro_score": avg(6),
        "technical_score": avg(7),
        "setup_score": avg(8),
        "sentiment_score": avg(9),
    }

    # 💾 JSON-backup
    try:
        backup_dir = "backend/backups"
        os.makedirs(backup_dir, exist_ok=True)
        backup_path = os.path.join(backup_dir, f"monthly_report_{today}.json")
        with open(backup_path, "w") as f:
            json.dump(report_data, f, indent=2)
        logger.info(f"🧾 Backup opgeslagen: {backup_path}")
    except Exception as e:
        logger.warning(f"⚠️ Backup json maken mislukt: {e}")

    success = save_monthly_report_to_db(today, report_data)

    return {
        "status": "ok" if success else "db_failed",
        "date": str(today),
        "records": len(weekly_reports),
        "report_data": report_data
    }
