import os
import json
import logging
from datetime import datetime, timedelta
from pytz import timezone
from celery import shared_task

from backend.utils.db import get_db_connection

# Logging instellen
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def sanitize_field(val):
    if val is None:
        return ""
    if isinstance(val, (dict, list)):
        return str(val)
    return str(val)


def fetch_daily_reports_for_week():
    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen databaseverbinding bij ophalen daily reports.")
        return []

    try:
        today = datetime.now(timezone("UTC")).date()
        start_date = today - timedelta(days=7)

        with conn.cursor() as cur:
            cur.execute("""
                SELECT report_date, btc_summary, macro_summary, setup_checklist,
                       priorities, wyckoff_analysis, recommendations,
                       conclusion, outlook,
                       macro_score, technical_score, setup_score, sentiment_score
                FROM daily_reports
                WHERE report_date >= %s
                ORDER BY report_date ASC
            """, (start_date,))
            return cur.fetchall()
    except Exception as e:
        logger.error(f"❌ Fout bij ophalen daily reports: {e}")
        return []
    finally:
        conn.close()


def save_weekly_report_to_db(date, report_data):
    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen databaseverbinding beschikbaar.")
        return False

    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO weekly_reports (
                    report_date,
                    week_summary,
                    best_setup,
                    missed_opportunity,
                    ai_reflection,
                    outlook,
                    macro_score,
                    technical_score,
                    setup_score,
                    sentiment_score
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (report_date) DO UPDATE SET
                    week_summary = EXCLUDED.week_summary,
                    best_setup = EXCLUDED.best_setup,
                    missed_opportunity = EXCLUDED.missed_opportunity,
                    ai_reflection = EXCLUDED.ai_reflection,
                    outlook = EXCLUDED.outlook,
                    macro_score = EXCLUDED.macro_score,
                    technical_score = EXCLUDED.technical_score,
                    setup_score = EXCLUDED.setup_score,
                    sentiment_score = EXCLUDED.sentiment_score
            """, (
                date,
                report_data.get("week_summary"),
                report_data.get("best_setup"),
                report_data.get("missed_opportunity"),
                report_data.get("ai_reflection"),
                report_data.get("outlook"),
                report_data.get("macro_score"),
                report_data.get("technical_score"),
                report_data.get("setup_score"),
                report_data.get("sentiment_score"),
            ))
            conn.commit()
        logger.info("✅ Weekrapport succesvol opgeslagen in de database.")
        return True
    except Exception as e:
        logger.error(f"❌ Fout bij opslaan weekrapport: {e}")
        return False
    finally:
        conn.close()


@shared_task(name="backend.celery_task.weekly_report_task.generate_weekly_report")
def generate_weekly_report():
    logger.info("📅 Start genereren van weekrapport...")

    daily_reports = fetch_daily_reports_for_week()
    if not daily_reports:
        logger.warning("⚠️ Geen daily reports beschikbaar voor deze week.")
        return {"status": "no_data"}

    week_summary = "Overzicht van week:\n" + "\n\n".join(
        [f"{r[0]}:\n{sanitize_field(r[1])}" for r in daily_reports]
    )

    best_setup = "Setup A – breakout gaf +15% rendement op woensdag."
    missed_opportunity = "Setup C werd niet geactiveerd door lage volatiliteit, maar had potentieel."

    ai_reflection = (
        "Deze week was de RSI vaak oversold terwijl volume achterbleef. "
        "De breakout-strategieën werkten goed in combinatie met macro-bullish sentiment. "
        "Een fout was het onderschatten van DXY op woensdag. "
        "In de toekomst zouden we dat kunnen koppelen aan alertverhoging voor risico."
    )

    outlook = "Volgende week mogelijk voortzetting bullish trend zolang macro en volume dit ondersteunen."
    today = datetime.now(timezone("UTC")).date()

    # 🎯 Scores aggregeren
    def avg(index):
        values = [r[index] for r in daily_reports if r[index] is not None]
        return round(sum(values) / len(values)) if values else None

    report_data = {
        "week_summary": sanitize_field(week_summary),
        "best_setup": sanitize_field(best_setup),
        "missed_opportunity": sanitize_field(missed_opportunity),
        "ai_reflection": sanitize_field(ai_reflection),
        "outlook": sanitize_field(outlook),
        "macro_score": avg(9),
        "technical_score": avg(10),
        "setup_score": avg(11),
        "sentiment_score": avg(12),
    }

    try:
        backup_dir = "backend/backups"
        os.makedirs(backup_dir, exist_ok=True)
        backup_path = os.path.join(backup_dir, f"weekly_report_{today}.json")
        with open(backup_path, "w") as f:
            json.dump(report_data, f, indent=2)
        logger.info(f"🧾 Backup opgeslagen: {backup_path}")
    except Exception as e:
        logger.warning(f"⚠️ Backup json maken mislukt: {e}")

    success = save_weekly_report_to_db(today, report_data)

    return {
        "status": "ok" if success else "db_failed",
        "date": str(today),
        "records": len(daily_reports),
        "report_data": report_data
    }
