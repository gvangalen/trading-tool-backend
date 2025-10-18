import os
import json
import logging
from datetime import datetime, timedelta
from pytz import timezone
from celery import shared_task

from backend.utils.db import get_db_connection

# === 🧩 Logging configuratie
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# =====================================================
# 🔧 Helpers
# =====================================================

def sanitize_field(val):
    """Zorgt dat alle velden veilig naar tekst worden omgezet."""
    if val is None:
        return ""
    if isinstance(val, (dict, list)):
        return json.dumps(val, ensure_ascii=False)
    return str(val)


def avg(values):
    """Gemiddelde van een lijst berekenen met fallback."""
    nums = [v for v in values if isinstance(v, (int, float))]
    return round(sum(nums) / len(nums), 2) if nums else 0


# =====================================================
# 📅 Data ophalen (weekly reports voor de maand)
# =====================================================

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
                SELECT report_date,
                       summary,               -- week samenvatting
                       macro_score, technical_score, setup_score, sentiment_score
                FROM weekly_reports
                WHERE report_date >= %s
                ORDER BY report_date ASC
            """, (start_date,))
            results = cur.fetchall()
            logger.info(f"📊 {len(results)} weekly reports gevonden in laatste 31 dagen.")
            return results

    except Exception as e:
        logger.error(f"❌ Fout bij ophalen weekly reports: {e}", exc_info=True)
        return []
    finally:
        conn.close()


# =====================================================
# 💾 Opslaan in database
# =====================================================

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
                    summary,
                    best_setup,
                    biggest_mistake,
                    ai_reflection,
                    outlook,
                    macro_score,
                    technical_score,
                    setup_score,
                    sentiment_score
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (report_date) DO UPDATE SET
                    summary = EXCLUDED.summary,
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
                report_data.get("summary"),
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
            logger.info(f"✅ Maandrapport succesvol opgeslagen of bijgewerkt ({date})")
            return True

    except Exception as e:
        logger.error(f"❌ Fout bij opslaan maandrapport: {e}", exc_info=True)
        return False
    finally:
        conn.close()


# =====================================================
# 🚀 Hoofd Celery-task
# =====================================================

@shared_task(name="backend.celery_task.monthly_report_task.generate_monthly_report")
def generate_monthly_report():
    logger.info("📆 Start genereren maandrapport...")

    # 1️⃣ Weekly reports ophalen
    weekly_reports = fetch_weekly_reports_for_month()
    if not weekly_reports:
        logger.warning("⚠️ Geen weekly reports beschikbaar voor deze maand.")
        return {"status": "no_data"}

    today = datetime.now(timezone("UTC")).date()

    # 2️⃣ Samenvatting en scores berekenen
    month_summary = "📅 Samenvatting van de maand:\n\n" + "\n\n".join(
        [f"{r[0]}:\n{sanitize_field(r[1])}" for r in weekly_reports]
    )

    macro_scores = [r[2] for r in weekly_reports]
    technical_scores = [r[3] for r in weekly_reports]
    setup_scores = [r[4] for r in weekly_reports]
    sentiment_scores = [r[5] for r in weekly_reports]

    report_data = {
        "summary": sanitize_field(month_summary),
        "best_setup": "Setup B werkte meerdere keren goed op momentum reversal.",
        "biggest_mistake": "Verkeerde inschatting van macro-data tijdens FOMC-week zorgde voor verlies.",
        "ai_reflection": (
            "De maand toonde hoge volatiliteit met sterke bullish ondertoon. "
            "Het combineren van technische breakouts met macro-data leverde de beste resultaten op. "
            "Toekomstige optimalisatie ligt in nauwkeurigere exit-strategieën en setupfiltering bij low volume."
        ),
        "outlook": "Volgende maand mogelijk consolidatie na sterke stijging – waakzaam voor omslag macro.",
        "macro_score": avg(macro_scores),
        "technical_score": avg(technical_scores),
        "setup_score": avg(setup_scores),
        "sentiment_score": avg(sentiment_scores),
    }

    # 3️⃣ JSON-backup
    try:
        backup_dir = "backend/backups"
        os.makedirs(backup_dir, exist_ok=True)
        backup_path = os.path.join(backup_dir, f"monthly_report_{today}.json")
        with open(backup_path, "w") as f:
            json.dump(report_data, f, indent=2, ensure_ascii=False)
        logger.info(f"🧾 Backup opgeslagen: {backup_path}")
    except Exception as e:
        logger.warning(f"⚠️ Backup JSON maken mislukt: {e}")

    # 4️⃣ Opslaan in database
    success = save_monthly_report_to_db(today, report_data)

    status = "ok" if success else "db_failed"
    logger.info(f"🏁 Maandrapport afgerond: {status.upper()} ({today})")

    return {
        "status": status,
        "date": str(today),
        "records": len(weekly_reports),
        "report_data": report_data
    }
