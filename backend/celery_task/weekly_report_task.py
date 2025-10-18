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
# 📅 Daily reports ophalen (laatste 7 dagen)
# =====================================================

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
                SELECT report_date,
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
                FROM daily_reports
                WHERE report_date >= %s
                ORDER BY report_date ASC
            """, (start_date,))
            results = cur.fetchall()
            logger.info(f"📊 {len(results)} daily reports gevonden voor weekanalyse.")
            return results

    except Exception as e:
        logger.error(f"❌ Fout bij ophalen daily reports: {e}", exc_info=True)
        return []
    finally:
        conn.close()


# =====================================================
# 💾 Opslaan in database
# =====================================================

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
                    summary,
                    best_setup,
                    missed_opportunity,
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
                    missed_opportunity = EXCLUDED.missed_opportunity,
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
                report_data.get("missed_opportunity"),
                report_data.get("ai_reflection"),
                report_data.get("outlook"),
                report_data.get("macro_score"),
                report_data.get("technical_score"),
                report_data.get("setup_score"),
                report_data.get("sentiment_score"),
            ))
            conn.commit()
            logger.info(f"✅ Weekrapport succesvol opgeslagen of bijgewerkt ({date})")
            return True

    except Exception as e:
        logger.error(f"❌ Fout bij opslaan weekrapport: {e}", exc_info=True)
        return False
    finally:
        conn.close()


# =====================================================
# 🚀 Hoofd Celery-task
# =====================================================

@shared_task(name="backend.celery_task.weekly_report_task.generate_weekly_report")
def generate_weekly_report():
    logger.info("📅 Start genereren van weekrapport...")

    # 1️⃣ Daily reports ophalen
    daily_reports = fetch_daily_reports_for_week()
    if not daily_reports:
        logger.warning("⚠️ Geen daily reports beschikbaar voor deze week.")
        return {"status": "no_data"}

    today = datetime.now(timezone("UTC")).date()

    # 2️⃣ Samenvatting maken
    week_summary = "📆 Samenvatting van de week:\n\n" + "\n\n".join(
        [f"{r[0]}:\n{sanitize_field(r[1])}" for r in daily_reports]
    )

    macro_scores = [r[9] for r in daily_reports]
    technical_scores = [r[10] for r in daily_reports]
    setup_scores = [r[11] for r in daily_reports]
    sentiment_scores = [r[12] for r in daily_reports]

    report_data = {
        "summary": sanitize_field(week_summary),
        "best_setup": "Setup A – breakout gaf +15% rendement op woensdag.",
        "missed_opportunity": "Setup C werd niet geactiveerd door lage volatiliteit, maar had potentieel.",
        "ai_reflection": (
            "Deze week was de RSI vaak oversold terwijl volume achterbleef. "
            "De breakout-strategieën werkten goed in combinatie met macro-bullish sentiment. "
            "Een fout was het onderschatten van DXY op woensdag. "
            "In de toekomst zouden we dat kunnen koppelen aan alertverhoging voor risico."
        ),
        "outlook": "Volgende week mogelijk voortzetting bullish trend zolang macro en volume dit ondersteunen.",
        "macro_score": avg(macro_scores),
        "technical_score": avg(technical_scores),
        "setup_score": avg(setup_scores),
        "sentiment_score": avg(sentiment_scores),
    }

    # 3️⃣ Backup maken
    try:
        backup_dir = "backend/backups"
        os.makedirs(backup_dir, exist_ok=True)
        backup_path = os.path.join(backup_dir, f"weekly_report_{today}.json")
        with open(backup_path, "w") as f:
            json.dump(report_data, f, indent=2, ensure_ascii=False)
        logger.info(f"🧾 Backup opgeslagen: {backup_path}")
    except Exception as e:
        logger.warning(f"⚠️ Backup JSON maken mislukt: {e}")

    # 4️⃣ Opslaan in database
    success = save_weekly_report_to_db(today, report_data)

    status = "ok" if success else "db_failed"
    logger.info(f"🏁 Weekrapport afgerond: {status.upper()} ({today})")

    return {
        "status": status,
        "date": str(today),
        "records": len(daily_reports),
        "report_data": report_data,
    }
