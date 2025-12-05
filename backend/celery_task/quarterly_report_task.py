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
# 📅 Maandrapporten ophalen voor HET KWARTAAL — PER USER
# =====================================================

def fetch_monthly_reports_for_quarter(user_id: int):
    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen databaseverbinding bij ophalen maandrapporten.")
        return []

    try:
        today = datetime.now(timezone("UTC")).date()
        start_date = today - timedelta(days=93)  # ± drie maanden

        with conn.cursor() as cur:
            cur.execute("""
                SELECT report_date,
                       summary,
                       macro_score,
                       technical_score,
                       setup_score,
                       market_score
                FROM monthly_reports
                WHERE report_date >= %s
                  AND user_id = %s
                ORDER BY report_date ASC
            """, (start_date, user_id))

            results = cur.fetchall()
            logger.info(f"📊 {len(results)} maandrapporten gevonden voor kwartaalanalyse (user={user_id}).")
            return results

    except Exception as e:
        logger.error(f"❌ Fout bij ophalen maandrapporten: {e}", exc_info=True)
        return []
    finally:
        conn.close()


# =====================================================
# 💾 Opslaan in quarterly_reports — PER USER
# =====================================================

def save_quarterly_report_to_db(date, report_data, user_id: int):
    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen databaseverbinding beschikbaar.")
        return False

    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO quarterly_reports (
                    report_date,
                    user_id,
                    summary,
                    top_performance,
                    major_mistake,
                    ai_reflection,
                    outlook,
                    macro_score,
                    technical_score,
                    setup_score,
                    market_score
                ) VALUES (%s, %s,
                          %s, %s, %s, %s, %s,
                          %s, %s, %s, %s)
                ON CONFLICT (report_date, user_id) DO UPDATE SET
                    summary = EXCLUDED.summary,
                    top_performance = EXCLUDED.top_performance,
                    major_mistake = EXCLUDED.major_mistake,
                    ai_reflection = EXCLUDED.ai_reflection,
                    outlook = EXCLUDED.outlook,
                    macro_score = EXCLUDED.macro_score,
                    technical_score = EXCLUDED.technical_score,
                    setup_score = EXCLUDED.setup_score,
                    market_score = EXCLUDED.market_score
            """, (
                date,
                user_id,
                report_data.get("summary"),
                report_data.get("top_performance"),
                report_data.get("major_mistake"),
                report_data.get("ai_reflection"),
                report_data.get("outlook"),
                report_data.get("macro_score"),
                report_data.get("technical_score"),
                report_data.get("setup_score"),
                report_data.get("market_score"),
            ))

            conn.commit()
            logger.info(f"✅ Kwartaalrapport opgeslagen of bijgewerkt ({date}, user={user_id})")
            return True

    except Exception as e:
        logger.error(f"❌ Fout bij opslaan kwartaalrapport: {e}", exc_info=True)
        return False
    finally:
        conn.close()


# =====================================================
# 🚀 Hoofd Celery-task — draait per user
# =====================================================

@shared_task(name="backend.celery_task.quarterly_report_task.generate_quarterly_report")
def generate_quarterly_report(user_id: int):
    logger.info(f"📊 Start genereren kwartaalrapport voor user_id={user_id}...")

    # 1️⃣ Maandrapporten ophalen
    monthly_reports = fetch_monthly_reports_for_quarter(user_id=user_id)
    if not monthly_reports:
        logger.warning(f"⚠️ Geen maandrapporten beschikbaar voor dit kwartaal (user={user_id}).")
        return {"status": "no_data", "user_id": user_id}

    today = datetime.now(timezone("UTC")).date()

    # 2️⃣ Samenvatting & scoreberekening
    quarter_summary = "📆 Kwartaaloverzicht:\n\n" + "\n\n".join(
        [f"{r[0]}:\n{sanitize_field(r[1])}" for r in monthly_reports]
    )

    macro_scores     = [r[2] for r in monthly_reports]
    technical_scores = [r[3] for r in monthly_reports]
    setup_scores     = [r[4] for r in monthly_reports]
    market_scores    = [r[5] for r in monthly_reports]

    report_data = {
        "summary": sanitize_field(quarter_summary),
        "top_performance": "Setup X leverde sterke rendementen in dit kwartaal.",
        "major_mistake": "Verkeerde inschatting van volatiliteit tijdens CPI/FOMC week.",
        "ai_reflection": (
            "Het kwartaal toonde structureel bullish momentum. "
            "Breakouts werkten goed wanneer macro-data dit ondersteunden. "
            "Risk management blijft verbeterpunt, vooral bij low-volume omstandigheden."
        ),
        "outlook": "Komend kwartaal waarschijnlijk consolidatie; macro blijft bepalend.",
        "macro_score": avg(macro_scores),
        "technical_score": avg(technical_scores),
        "setup_score": avg(setup_scores),
        "market_score": avg(market_scores),
    }

    # 3️⃣ Backup (per user)
    try:
        backup_dir = "backend/backups/quarterly"
        os.makedirs(backup_dir, exist_ok=True)
        backup_path = os.path.join(backup_dir, f"quarterly_report_{today}_u{user_id}.json")
        with open(backup_path, "w") as f:
            json.dump(report_data, f, indent=2, ensure_ascii=False)
        logger.info(f"🧾 Backup opgeslagen: {backup_path}")
    except Exception as e:
        logger.warning(f"⚠️ Backup JSON mislukt voor user {user_id}: {e}")

    # 4️⃣ Opslaan in DB
    success = save_quarterly_report_to_db(today, report_data, user_id=user_id)

    status = "ok" if success else "db_failed"
    logger.info(f"🏁 Kwartaalrapport afgerond ({today}) voor user {user_id} → {status.upper()}")

    return {
        "status": status,
        "date": str(today),
        "user_id": user_id,
        "records": len(monthly_reports),
        "report_data": report_data,
    }
