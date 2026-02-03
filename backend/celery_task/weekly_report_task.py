import logging
from datetime import date

from celery import shared_task

from backend.utils.db import get_db_connection
from backend.ai_agents.weekly_report_agent import generate_weekly_report_sections

# =====================================================
# Logging
# =====================================================
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# =====================================================
# 🧠 WEEKLY REPORT TASK — CANONICAL ARCHITECTUUR
# =====================================================

@shared_task(name="backend.celery_task.weekly_report_task.generate_weekly_report")
def generate_weekly_report(user_id: int):
    """
    Genereert een weekly report voor één user.

    Architectuur:
    - AI agent genereert ALLE inhoud
    - Task orkestreert + slaat op
    - DB is single source of truth
    - Canonieke kolomnamen (zelfde als monthly / quarterly)
    """

    logger.info("🟢 Start weekly report generation (user_id=%s)", user_id)

    # -------------------------------------------------
    # 1️⃣ AI AGENT
    # -------------------------------------------------
    report = generate_weekly_report_sections(user_id=user_id)

    if not report or not isinstance(report, dict):
        logger.error("❌ Weekly report agent gaf geen geldig resultaat")
        raise RuntimeError("Weekly report agent failed")

    # -------------------------------------------------
    # 2️⃣ OPSLAAN IN DATABASE
    # -------------------------------------------------
    conn = get_db_connection()
    if not conn:
        raise RuntimeError("Geen databaseverbinding beschikbaar")

    today = date.today()

    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO weekly_reports (
                    user_id,
                    report_date,

                    executive_summary,
                    market_overview,
                    macro_trends,
                    technical_structure,
                    setup_performance,
                    bot_performance,
                    strategic_lessons,
                    outlook,

                    meta_json,
                    created_at
                ) VALUES (
                    %s,
                    %s,

                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,

                    %s,
                    NOW()
                )
                ON CONFLICT (user_id, report_date)
                DO UPDATE SET
                    executive_summary   = EXCLUDED.executive_summary,
                    market_overview     = EXCLUDED.market_overview,
                    macro_trends        = EXCLUDED.macro_trends,
                    technical_structure = EXCLUDED.technical_structure,
                    setup_performance   = EXCLUDED.setup_performance,
                    bot_performance     = EXCLUDED.bot_performance,
                    strategic_lessons   = EXCLUDED.strategic_lessons,
                    outlook             = EXCLUDED.outlook,
                    meta_json           = EXCLUDED.meta_json;
            """, (
                user_id,
                today,

                report.get("executive_summary"),
                report.get("market_overview"),
                report.get("macro_trends"),
                report.get("technical_structure"),
                report.get("setup_performance"),
                report.get("bot_performance"),
                report.get("strategic_lessons"),
                report.get("outlook"),

                report
            ))

        conn.commit()
        logger.info("✅ Weekly report opgeslagen (user=%s, date=%s)", user_id, today)

    finally:
        conn.close()

    return {
        "status": "ok",
        "user_id": user_id,
        "report_date": str(today),
        "keys": list(report.keys()),
    }
