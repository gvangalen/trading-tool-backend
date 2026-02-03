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
# 🧠 WEEKLY REPORT TASK (NIEUWE ARCHITECTUUR)
# =====================================================

@shared_task(name="backend.celery_task.weekly_report_task.generate_weekly_report")
def generate_weekly_report(user_id: int):
    """
    Genereert een weekly report voor één user.

    Architectuur:
    - AI agent doet ALLE inhoud
    - Task orkestreert + slaat op
    - DB is single source of truth
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
                    weekly_market_review,
                    macro_context,
                    technical_structure,
                    setup_overview,
                    bot_activity,
                    strategic_implications,
                    outlook,

                    meta_json,
                    created_at,
                    updated_at
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
                    NOW(),
                    NOW()
                )
                ON CONFLICT (user_id, report_date)
                DO UPDATE SET
                    executive_summary      = EXCLUDED.executive_summary,
                    weekly_market_review   = EXCLUDED.weekly_market_review,
                    macro_context          = EXCLUDED.macro_context,
                    technical_structure    = EXCLUDED.technical_structure,
                    setup_overview         = EXCLUDED.setup_overview,
                    bot_activity           = EXCLUDED.bot_activity,
                    strategic_implications = EXCLUDED.strategic_implications,
                    outlook                = EXCLUDED.outlook,
                    meta_json              = EXCLUDED.meta_json,
                    updated_at             = NOW();
            """, (
                user_id,
                today,

                report.get("executive_summary"),
                report.get("weekly_market_review"),
                report.get("macro_context"),
                report.get("technical_structure"),
                report.get("setup_overview"),
                report.get("bot_activity"),
                report.get("strategic_implications"),
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
