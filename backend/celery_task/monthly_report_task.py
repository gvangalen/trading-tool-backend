import logging
import json
from datetime import date

from celery import shared_task

from backend.utils.db import get_db_connection
from backend.ai_agents.monthly_report_agent import generate_monthly_report_sections

# =====================================================
# Logging
# =====================================================
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# =====================================================
# 🧠 MONTHLY REPORT TASK — CANONICAL ARCHITECTUUR
# =====================================================

@shared_task(name="backend.celery_task.monthly_report_task.generate_monthly_report")
def generate_monthly_report(user_id: int):
    """
    Genereert een maandrapport voor één user.

    Architectuur:
    - AI agent genereert ALLE inhoud
    - Task orkestreert + slaat op
    - DB is single source of truth
    - Canonieke kolomnamen (zelfde als weekly / quarterly)
    """

    logger.info("🟢 Start monthly report generation (user_id=%s)", user_id)

    # -------------------------------------------------
    # 1️⃣ AI AGENT
    # -------------------------------------------------
    report = generate_monthly_report_sections(user_id=user_id)

    if not report or not isinstance(report, dict):
        logger.error("❌ Monthly report agent gaf geen geldig resultaat")
        raise RuntimeError("Monthly report agent failed")

    # -------------------------------------------------
    # 2️⃣ OPSLAAN IN DATABASE
    # -------------------------------------------------
    conn = get_db_connection()
    if not conn:
        raise RuntimeError("Geen databaseverbinding beschikbaar")

    today = date.today()

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO monthly_reports (
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
                """,
                (
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

                    json.dumps(report),  # ✅ FIX
                ),
            )

        conn.commit()
        logger.info("✅ Monthly report opgeslagen (user=%s, date=%s)", user_id, today)

    finally:
        conn.close()

    return {
        "status": "ok",
        "user_id": user_id,
        "report_date": str(today),
        "keys": list(report.keys()),
    }
