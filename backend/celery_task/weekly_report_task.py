import logging
import json
from datetime import date, timedelta

from celery import shared_task

from backend.utils.db import get_db_connection
from backend.ai_agents.weekly_report_agent import generate_weekly_report_sections

# =====================================================
# Logging
# =====================================================
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# =====================================================
# 🧠 Helpers
# =====================================================
def _get_week_period(d: date):
    """
    ISO week:
    - maandag = start
    - zondag  = einde
    """
    period_start = d - timedelta(days=d.weekday())
    period_end = period_start + timedelta(days=6)
    return period_start, period_end


# =====================================================
# 🧠 WEEKLY REPORT TASK — CANONICAL
# =====================================================
@shared_task(name="backend.celery_task.weekly_report_task.generate_weekly_report")
def generate_weekly_report(user_id: int):
    """
    Genereert een weekly report voor één user.

    Principes:
    - AI agent = content only
    - Task = periode + opslag
    - DB = single source of truth
    """

    logger.info("🟢 Start weekly report generation (user_id=%s)", user_id)

    today = date.today()
    period_start, period_end = _get_week_period(today)

    # -------------------------------------------------
    # 1️⃣ AI AGENT
    # -------------------------------------------------
    report = generate_weekly_report_sections(user_id=user_id)

    if not report or not isinstance(report, dict):
        logger.error("❌ Weekly report agent gaf geen geldig resultaat")
        raise RuntimeError("Weekly report agent failed")

    logger.info(
        "✅ Weekly report agent OK, sections=%s",
        list(report.keys()),
    )

    # -------------------------------------------------
    # 2️⃣ OPSLAAN IN DATABASE
    # -------------------------------------------------
    conn = get_db_connection()
    if not conn:
        raise RuntimeError("Geen databaseverbinding beschikbaar")

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO weekly_reports (
                    user_id,
                    report_date,
                    period_start,
                    period_end,

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
                )
                VALUES (
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
                    %s,

                    %s,
                    NOW()
                )
                ON CONFLICT (user_id, period_start)
                DO UPDATE SET
                    report_date         = EXCLUDED.report_date,
                    period_end          = EXCLUDED.period_end,

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
                    period_start,
                    period_end,

                    report.get("executive_summary"),
                    report.get("market_overview"),
                    report.get("macro_trends"),
                    report.get("technical_structure"),
                    report.get("setup_performance"),
                    report.get("bot_performance"),
                    report.get("strategic_lessons"),
                    report.get("outlook"),

                    json.dumps(report),
                ),
            )

        conn.commit()

        logger.info(
            "✅ Weekly report opgeslagen (user=%s, week=%s → %s)",
            user_id,
            period_start,
            period_end,
        )

    finally:
        conn.close()

    return {
        "status": "ok",
        "user_id": user_id,
        "period_start": str(period_start),
        "period_end": str(period_end),
        "sections": list(report.keys()),
    }
    }
