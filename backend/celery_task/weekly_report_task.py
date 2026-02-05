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
    ISO-week:
    - maandag = start
    - zondag  = einde
    """
    period_start = d - timedelta(days=d.weekday())
    period_end = period_start + timedelta(days=6)
    return period_start, period_end


# =====================================================
# 🧠 WEEKLY REPORT TASK — MATCHT DB SCHEMA
# =====================================================
@shared_task(name="backend.celery_task.weekly_report_task.generate_weekly_report")
def generate_weekly_report(user_id: int):
    """
    Genereert en slaat een weekly report op.

    DB is single source of truth.
    Deze task sluit EXACT aan op public.weekly_reports.
    """

    logger.info("🟢 Start weekly report generation (user_id=%s)", user_id)

    today = date.today()
    period_start, period_end = _get_week_period(today)

    # -------------------------------------------------
    # 1️⃣ AI AGENT — CONTENT ONLY
    # -------------------------------------------------
    report = generate_weekly_report_sections(user_id=user_id)

    if not report or not isinstance(report, dict):
        logger.error("❌ Weekly report agent gaf geen geldig resultaat")
        raise RuntimeError("Weekly report agent failed")

    logger.info(
        "✅ Weekly report agent OK, sections=%s",
        list(report.keys()),
    )

    # Fallback summary (vereist veld in tabel)
    summary = report.get("executive_summary") or report.get("outlook") or "Weekly market summary"

    # -------------------------------------------------
    # 2️⃣ OPSLAAN IN DATABASE (SCHEMA-EXACT)
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

                    summary,
                    executive_summary,
                    market_overview,
                    macro_trends,
                    technical_structure,
                    setup_performance,
                    bot_performance,
                    strategic_lessons,
                    outlook,

                    macro_score,
                    technical_score,
                    setup_score,

                    meta_json
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

                    %s,
                    %s,
                    %s,

                    %s
                )
                ON CONFLICT (user_id, report_date)
                DO UPDATE SET
                    period_start        = EXCLUDED.period_start,
                    period_end          = EXCLUDED.period_end,

                    summary             = EXCLUDED.summary,
                    executive_summary   = EXCLUDED.executive_summary,
                    market_overview     = EXCLUDED.market_overview,
                    macro_trends        = EXCLUDED.macro_trends,
                    technical_structure = EXCLUDED.technical_structure,
                    setup_performance   = EXCLUDED.setup_performance,
                    bot_performance     = EXCLUDED.bot_performance,
                    strategic_lessons   = EXCLUDED.strategic_lessons,
                    outlook             = EXCLUDED.outlook,

                    macro_score         = EXCLUDED.macro_score,
                    technical_score     = EXCLUDED.technical_score,
                    setup_score         = EXCLUDED.setup_score,

                    meta_json           = EXCLUDED.meta_json;
                """,
                (
                    user_id,
                    today,
                    period_start,
                    period_end,

                    summary,
                    report.get("executive_summary"),
                    report.get("market_overview"),
                    report.get("macro_trends"),
                    report.get("technical_structure"),
                    report.get("setup_performance"),
                    report.get("bot_performance"),
                    report.get("strategic_lessons"),
                    report.get("outlook"),

                    report.get("macro_score"),
                    report.get("technical_score"),
                    report.get("setup_score"),

                    json.dumps(report),
                ),
            )

        conn.commit()

        logger.info(
            "✅ Weekly report opgeslagen (user=%s, report_date=%s, week=%s → %s)",
            user_id,
            today,
            period_start,
            period_end,
        )

    finally:
        conn.close()

    return {
        "status": "ok",
        "user_id": user_id,
        "report_date": str(today),
        "period_start": str(period_start),
        "period_end": str(period_end),
        "sections": list(report.keys()),
    }
