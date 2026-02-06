import logging
import json
from datetime import date, timedelta

from celery import shared_task

from backend.utils.db import get_db_connection
from backend.ai_agents.quarterly_report_agent import (
    generate_quarterly_report_sections,
)

# =====================================================
# Logging
# =====================================================
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# =====================================================
# 🧠 Helpers
# =====================================================
def _get_quarter_period(d: date):
    """
    Bepaalt kwartaalperiode:
    - Q1: jan–mrt
    - Q2: apr–jun
    - Q3: jul–sep
    - Q4: okt–dec
    """
    quarter = (d.month - 1) // 3 + 1
    start_month = (quarter - 1) * 3 + 1
    end_month = start_month + 2

    period_start = date(d.year, start_month, 1)

    if end_month == 12:
        period_end = date(d.year, 12, 31)
    else:
        period_end = date(d.year, end_month + 1, 1) - timedelta(days=1)

    return period_start, period_end


# =====================================================
# 🧠 QUARTERLY REPORT TASK — CANONICAL
# =====================================================
@shared_task(
    name="backend.celery_task.quarterly_report_task.generate_quarterly_report"
)
def generate_quarterly_report(user_id: int):
    """
    Genereert een kwartaalrapport voor één user.

    Principes:
    - AI agent = content only
    - Task = periode + opslag
    - DB = single source of truth
    """

    logger.info("🟢 Start quarterly report generation (user_id=%s)", user_id)

    today = date.today()
    period_start, period_end = _get_quarter_period(today)

    # -------------------------------------------------
    # 1️⃣ AI AGENT
    # -------------------------------------------------
    report = generate_quarterly_report_sections(user_id=user_id)

    if not report or not isinstance(report, dict):
        logger.error("❌ Quarterly report agent gaf geen geldig resultaat")
        raise RuntimeError("Quarterly report agent failed")

    logger.info("✅ Quarterly report agent OK")

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
                INSERT INTO quarterly_reports (
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
            "✅ Quarterly report opgeslagen (user=%s, kwartaal=%s → %s)",
            user_id,
            period_start,
            period_end,
        )

    except Exception:
        conn.rollback()
        logger.exception("❌ Fout bij opslaan quarterly report")
        raise

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
