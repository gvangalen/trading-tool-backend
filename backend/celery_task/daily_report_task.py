import os
import logging
from datetime import date
from celery import shared_task
from dotenv import load_dotenv

from backend.utils.db import get_db_connection
from backend.ai_agents.report_ai_agent import generate_daily_report_sections
from backend.utils.pdf_generator import generate_pdf_report
from backend.utils.email_utils import send_email_with_attachment

# =====================================================
# Logging
# =====================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)
load_dotenv()


# =====================================================
# 🧾 DAILY REPORT TASK (PER USER)
# =====================================================
@shared_task(name="backend.celery_task.daily_report_task.generate_daily_report")
def generate_daily_report(user_id: int):
    """
    Genereert dagelijks rapport per user:
    - gebruikt report_ai_agent (single source of truth)
    - slaat op in daily_reports
    - genereert PDF
    - verstuurt optioneel e-mail
    """

    today = date.today()
    logger.info(f"📄 Daily report task gestart | user_id={user_id} | {today}")

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen databaseverbinding")
        return

    cursor = None

    try:
        cursor = conn.cursor()

        # -------------------------------------------------
        # 1️⃣ REPORT GENEREREN (AI + DB)
        # -------------------------------------------------
        report = generate_daily_report_sections(symbol="BTC", user_id=user_id)

        if not isinstance(report, dict):
            logger.error("❌ Report agent gaf geen geldig dict terug")
            return

        scores = report.get("scores", {}) or {}

        macro_score     = scores.get("macro_score")
        technical_score = scores.get("technical_score")
        market_score    = scores.get("market_score")
        setup_score     = scores.get("setup_score")

        # -------------------------------------------------
        # 2️⃣ MASTER SCORE (OPTIONEEL)
        # -------------------------------------------------
        cursor.execute("""
            SELECT avg_score, trend, bias, risk, summary
            FROM ai_category_insights
            WHERE user_id = %s
              AND category = 'master'
              AND date = CURRENT_DATE
            LIMIT 1;
        """, (user_id,))

        row = cursor.fetchone()
        if row:
            ai_master_score, ai_master_trend, ai_master_bias, ai_master_risk, ai_master_summary = row
        else:
            ai_master_score = None
            ai_master_trend = None
            ai_master_bias = None
            ai_master_risk = None
            ai_master_summary = None

        # -------------------------------------------------
        # 3️⃣ OPSLAAN IN daily_reports (ZONDER updated_at)
        # -------------------------------------------------
        cursor.execute("""
            INSERT INTO daily_reports (
                report_date,
                user_id,

                btc_summary,
                macro_summary,
                setup_checklist,
                recommendations,
                outlook,

                macro_score,
                technical_score,
                setup_score,
                market_score,

                ai_master_score,
                ai_master_trend,
                ai_master_bias,
                ai_master_risk,
                ai_master_summary
            )
            VALUES (
                %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s, %s
            )
            ON CONFLICT (user_id, report_date)
            DO UPDATE SET
                btc_summary       = EXCLUDED.btc_summary,
                macro_summary     = EXCLUDED.macro_summary,
                setup_checklist   = EXCLUDED.setup_checklist,
                recommendations   = EXCLUDED.recommendations,
                outlook           = EXCLUDED.outlook,
                macro_score       = EXCLUDED.macro_score,
                technical_score   = EXCLUDED.technical_score,
                setup_score       = EXCLUDED.setup_score,
                market_score      = EXCLUDED.market_score,
                ai_master_score   = EXCLUDED.ai_master_score,
                ai_master_trend   = EXCLUDED.ai_master_trend,
                ai_master_bias    = EXCLUDED.ai_master_bias,
                ai_master_risk    = EXCLUDED.ai_master_risk,
                ai_master_summary = EXCLUDED.ai_master_summary;
        """, (
            today,
            user_id,

            report.get("executive_summary", ""),
            report.get("macro_summary", ""),
            report.get("setup_validation", ""),
            report.get("strategy_implication", ""),
            report.get("outlook", ""),
            
            macro_score,
            technical_score,
            setup_score,
            market_score,

            ai_master_score,
            ai_master_trend,
            ai_master_bias,
            ai_master_risk,
            ai_master_summary,
        ))

        conn.commit()
        logger.info(f"💾 daily_reports opgeslagen | user_id={user_id}")

        # -------------------------------------------------
        # 4️⃣ PDF GENEREREN
        # -------------------------------------------------
        cursor.execute("""
            SELECT *
            FROM daily_reports
            WHERE report_date = %s AND user_id = %s
            LIMIT 1;
        """, (today, user_id))

        row = cursor.fetchone()
        if not row:
            logger.warning("⚠️ Geen rapport gevonden voor PDF")
            return

        cols = [d[0] for d in cursor.description]
        report_row = dict(zip(cols, row))

        pdf_bytes = generate_pdf_report(report_row, report_type="daily")
        if not pdf_bytes:
            logger.error("❌ PDF generatie mislukt")
            return

        pdf_dir = os.path.join("static", "pdf", "daily")
        os.makedirs(pdf_dir, exist_ok=True)
        pdf_path = os.path.join(pdf_dir, f"daily_{today}_u{user_id}.pdf")

        with open(pdf_path, "wb") as f:
            f.write(pdf_bytes.getbuffer() if hasattr(pdf_bytes, "getbuffer") else pdf_bytes)

        logger.info(f"🖨️ PDF opgeslagen: {pdf_path}")

        # -------------------------------------------------
        # 5️⃣ EMAIL (OPTIONEEL)
        # -------------------------------------------------
        try:
            subject = f"📈 BTC Daily Report – {today}"
            body = (
                f"Dagelijks Bitcoin rapport voor {today}.\n\n"
                f"Macro: {macro_score}\n"
                f"Technical: {technical_score}\n"
                f"Market: {market_score}\n"
                f"Setup: {setup_score}\n\n"
                "Zie bijlage voor volledige analyse."
            )
            send_email_with_attachment(subject, body, pdf_path)
            logger.info("📤 Rapport per e-mail verzonden")
        except Exception:
            logger.warning("⚠️ E-mail verzenden mislukt", exc_info=True)

    except Exception:
        logger.error("❌ Fout in daily_report_task", exc_info=True)
        conn.rollback()

    finally:
        try:
            if cursor:
                cursor.close()
        except Exception:
            pass
        conn.close()
        logger.info(f"✅ Daily report task afgerond | user_id={user_id}")
