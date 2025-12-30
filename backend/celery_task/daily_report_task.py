import os
import json
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
# 🔧 Helpers
# =====================================================
def safe_json(value, fallback=None):
    """
    Zorgt dat DB nooit dict/list krijgt.
    """
    if value is None:
        return fallback
    if isinstance(value, (dict, list)):
        return json.dumps(value)
    if isinstance(value, str):
        return value
    return json.dumps(str(value))


# =====================================================
# 🧾 DAILY REPORT TASK (PER USER)
# =====================================================
@shared_task(name="backend.celery_task.daily_report_task.generate_daily_report")
def generate_daily_report(user_id: int):
    """
    Genereert dagelijks rapport per user.

    - gebruikt report_ai_agent (single source of truth)
    - slaat exact de daily_reports structuur op
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
        # 1️⃣ REPORT GENEREREN (AI AGENT)
        # -------------------------------------------------
        report = generate_daily_report_sections(
            symbol="BTC",
            user_id=user_id
        )

        if not isinstance(report, dict):
            logger.error("❌ Report agent gaf geen geldig dict terug")
            return

        # -------------------------------------------------
        # 2️⃣ EXTRACT & NORMALISEER
        # -------------------------------------------------
        executive_summary    = safe_json(report.get("executive_summary"))
        macro_context        = safe_json(report.get("macro_context"))
        setup_validation     = safe_json(report.get("setup_validation"))
        strategy_implication = safe_json(report.get("strategy_implication"))
        outlook              = safe_json(report.get("outlook"))

        price       = report.get("price")
        change_24h  = report.get("change_24h")
        volume      = report.get("volume")

        indicator_highlights = safe_json(
            report.get("indicator_highlights", [])
        )

        macro_score     = report.get("macro_score")
        technical_score = report.get("technical_score")
        market_score    = report.get("market_score")
        setup_score     = report.get("setup_score")

        # -------------------------------------------------
        # 3️⃣ OPSLAAN IN daily_reports
        # -------------------------------------------------
        cursor.execute("""
            INSERT INTO daily_reports (
                report_date,
                user_id,

                executive_summary,
                macro_context,
                setup_validation,
                strategy_implication,
                outlook,

                price,
                change_24h,
                volume,
                indicator_highlights,

                macro_score,
                technical_score,
                market_score,
                setup_score
            )
            VALUES (
                %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s
            )
            ON CONFLICT (user_id, report_date)
            DO UPDATE SET
                executive_summary    = EXCLUDED.executive_summary,
                macro_context        = EXCLUDED.macro_context,
                setup_validation     = EXCLUDED.setup_validation,
                strategy_implication = EXCLUDED.strategy_implication,
                outlook              = EXCLUDED.outlook,

                price                = EXCLUDED.price,
                change_24h           = EXCLUDED.change_24h,
                volume               = EXCLUDED.volume,
                indicator_highlights = EXCLUDED.indicator_highlights,

                macro_score          = EXCLUDED.macro_score,
                technical_score      = EXCLUDED.technical_score,
                market_score         = EXCLUDED.market_score,
                setup_score          = EXCLUDED.setup_score;
        """, (
            today,
            user_id,

            executive_summary,
            macro_context,
            setup_validation,
            strategy_implication,
            outlook,

            price,
            change_24h,
            volume,
            indicator_highlights,

            macro_score,
            technical_score,
            market_score,
            setup_score,
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
        pdf_path = os.path.join(
            pdf_dir,
            f"daily_{today}_u{user_id}.pdf"
        )

        with open(pdf_path, "wb") as f:
            f.write(
                pdf_bytes.getbuffer()
                if hasattr(pdf_bytes, "getbuffer")
                else pdf_bytes
            )

        logger.info(f"🖨️ PDF opgeslagen: {pdf_path}")

        # -------------------------------------------------
        # 5️⃣ EMAIL (OPTIONEEL)
        # -------------------------------------------------
        try:
            subject = f"📈 BTC Daily Report – {today}"
            body = (
                f"Dagelijks Bitcoin rapport voor {today}.\n\n"
                f"Macro score: {macro_score}\n"
                f"Technical score: {technical_score}\n"
                f"Market score: {market_score}\n"
                f"Setup score: {setup_score}\n\n"
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
