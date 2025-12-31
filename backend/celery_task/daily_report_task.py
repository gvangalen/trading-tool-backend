import os
import json
import logging
from datetime import date
from decimal import Decimal

from celery import shared_task
from dotenv import load_dotenv
from psycopg2.extras import Json

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
# Helpers
# =====================================================
def to_float(v):
    if v is None:
        return None
    if isinstance(v, Decimal):
        return float(v)
    try:
        return float(v)
    except Exception:
        return None


def jsonb(v, fallback=None):
    """
    ✅ Altijd veilige psycopg2 json adapter voor jsonb kolommen.
    - dict/list → Json(dict/list)
    - string/number/bool → Json(value)
    - None → Json(fallback) of None
    """
    if v is None:
        return Json(fallback) if fallback is not None else None

    # Als het al dict/list is → direct
    if isinstance(v, (dict, list)):
        return Json(v)

    # Als het string is → proberen JSON te parsen, anders als string opslaan (jsonb string)
    if isinstance(v, str):
        s = v.strip()
        if (s.startswith("{") and s.endswith("}")) or (s.startswith("[") and s.endswith("]")):
            try:
                return Json(json.loads(s))
            except Exception:
                pass
        return Json(v)

    # numbers/bool/whatever
    return Json(v)


def pick_market(report: dict) -> dict:
    """
    Nieuwe agent geeft vaak market_data; fallback naar oude keys.
    """
    m = report.get("market_data")
    if isinstance(m, dict):
        return m

    # fallback oude structuur
    return {
        "price": report.get("price"),
        "change_24h": report.get("change_24h"),
        "volume": report.get("volume"),
    }


def pick_scores(report: dict) -> dict:
    """
    Nieuwe agent geeft scores dict; fallback naar oude keys.
    """
    s = report.get("scores")
    if isinstance(s, dict):
        return s

    return {
        "macro_score": report.get("macro_score"),
        "technical_score": report.get("technical_score"),
        "market_score": report.get("market_score"),
        "setup_score": report.get("setup_score"),
    }


# =====================================================
# 🧾 DAILY REPORT TASK (PER USER)
# =====================================================
@shared_task(name="backend.celery_task.daily_report_task.generate_daily_report")
def generate_daily_report(user_id: int):
    """
    Genereert daily report per user.

    - gebruikt report_ai_agent (single source of truth)
    - slaat exact daily_reports structuur op (jsonb)
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
        report = generate_daily_report_sections(user_id=user_id)

        if not isinstance(report, dict):
            logger.error("❌ Report agent gaf geen geldig dict terug")
            return

        # -------------------------------------------------
        # 2️⃣ NORMALISEER (nieuw schema)
        # -------------------------------------------------
        market = pick_market(report)
        scores = pick_scores(report)

        # jsonb secties (kunnen string of dict zijn → jsonb() regelt dat)
        executive_summary    = jsonb(report.get("executive_summary"), fallback={})
        macro_context        = jsonb(report.get("macro_context"), fallback={})
        setup_validation     = jsonb(report.get("setup_validation"), fallback={})
        strategy_implication = jsonb(report.get("strategy_implication"), fallback={})
        outlook              = jsonb(report.get("outlook"), fallback={})

        # market fields
        price      = to_float(market.get("price"))
        change_24h = to_float(market.get("change_24h"))
        volume     = to_float(market.get("volume"))

        # indicator highlights jsonb
        indicator_highlights = jsonb(report.get("indicator_highlights") or [], fallback=[])

        # scores
        macro_score     = to_float(scores.get("macro_score") or scores.get("macro"))
        technical_score = to_float(scores.get("technical_score") or scores.get("technical"))
        market_score    = to_float(scores.get("market_score") or scores.get("market"))
        setup_score     = to_float(scores.get("setup_score") or scores.get("setup"))

        # -------------------------------------------------
        # 3️⃣ OPSLAAN IN daily_reports (jsonb compatible)
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
        # 4️⃣ PDF GENEREREN (uit DB row)
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

        # generate_pdf_report returnt BytesIO
        pdf_buffer = generate_pdf_report(report_row, report_type="daily", save_to_disk=False)
        if not pdf_buffer:
            logger.error("❌ PDF generatie mislukt")
            return

        pdf_dir = os.path.join("static", "pdf", "daily")
        os.makedirs(pdf_dir, exist_ok=True)
        pdf_path = os.path.join(pdf_dir, f"daily_{today}_u{user_id}.pdf")

        with open(pdf_path, "wb") as f:
            f.write(pdf_buffer.getvalue())

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
        try:
            conn.rollback()
        except Exception:
            pass

    finally:
        try:
            if cursor:
                cursor.close()
        except Exception:
            pass
        conn.close()
        logger.info(f"✅ Daily report task afgerond | user_id={user_id}")
