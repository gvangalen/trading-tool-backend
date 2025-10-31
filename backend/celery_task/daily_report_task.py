import os
import logging
from datetime import datetime
from celery import shared_task
from dotenv import load_dotenv

from backend.utils.db import get_db_connection
from backend.utils.ai_report_utils import generate_daily_report_sections
from backend.utils.pdf_generator import generate_pdf_report
from backend.utils.email_utils import send_email_with_attachment

# === Logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
load_dotenv()

# =====================================================
# 🧠 Hulpfunctie: scores ophalen uit daily_scores
# =====================================================
def get_scores_from_db():
    """Haalt de meest recente dag-scores op uit daily_scores."""
    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen databaseverbinding voor get_scores_from_db()")
        return {}

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT macro_score, technical_score, setup_score, market_score
                FROM daily_scores
                ORDER BY report_date DESC
                LIMIT 1
            """)
            row = cur.fetchone()
            if not row:
                logger.warning("⚠️ Geen scores gevonden in daily_scores.")
                return {}

            return {
                "macro_score": float(row[0]) if row[0] is not None else 0,
                "technical_score": float(row[1]) if row[1] is not None else 0,
                "setup_score": float(row[2]) if row[2] is not None else 0,
                "market_score": float(row[4]) if row[4] is not None else 0,
            }
    except Exception as e:
        logger.error(f"❌ Fout bij ophalen van scores: {e}")
        return {}
    finally:
        conn.close()

# =====================================================
# 🧾 Dagrapport genereren
# =====================================================
@shared_task(name="backend.celery_task.daily_report_task.generate_daily_report")
def generate_daily_report():
    logger.info("🔄 Dagrapport-task gestart")

    today = datetime.now().date()
    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen databaseverbinding bij rapportgeneratie")
        return

    try:
        cursor = conn.cursor()

        # 1️⃣ Scores ophalen uit daily_scores
        scores = get_scores_from_db()
        if not scores:
            logger.warning("⚠️ Geen scores beschikbaar — rapport wordt wel aangemaakt, maar scores = 0")

        macro_score = scores.get("macro_score", 0)
        technical_score = scores.get("technical_score", 0)
        setup_score = scores.get("setup_score", 0)
        market_score = scores.get("market_score", 0)

        # 2️⃣ AI-rapport genereren (gebruikt o.a. scores en marktdata)
        logger.info("🧠 Rapportgeneratie gestart...")
        full_report = generate_daily_report_sections("BTC")

        if not isinstance(full_report, dict):
            logger.error("❌ Ongeldige rapportstructuur (geen dict). Afgebroken.")
            return

        # 3️⃣ Rapport opslaan in daily_reports
        logger.info(f"💾 Dagrapport opslaan in daily_reports voor {today}")
        cursor.execute(
            """
            INSERT INTO daily_reports (
                report_date, btc_summary, macro_summary,
                setup_checklist, priorities, wyckoff_analysis,
                recommendations, conclusion, outlook,
                macro_score, technical_score, setup_score, market_score
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (report_date) DO UPDATE
            SET btc_summary = EXCLUDED.btc_summary,
                macro_summary = EXCLUDED.macro_summary,
                setup_checklist = EXCLUDED.setup_checklist,
                priorities = EXCLUDED.priorities,
                wyckoff_analysis = EXCLUDED.wyckoff_analysis,
                recommendations = EXCLUDED.recommendations,
                conclusion = EXCLUDED.conclusion,
                outlook = EXCLUDED.outlook,
                macro_score = EXCLUDED.macro_score,
                technical_score = EXCLUDED.technical_score,
                setup_score = EXCLUDED.setup_score,
                sentiment_score = EXCLUDED.sentiment_score,
                market_score = EXCLUDED.market_score
            """,
            (
                today,
                full_report.get("btc_summary", ""),
                full_report.get("macro_summary", ""),
                full_report.get("setup_checklist", ""),
                full_report.get("priorities", ""),
                full_report.get("wyckoff_analysis", ""),
                full_report.get("recommendations", ""),
                full_report.get("conclusion", ""),
                full_report.get("outlook", ""),
                macro_score, technical_score, setup_score, market_score
            )
        )
        conn.commit()

        # 4️⃣ PDF genereren
        cursor.execute("SELECT * FROM daily_reports WHERE report_date = %s LIMIT 1;", (today,))
        row = cursor.fetchone()
        if not row:
            logger.warning(f"⚠️ Geen rapport gevonden voor PDF voor {today}")
            return

        cols = [desc[0] for desc in cursor.description]
        report_dict = dict(zip(cols, row))
        pdf_buffer = generate_pdf_report(report_dict, report_type="daily")
        logger.info(f"🖨️ PDF gegenereerd voor {today}")

        # 5️⃣ E-mail versturen
        pdf_path = os.path.join("static", "pdf", "daily", f"daily_{today}.pdf")

        # Extra prijsinformatie uit rapport (optioneel)
        market_data = full_report.get("market_data", {})
        price = market_data.get("price", "–")
        volume = market_data.get("volume", "–")
        change_24h = market_data.get("change_24h", "–")

        try:
            subject = f"📈 BTC Daily Report – {today}"
            body = (
                f"Hierbij het automatisch gegenereerde dagelijkse Bitcoin rapport voor {today}.\n\n"
                f"Huidige prijs: ${price} | Volume: {volume} | 24u verandering: {change_24h}%\n\n"
                "Bekijk de belangrijkste samenvatting, Wyckoff-analyse en strategieën in de bijlage."
            )
            send_email_with_attachment(subject, body, pdf_path)
            logger.info(f"📤 Dagrapport verzonden via e-mail ({pdf_path})")
        except Exception as e:
            logger.error(f"❌ Fout bij verzenden van e-mail: {e}", exc_info=True)

    except Exception as e:
        logger.error(f"❌ Fout tijdens rapportgeneratie: {e}", exc_info=True)
    finally:
        conn.close()
        logger.info(f"✅ Dagrapport voltooid voor {today}")
