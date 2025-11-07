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
# 🧾 Dagrapport genereren (DB-gedreven scores)
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

        # 1️⃣ AI-rapport genereren (haalt zelf live scores uit DB via scoring_utils)
        logger.info("🧠 Rapportgeneratie gestart...")
        full_report = generate_daily_report_sections("BTC")
        if not isinstance(full_report, dict):
            logger.error("❌ Ongeldige rapportstructuur (geen dict). Afgebroken.")
            return

        # ✅ Scores direct uit het gegenereerde rapport
        macro_score     = float(full_report.get("macro_score", 0) or 0)
        technical_score = float(full_report.get("technical_score", 0) or 0)
        setup_score     = float(full_report.get("setup_score", 0) or 0)
        market_score    = float(full_report.get("market_score", 0) or 0)

        # 2️⃣ Rapport opslaan in daily_reports (upsert op report_date)
        logger.info(f"💾 Dagrapport opslaan in daily_reports voor {today}")
        cursor.execute(
            """
            INSERT INTO daily_reports (
                report_date, btc_summary, macro_summary,
                setup_checklist, priorities, wyckoff_analysis,
                recommendations, conclusion, outlook,
                macro_score, technical_score, setup_score, market_score
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (report_date) DO UPDATE
            SET btc_summary      = EXCLUDED.btc_summary,
                macro_summary    = EXCLUDED.macro_summary,
                setup_checklist  = EXCLUDED.setup_checklist,
                priorities       = EXCLUDED.priorities,
                wyckoff_analysis = EXCLUDED.wyckoff_analysis,
                recommendations  = EXCLUDED.recommendations,
                conclusion       = EXCLUDED.conclusion,
                outlook          = EXCLUDED.outlook,
                macro_score      = EXCLUDED.macro_score,
                technical_score  = EXCLUDED.technical_score,
                setup_score      = EXCLUDED.setup_score,
                market_score     = EXCLUDED.market_score
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

        # 3️⃣ PDF genereren op basis van actuele rij
        cursor.execute("SELECT * FROM daily_reports WHERE report_date = %s LIMIT 1;", (today,))
        row = cursor.fetchone()
        if not row:
            logger.warning(f"⚠️ Geen rapport gevonden voor PDF voor {today}")
            return

        cols = [desc[0] for desc in cursor.description]
        report_dict = dict(zip(cols, row))

        pdf_buffer = generate_pdf_report(report_dict, report_type="daily")
        logger.info(f"🖨️ PDF gegenereerd voor {today}")

        # 4️⃣ E-mail versturen
        pdf_dir = os.path.join("static", "pdf", "daily")
        os.makedirs(pdf_dir, exist_ok=True)  # ✅ zorg dat het pad bestaat
        pdf_path = os.path.join(pdf_dir, f"daily_{today}.pdf")

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
