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
# 🧾 Dagrapport genereren (DB- + AI-agent gedreven)
# =====================================================
@shared_task(name="backend.celery_task.daily_report_task.generate_daily_report")
def generate_daily_report():
    logger.info("🔄 Dagrapport-task gestart")

    today = datetime.now().date()
    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen databaseverbinding bij rapportgeneratie")
        return

    cursor = None
    try:
        cursor = conn.cursor()

        # 1️⃣ AI-rapport genereren (haalt zelf actuele data/scores/AI-insights uit DB)
        logger.info("🧠 Rapportgeneratie gestart...")
        full_report = generate_daily_report_sections("BTC")
        if not isinstance(full_report, dict):
            logger.error("❌ Ongeldige rapportstructuur (geen dict). Afgebroken.")
            return

        # Veilige float parser
        def _to_float(x, default=0.0):
            try:
                return float(x)
            except Exception:
                return default

        macro_score     = _to_float(full_report.get("macro_score", 0))
        technical_score = _to_float(full_report.get("technical_score", 0))
        setup_score     = _to_float(full_report.get("setup_score", 0))
        market_score    = _to_float(full_report.get("market_score", 0))

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
                full_report.get("btc_summary", "") or "",
                full_report.get("macro_summary", "") or "",
                full_report.get("setup_checklist", "") or "",
                full_report.get("priorities", "") or "",
                full_report.get("wyckoff_analysis", "") or "",
                full_report.get("recommendations", "") or "",
                full_report.get("conclusion", "") or "",
                full_report.get("outlook", "") or "",
                macro_score, technical_score, setup_score, market_score
            )
        )
        conn.commit()

        # 3️⃣ Laatste rij ophalen voor PDF
        cursor.execute("SELECT * FROM daily_reports WHERE report_date = %s LIMIT 1;", (today,))
        row = cursor.fetchone()
        if not row:
            logger.warning(f"⚠️ Geen rapport gevonden voor PDF voor {today}")
            return

        cols = [desc[0] for desc in cursor.description]
        report_dict = dict(zip(cols, row))

        # ➕ Injecteer AI Master Score / inzichten (zonder DB schema te wijzigen)
        ai_master = full_report.get("ai_master_score", {}) or {}
        report_dict.update({
            "ai_master_score": ai_master.get("score"),
            "ai_master_trend": ai_master.get("trend"),
            "ai_master_bias": ai_master.get("bias"),
            "ai_master_risk": ai_master.get("risk"),
            "ai_master_outlook": ai_master.get("outlook"),
            "ai_master_summary": ai_master.get("summary"),
            # Hele set per categorie (macro/market/technical/score) is ook beschikbaar:
            "ai_insights": full_report.get("ai_insights", {}),
        })

        # 4️⃣ PDF genereren (generator kan bovenstaande extra keys gebruiken)
        pdf_bytes = generate_pdf_report(report_dict, report_type="daily")
        if not pdf_bytes:
            logger.error("❌ generate_pdf_report gaf geen inhoud terug.")
            return

        pdf_dir = os.path.join("static", "pdf", "daily")
        os.makedirs(pdf_dir, exist_ok=True)
        pdf_path = os.path.join(pdf_dir, f"daily_{today}.pdf")

        try:
            if hasattr(pdf_bytes, "getbuffer"):
                with open(pdf_path, "wb") as f:
                    f.write(pdf_bytes.getbuffer())
            else:
                with open(pdf_path, "wb") as f:
                    f.write(pdf_bytes)
            logger.info(f"🖨️ PDF opgeslagen: {pdf_path}")
        except Exception as e:
            logger.error(f"❌ Kon PDF niet wegschrijven naar {pdf_path}: {e}", exc_info=True)
            return

        # 5️⃣ E-mail versturen (met AI master score in subject/body)
        market_data = full_report.get("market_data", {}) or {}
        price = market_data.get("price", "–")
        volume = market_data.get("volume", "–")
        change_24h = market_data.get("change_24h", "–")

        ms = ai_master.get("score")
        mtrend = ai_master.get("trend") or "–"
        subject_ms = f" | AI {int(ms)}" if isinstance(ms, (int, float)) else ""

        try:
            subject = f"📈 BTC Daily Report – {today}{subject_ms}"
            body = (
                f"Hierbij het automatisch gegenereerde dagelijkse Bitcoin rapport voor {today}.\n\n"
                f"Huidige prijs: ${price} | Volume: {volume} | 24u verandering: {change_24h}%\n"
                f"AI Master Score: {ms} ({mtrend})\n\n"
                "Bekijk de samenvatting, Wyckoff-analyse en strategieën in de bijlage."
            )
            send_email_with_attachment(subject, body, pdf_path)
            logger.info(f"📤 Dagrapport verzonden via e-mail ({pdf_path})")
        except Exception as e:
            logger.error(f"❌ Fout bij verzenden van e-mail: {e}", exc_info=True)

    except Exception as e:
        logger.error(f"❌ Fout tijdens rapportgeneratie: {e}", exc_info=True)
        if conn:
            conn.rollback()
    finally:
        try:
            if cursor:
                cursor.close()
        except Exception:
            pass
        conn.close()
        logger.info(f"✅ Dagrapport voltooid voor {today}")
