# daily_report_task.py
import os
import json
import logging
from datetime import datetime
from pytz import timezone
from celery import Celery
from db import get_db_connection
from utils.scoring_utils import generate_scores
from utils.setup_validator import validate_setups
from utils.strategy_advice_generator import generate_strategy_advice

# ✅ Logging instellen
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ✅ Celery initialisatie met fallback
celery = Celery(__name__)
celery.conf.broker_url = os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0")
celery.conf.result_backend = os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/0")

# ✅ Opslaan in database met ON CONFLICT
def save_report_to_db(date, report_data):
    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen databaseverbinding beschikbaar.")
        return False

    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO daily_reports (
                    report_date,
                    btc_summary,
                    macro_summary,
                    setup_checklist,
                    priorities,
                    wyckoff_analysis,
                    recommendations,
                    conclusion,
                    outlook
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (report_date) DO UPDATE SET
                    btc_summary = EXCLUDED.btc_summary,
                    macro_summary = EXCLUDED.macro_summary,
                    setup_checklist = EXCLUDED.setup_checklist,
                    priorities = EXCLUDED.priorities,
                    wyckoff_analysis = EXCLUDED.wyckoff_analysis,
                    recommendations = EXCLUDED.recommendations,
                    conclusion = EXCLUDED.conclusion,
                    outlook = EXCLUDED.outlook
            """, (
                date,
                report_data.get("btc_summary"),
                report_data.get("macro_summary"),
                report_data.get("setup_checklist"),
                report_data.get("priorities"),
                report_data.get("wyckoff_analysis"),
                report_data.get("recommendations"),
                report_data.get("conclusion"),
                report_data.get("outlook")
            ))
            conn.commit()
        logger.info("✅ Dagrapport succesvol opgeslagen in de database.")
        return True
    except Exception as e:
        logger.error(f"❌ Fout bij opslaan rapport: {e}")
        return False
    finally:
        conn.close()

# ✅ Celery taak: Dagelijks rapport genereren
@celery.task(name="generate_daily_report")
def generate_daily_report():
    logger.info("📝 Genereren van dagelijks rapport gestart...")

    if not get_db_connection():
        logger.error("❌ Dagrapport geannuleerd: databaseverbinding faalt.")
        return

    # 🔍 Data ophalen
    scores = generate_scores(asset="BTC")
    setups = validate_setups(asset="BTC")
    strategy = generate_strategy_advice(asset="BTC", scores=scores, setups=setups)

    # ✅ Fallback bij incomplete data
    if not scores or not setups or not strategy:
        logger.warning("⚠️ Incomplete data voor rapport. Mogelijk ontbrekende scores of setups.")

    # 📅 Datum bepalen (met UTC timezone)
    today = datetime.now(timezone("UTC")).date()

    # 🧠 AI-tradingadvies opstellen als tekstblok
    advies_blok = (
        f"📋 Setup: {strategy.get('setup', 'n.v.t.')}\n"
        f"📈 Trend: {strategy.get('trend', 'n.v.t.')}\n"
        f"🎯 Entry: ${strategy.get('entry', 'n.v.t.')}\n"
        f"🎯 Targets: {', '.join(map(str, strategy.get('targets', [])))}\n"
        f"🛑 Stop-loss: ${strategy.get('stop_loss', 'n.v.t.')}\n"
        f"⚠️ Risico: {strategy.get('risico', 'n.v.t.')}\n"
        f"💬 Opmerking: {strategy.get('reden', '—')}"
    )

    # 📄 Rapportgegevens samenstellen
    report_data = {
        "btc_summary": strategy.get("btc_summary", "Samenvatting BTC volgt..."),
        "macro_summary": strategy.get("macro_summary", "Macro-overzicht niet beschikbaar."),
        "setup_checklist": strategy.get("setup_checklist", "Geen checklist gegenereerd."),
        "priorities": strategy.get("priorities", "Nog geen duidelijke focuspunten."),
        "wyckoff_analysis": strategy.get("wyckoff_analysis", "Wyckoff-analyse nog niet toegevoegd."),
        "recommendations": advies_blok,
        "conclusion": strategy.get("conclusion", "Conclusie volgt."),
        "outlook": strategy.get("outlook", "Vooruitblik niet beschikbaar.")
    }

    # 💾 Backup JSON maken
    try:
        with open(f"daily_report_{today}.json", "w") as f:
            json.dump(report_data, f, indent=2)
        logger.info(f"🧾 Backup opgeslagen als daily_report_{today}.json")
    except Exception as e:
        logger.warning(f"⚠️ Backup json maken mislukt: {e}")

    # 💾 Opslaan in de database
    logger.info("🧠 Rapportinhoud gegenereerd. Opslaan...")
    save_report_to_db(today, report_data)
    return report_data
