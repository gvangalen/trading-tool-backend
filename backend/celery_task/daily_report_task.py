import json
import logging
from datetime import date, datetime
from decimal import Decimal

from celery import shared_task
from dotenv import load_dotenv
from psycopg2.extras import Json

from backend.utils.db import get_db_connection
from backend.ai_agents.report_ai_agent import generate_daily_report_sections

# 🧠 Regime memory
from backend.ai_core.regime_memory import (
    store_regime_memory,
    get_regime_memory,
)

# 📸 Snapshot service
from backend.services.report_snapshot_service import create_report_snapshot

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
    if v is None:
        return Json(fallback) if fallback is not None else None
    if isinstance(v, (dict, list)):
        return Json(v)
    if isinstance(v, str):
        try:
            return Json(json.loads(v))
        except Exception:
            return Json(v)
    return Json(v)


# =====================================================
# 🧾 DAILY REPORT TASK
# =====================================================

@shared_task(name="backend.celery_task.daily_report_task.generate_daily_report")
def generate_daily_report(user_id: int):

    today = date.today()
    logger.info(f"📄 Daily report | user_id={user_id} | {today}")

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen databaseverbinding")
        return

    cursor = None

    try:
        cursor = conn.cursor()

        # 🧠 REGIME MEMORY
        get_regime_memory(user_id)
        store_regime_memory(user_id)

        # -------------------------------------------------
        # 1️⃣ GENERATE REPORT
        # -------------------------------------------------
        report = generate_daily_report_sections(user_id=user_id)

        if not isinstance(report, dict):
            raise ValueError("Report agent gaf geen geldig dict terug")

        report["meta"] = {
            "version": "daily_v1",
            "generated_at": datetime.utcnow().isoformat(),
        }

        executive_summary    = jsonb(report.get("executive_summary"), {})
        market_analysis      = jsonb(report.get("market_analysis"), {})
        macro_context        = jsonb(report.get("macro_context"), {})
        technical_analysis   = jsonb(report.get("technical_analysis"), {})
        setup_validation     = jsonb(report.get("setup_validation"), {})
        strategy_implication = jsonb(report.get("strategy_implication"), {})
        outlook              = jsonb(report.get("outlook"), {})

        bot_strategy = jsonb(report.get("bot_strategy"), {})
        bot_snapshot = jsonb(report.get("bot_snapshot"))

        price      = to_float(report.get("price"))
        change_24h = to_float(report.get("change_24h"))
        volume     = to_float(report.get("volume"))

        macro_score     = to_float(report.get("macro_score"))
        technical_score = to_float(report.get("technical_score"))
        market_score    = to_float(report.get("market_score"))
        setup_score     = to_float(report.get("setup_score"))

        market_indicators    = jsonb(report.get("market_indicator_highlights"), [])
        macro_indicators     = jsonb(report.get("macro_indicator_highlights"), [])
        technical_indicators = jsonb(report.get("technical_indicator_highlights"), [])

        best_setup      = jsonb(report.get("best_setup"))
        top_setups      = jsonb(report.get("top_setups"), [])
        active_strategy = jsonb(report.get("active_strategy"))

        # -------------------------------------------------
        # 2️⃣ UPSERT REPORT
        # -------------------------------------------------
        cursor.execute("""
            INSERT INTO daily_reports (
                report_date, user_id,
                executive_summary, market_analysis, macro_context,
                technical_analysis, setup_validation, strategy_implication, outlook,
                bot_strategy, bot_snapshot,
                price, change_24h, volume,
                macro_score, technical_score, market_score, setup_score,
                market_indicator_highlights, macro_indicator_highlights, technical_indicator_highlights,
                best_setup, top_setups, active_strategy
            )
            VALUES (
                %s,%s,
                %s,%s,%s,%s,%s,%s,%s,
                %s,%s,
                %s,%s,%s,
                %s,%s,%s,%s,
                %s,%s,%s,
                %s,%s,%s
            )
            ON CONFLICT (user_id, report_date)
            DO UPDATE SET
                executive_summary = EXCLUDED.executive_summary,
                market_analysis = EXCLUDED.market_analysis,
                macro_context = EXCLUDED.macro_context,
                technical_analysis = EXCLUDED.technical_analysis,
                setup_validation = EXCLUDED.setup_validation,
                strategy_implication = EXCLUDED.strategy_implication,
                outlook = EXCLUDED.outlook,
                bot_strategy = EXCLUDED.bot_strategy,
                bot_snapshot = EXCLUDED.bot_snapshot,
                price = EXCLUDED.price,
                change_24h = EXCLUDED.change_24h,
                volume = EXCLUDED.volume,
                macro_score = EXCLUDED.macro_score,
                technical_score = EXCLUDED.technical_score,
                market_score = EXCLUDED.market_score,
                setup_score = EXCLUDED.setup_score,
                market_indicator_highlights = EXCLUDED.market_indicator_highlights,
                macro_indicator_highlights = EXCLUDED.macro_indicator_highlights,
                technical_indicator_highlights = EXCLUDED.technical_indicator_highlights,
                best_setup = EXCLUDED.best_setup,
                top_setups = EXCLUDED.top_setups,
                active_strategy = EXCLUDED.active_strategy,
                generated_at = NOW();
        """, (
            today, user_id,
            executive_summary, market_analysis, macro_context,
            technical_analysis, setup_validation, strategy_implication, outlook,
            bot_strategy, bot_snapshot,
            price, change_24h, volume,
            macro_score, technical_score, market_score, setup_score,
            market_indicators, macro_indicators, technical_indicators,
            best_setup, top_setups, active_strategy
        ))

        conn.commit()
        logger.info("💾 daily_reports opgeslagen")

        # -------------------------------------------------
        # 3️⃣ CREATE SNAPSHOT
        # -------------------------------------------------
        snapshot_id, token = create_report_snapshot(
            user_id=user_id,
            report_type="daily",
            report_id=0,
            report_json=report,
        )

        logger.info(f"📸 Snapshot created | id={snapshot_id}")

        # ⚠️ GEEN PDF CALL HIER!
        # snapshot service triggert Celery al.

    except Exception:
        logger.exception("❌ Fout in daily_report_task")
        conn.rollback()

    finally:
        if cursor:
            cursor.close()
        conn.close()
        logger.info("✅ Daily report task afgerond")
