import logging
import json
from datetime import date
from celery import shared_task

from backend.utils.db import get_db_connection
from backend.utils.scoring_utils import get_scores_for_symbol

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# =========================================================
# 🧭 DAILY SCORE DISPATCHER (READ-ONLY)
# =========================================================
@shared_task(
    name="backend.celery_task.store_daily_scores_task.dispatch_daily_scores"
)
def dispatch_daily_scores(user_id: int):
    """
    ⚠️ Deze task:
    - SCHRIJFT NIETS in daily_scores
    - SCHRIJFT NIETS in daily_setup_scores
    - Bestaat alleen voor:
        • validatie
        • logging
        • fallback detectie
        • debugging

    Alle ECHTE scores worden:
    - gegenereerd door AI agents
    """

    logger.info(f"🧭 Daily score dispatcher gestart (user_id={user_id})")

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding")
        return

    today = date.today()

    try:
        # -------------------------------------------------
        # 1️⃣ Scores ophalen (READ-ONLY)
        # -------------------------------------------------
        try:
            scores = get_scores_for_symbol(
                user_id=user_id,
                include_metadata=True,
            )
        except TypeError:
            # legacy fallback
            scores = get_scores_for_symbol(include_metadata=True)

        if not scores:
            logger.warning(f"⚠️ Geen scores gevonden voor user_id={user_id}")
            return

        logger.info(
            f"📊 Scores snapshot user_id={user_id}:\n"
            f"{json.dumps(scores, indent=2)}"
        )

        # -------------------------------------------------
        # 2️⃣ Validatie: bestaan AI-scores al?
        # -------------------------------------------------
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    macro_score,
                    market_score,
                    technical_score
                FROM daily_scores
                WHERE user_id = %s
                  AND report_date = %s
                LIMIT 1;
                """,
                (user_id, today),
            )
            row = cur.fetchone()

        if not row:
            logger.warning(
                f"⚠️ daily_scores ontbreken voor user_id={user_id} ({today})"
            )
            logger.warning(
                "👉 Verwacht: macro/market/technical AI agent nog niet gedraaid"
            )
            return

        logger.info(
            f"✅ daily_scores aanwezig voor user_id={user_id} "
            f"(macro={row[0]}, market={row[1]}, technical={row[2]})"
        )

    except Exception as e:
        logger.error(
            f"❌ Fout in daily score dispatcher user_id={user_id}: {e}",
            exc_info=True,
        )

    finally:
        conn.close()
        logger.info(f"🔒 Dispatcher afgerond (user_id={user_id})")
