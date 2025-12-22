import logging
import json
from celery import shared_task

from backend.utils.db import get_db_connection
from backend.utils.scoring_utils import get_scores_for_symbol
from backend.ai_agents.score_ai_agent import generate_master_score

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def _jsonb(value):
    """Zorgt dat we altijd geldige JSON naar jsonb casten."""
    return json.dumps(value or [], ensure_ascii=False)


# =========================================================
# 1️⃣ BUILD DAILY SCORES (RULE-BASED) — PER USER
# =========================================================
def build_daily_scores_for_user(user_id: int):
    scores = get_scores_for_symbol(user_id=user_id, include_metadata=True)

    if not scores:
        logger.warning(f"⚠️ Geen scores voor user_id={user_id}")
        return

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding")
        return

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO daily_scores (
                    report_date,
                    user_id,
                    macro_score,
                    technical_score,
                    market_score,
                    setup_score,
                    macro_interpretation,
                    technical_interpretation,
                    market_interpretation,
                    macro_top_contributors,
                    technical_top_contributors,
                    market_top_contributors
                )
                VALUES (
                    CURRENT_DATE, %s, %s, %s, %s, %s,
                    %s, %s, %s,
                    %s::jsonb, %s::jsonb, %s::jsonb
                )
                ON CONFLICT (user_id, report_date)
                DO UPDATE SET
                    macro_score = EXCLUDED.macro_score,
                    technical_score = EXCLUDED.technical_score,
                    market_score = EXCLUDED.market_score,
                    setup_score = EXCLUDED.setup_score,
                    macro_interpretation = EXCLUDED.macro_interpretation,
                    technical_interpretation = EXCLUDED.technical_interpretation,
                    market_interpretation = EXCLUDED.market_interpretation,
                    macro_top_contributors = EXCLUDED.macro_top_contributors,
                    technical_top_contributors = EXCLUDED.technical_top_contributors,
                    market_top_contributors = EXCLUDED.market_top_contributors,
                    updated_at = NOW();
                """,
                (
                    user_id,
                    scores.get("macro_score"),
                    scores.get("technical_score"),
                    scores.get("market_score"),
                    scores.get("setup_score"),
                    scores.get("macro_interpretation"),
                    scores.get("technical_interpretation"),
                    scores.get("market_interpretation"),
                    _jsonb(scores.get("macro_top_contributors", [])),
                    _jsonb(scores.get("technical_top_contributors", [])),
                    _jsonb(scores.get("market_top_contributors", [])),
                ),
            )

        conn.commit()
        logger.info(f"💾 daily_scores opgeslagen voor user_id={user_id}")

    except Exception:
        conn.rollback()
        logger.error("❌ Fout bij opslaan daily_scores", exc_info=True)

    finally:
        conn.close()


# =========================================================
# 2️⃣ CELERY TASK: RULE-BASED DAILY SCORES (ALLE USERS)
# =========================================================
@shared_task(name="backend.celery_task.store_daily_scores_task.run_rule_based_daily_scores")
def run_rule_based_daily_scores():
    logger.info("🚀 Start RULE-BASED daily_scores (alle users)")

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding")
        return

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM users;")
            users = [r[0] for r in cur.fetchall()]
    finally:
        conn.close()

    for user_id in users:
        build_daily_scores_for_user(user_id)

    logger.info("✅ RULE-BASED daily_scores klaar")


# =========================================================
# 3️⃣ CELERY TASK: MASTER SCORE AI (ALLE USERS)
# =========================================================
@shared_task(name="backend.celery_task.store_daily_scores_task.run_master_score_ai")
def run_master_score_ai():
    """
    Draait master orchestrator AI.
    LET OP: plan deze NA macro/market/technical/setup/strategy agents.
    """
    logger.info("🧠 Start MASTER Score AI (alle users)")

    try:
        generate_master_score()  # draait intern voor alle users
        logger.info("✅ MASTER Score AI afgerond")
    except Exception:
        logger.error("❌ Fout tijdens MASTER Score AI", exc_info=True)
        raise
