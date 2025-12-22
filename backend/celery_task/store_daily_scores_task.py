import logging
from celery import shared_task

from backend.utils.db import get_db_connection
from backend.utils.scoring_utils import get_scores_for_symbol
from backend.ai_agents.score_ai_agent import generate_master_score

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# =========================================================
# 1️⃣ BUILD DAILY SCORES (RULE-BASED)
# =========================================================
def build_daily_scores_for_user(user_id: int):
    scores = get_scores_for_symbol(
        user_id=user_id,
        include_metadata=True
    )

    if not scores:
        logger.warning(f"⚠️ Geen scores voor user_id={user_id}")
        return

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
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
                    updated_at = NOW();
            """, (
                user_id,
                scores["macro_score"],
                scores["technical_score"],
                scores["market_score"],
                scores["setup_score"],
                scores.get("macro_interpretation"),
                scores.get("technical_interpretation"),
                scores.get("market_interpretation"),
                scores.get("macro_top_contributors", []),
                scores.get("technical_top_contributors", []),
                scores.get("market_top_contributors", []),
            ))

        conn.commit()
        logger.info(f"💾 daily_scores opgeslagen voor user_id={user_id}")

    finally:
        conn.close()


# =========================================================
# 2️⃣ CELERY ORCHESTRATOR (DAILY)
# =========================================================
@shared_task(
    name="backend.celery_task.store_daily_scores_task.run_daily_scoring_pipeline"
)
def run_daily_scoring_pipeline():
    """
    DAGELIJKSE SCORE PIPELINE

    Stap 1: rule-based daily_scores
    Stap 2: AI master score
    """

    logger.info("🚀 Start DAILY SCORING PIPELINE")

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM users;")
            users = [r[0] for r in cur.fetchall()]
    finally:
        conn.close()

    for user_id in users:
        build_daily_scores_for_user(user_id)

    # 🔥 PAS NU AI
    generate_master_score()

    logger.info("✅ DAILY SCORING PIPELINE afgerond")
