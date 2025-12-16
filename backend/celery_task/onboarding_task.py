import logging
from celery import shared_task, chain, group
from backend.utils.db import get_db_connection

logger = logging.getLogger(__name__)

# ======================================================
# 🚀 ONBOARDING PIPELINE — WERKEND & SIMPEL
# ======================================================
@shared_task(
    name="backend.celery_task.onboarding_task.run_onboarding_pipeline",
    bind=True,
)
def run_onboarding_pipeline(self, user_id: int):
    """
    Onboarding pipeline (exact 1x per user):

    USER-BASED:
    1) Daily scores
    2) Daily report

    GLOBAAL:
    3) Setup agent
    4) Strategy agent
    5) AI insights
    """

    logger.info("=================================================")
    logger.info(f"🚀 ONBOARDING START user_id={user_id}")
    logger.info(f"📌 task_id={self.request.id}")
    logger.info("=================================================")

    conn = get_db_connection()

    try:
        # --------------------------------------------------
        # 🔒 IDEMPOTENTIE
        # --------------------------------------------------
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE onboarding_steps
                SET pipeline_started = TRUE
                WHERE user_id = %s
                  AND flow = 'default'
                  AND pipeline_started = FALSE
                RETURNING id
                """,
                (user_id,),
            )
            rows = cur.fetchall()

        conn.commit()

        if not rows:
            logger.warning(f"⚠️ Onboarding al gestart voor user_id={user_id}")
            return {"status": "already_started", "user_id": user_id}

        logger.info(f"✅ pipeline_started gezet voor user_id={user_id}")

        # --------------------------------------------------
        # 🔽 LAZY IMPORTS (EXACT wat bestaat)
        # --------------------------------------------------
        from backend.celery_task.store_daily_scores_task import (
            store_daily_scores_task,
        )
        from backend.celery_task.daily_report_task import (
            generate_daily_report,
        )

        from backend.celery_task.setup_task import (
            run_setup_agent_daily,
        )
        from backend.celery_task.strategy_task import (
            generate_all,
        )

        from backend.ai_agents.macro_ai_agent import (
            generate_macro_insight,
        )
        from backend.ai_agents.market_ai_agent import (
            generate_market_insight,
        )
        from backend.ai_agents.technical_ai_agent import (
            generate_technical_insight,
        )
        from backend.ai_agents.score_ai_agent import (
            generate_master_score,
        )

        # --------------------------------------------------
        # 👤 USER FLOW (STRICT USER-BASED)
        # --------------------------------------------------
        user_flow = chain(
            store_daily_scores_task.s(user_id),
            generate_daily_report.si(user_id),
        )

        # --------------------------------------------------
        # 🌍 GLOBALE FLOW (GEEN user_id!)
        # --------------------------------------------------
        global_flow = group(
            run_setup_agent_daily.si(),
            generate_all.si(),
            generate_macro_insight.si(),
            generate_market_insight.si(),
            generate_technical_insight.si(),
            generate_master_score.si(),
        )

        # --------------------------------------------------
        # 🚀 EXECUTIE
        # --------------------------------------------------
        user_flow.apply_async()
        global_flow.apply_async()

        logger.info("✅ Onboarding volledig gestart")

        return {
            "status": "started",
            "user_id": user_id,
            "user_flow": "daily_scores → daily_report",
            "global_flow": "setup + strategy + ai_insights",
        }

    except Exception:
        conn.rollback()
        logger.exception("❌ Onboarding pipeline fout")
        raise

    finally:
        conn.close()
