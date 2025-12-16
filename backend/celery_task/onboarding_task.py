import logging
from celery import shared_task, chain

from backend.utils.db import get_db_connection

logger = logging.getLogger(__name__)

# ======================================================
# 🚀 Onboarding Pipeline Task
# ======================================================
@shared_task(
    name="backend.celery_task.onboarding_task.run_onboarding_pipeline",
    bind=True,
    autoretry_for=(Exception,),
    retry_kwargs={"max_retries": 3, "countdown": 30},
    retry_backoff=True,
)
def run_onboarding_pipeline(self, user_id: int):
    """
    Start de volledige onboarding pipeline voor een gebruiker.

    Wordt exact ÉÉN keer gestart per gebruiker.

    Volgorde:
    1️⃣ Daily scores (per user)
    2️⃣ Setup agent (globaal)
    3️⃣ Strategy agent (globaal)
    4️⃣ AI insights (globaal)
    5️⃣ Daily report (per user)
    """

    logger.info("=================================================")
    logger.info(f"🚀 Onboarding pipeline START voor user_id={user_id}")
    logger.info(f"📌 Parent task_id={self.request.id}")
    logger.info("=================================================")

    conn = get_db_connection()

    try:
        # --------------------------------------------------
        # 🔒 IDEMPOTENTIE CHECK
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
            updated_rows = cur.fetchall()

        conn.commit()

        if not updated_rows:
            logger.warning(
                f"⚠️ Onboarding pipeline AL EERDER gestart voor user_id={user_id} — skip"
            )
            return {
                "status": "already_started",
                "user_id": user_id,
                "parent_task_id": self.request.id,
            }

        logger.info(
            f"✅ pipeline_started=TRUE gezet voor user_id={user_id} "
            f"(rows={len(updated_rows)})"
        )

        # --------------------------------------------------
        # ⚠️ Lazy imports (NA idempotentie)
        # --------------------------------------------------
        from backend.celery_task.store_daily_scores_task import (
            store_daily_scores_task,
        )
        from backend.celery_task.setup_task import (
            run_setup_agent_daily,
        )
        from backend.celery_task.strategy_task import (
            generate_all,
        )

        from backend.ai_agents.macro_ai_agent import generate_macro_insight
        from backend.ai_agents.market_ai_agent import generate_market_insight
        from backend.ai_agents.technical_ai_agent import generate_technical_insight
        from backend.ai_agents.score_ai_agent import generate_master_score

        from backend.celery_task.daily_report_task import (
            generate_daily_report,
        )

        # --------------------------------------------------
        # 🔗 Celery chain
        # --------------------------------------------------
        workflow = chain(
            # 1️⃣ User scores
            store_daily_scores_task.s(user_id),

            # 2️⃣ Setup agent (globaal)
            run_setup_agent_daily.s(),

            # 3️⃣ Strategy agent (globaal)
            generate_all.s(),

            # 4️⃣ AI insights (globaal)
            generate_macro_insight.s(),
            generate_market_insight.s(),
            generate_technical_insight.s(),
            generate_master_score.s(),

            # 5️⃣ Daily report (user)
            generate_daily_report.si(user_id),
        )

        result = workflow.apply_async()

        logger.info(
            "🔗 Onboarding chain QUEUED | "
            f"chain_id={result.id} | parent_task_id={self.request.id}"
        )

        return {
            "status": "started",
            "user_id": user_id,
            "parent_task_id": self.request.id,
            "chain_id": result.id,
        }

    except Exception as e:
        conn.rollback()
        logger.error(
            f"❌ Fout in onboarding pipeline user_id={user_id}: {e}",
            exc_info=True,
        )
        raise

    finally:
        conn.close()
