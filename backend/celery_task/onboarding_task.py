import logging
from celery import shared_task, chain
from backend.utils.db import get_db_connection

logger = logging.getLogger(__name__)


@shared_task(
    name="backend.celery_task.onboarding_task.run_onboarding_pipeline",
    bind=True,
    autoretry_for=(Exception,),
    retry_kwargs={"max_retries": 3, "countdown": 30},
    retry_backoff=True,
)
def run_onboarding_pipeline(self, user_id: int):
    logger.info("=================================================")
    logger.info(f"🚀 ONBOARDING START user_id={user_id}")
    logger.info(f"📌 task_id={self.request.id}")
    logger.info("=================================================")

    conn = get_db_connection()

    try:
        # --------------------------------------------------
        # 🔒 Idempotentie
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
        # Lazy imports
        # --------------------------------------------------
        from backend.celery_task.store_daily_scores_task import store_daily_scores_task
        from backend.celery_task.setup_task import run_setup_agent_daily
        from backend.celery_task.strategy_task import generate_all as run_strategy_agent
        from backend.celery_task.daily_report_task import generate_daily_report

        from backend.ai_agents.macro_ai_agent import generate_macro_insight
        from backend.ai_agents.market_ai_agent import generate_market_insight
        from backend.ai_agents.technical_ai_agent import generate_technical_insight
        from backend.ai_agents.score_ai_agent import generate_master_score

        # --------------------------------------------------
        # 🔗 PER-USER FLOW (STRICT)
        # --------------------------------------------------
        user_flow = chain(
            store_daily_scores_task.si(user_id),
            run_setup_agent_daily.si(user_id),
            run_strategy_agent.si(user_id),
            generate_daily_report.si(user_id),
        )

        user_flow.apply_async()
        logger.info("🔗 Per-user onboarding flow gestart")

        # --------------------------------------------------
        # 🧠 GLOBALE AI INSIGHTS (1× trigger)
        # --------------------------------------------------
        generate_macro_insight.delay()
        generate_market_insight.delay()
        generate_technical_insight.delay()
        generate_master_score.delay()

        logger.info("🧠 Globale AI-insights geforceerd getriggerd")

        return {
            "status": "started",
            "user_id": user_id,
            "task_id": self.request.id,
        }

    except Exception:
        conn.rollback()
        logger.error("❌ Onboarding pipeline fout", exc_info=True)
        raise

    finally:
        conn.close()
