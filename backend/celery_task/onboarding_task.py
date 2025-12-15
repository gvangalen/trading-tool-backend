import logging
from celery import shared_task, chain

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

    Volgorde:
    1️⃣ Daily scores opslaan
    2️⃣ Daily report genereren
    """

    logger.info("=================================================")
    logger.info(f"🚀 Onboarding pipeline START voor user_id={user_id}")
    logger.info(f"📌 Parent task_id={self.request.id}")
    logger.info("=================================================")

    try:
        # ⚠️ Lazy imports
        from backend.celery_task.store_daily_scores_task import (
            store_daily_scores_task,
        )
        from backend.celery_task.daily_report_task import (
            generate_daily_report,
        )

        workflow = chain(
            store_daily_scores_task.s(user_id),
            generate_daily_report.si(user_id),
        )

        result = workflow.apply_async()

        logger.info(
            "🔗 Onboarding chain gestart | "
            f"chain_id={result.id} | root_id={result.root_id}"
        )

        return {
            "status": "started",
            "user_id": user_id,
            "parent_task_id": self.request.id,
            "chain_id": result.id,
            "root_id": result.root_id,
        }

    except Exception as e:
        logger.error(
            f"❌ Fout in onboarding pipeline user_id={user_id}: {e}",
            exc_info=True,
        )
        raise
