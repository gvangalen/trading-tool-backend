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

    Wordt exact ÉÉN keer getriggerd na:
    - laatste onboarding stap
    - of expliciete /onboarding/finish

    Volgorde:
    1️⃣ Daily scores opslaan
    2️⃣ Daily report genereren
    """

    logger.info("=================================================")
    logger.info(f"🚀 Onboarding pipeline START voor user_id={user_id}")
    logger.info("=================================================")

    try:
        # ⚠️ Lazy imports om circular imports te voorkomen
        from backend.celery_task.store_daily_scores_task import (
            store_daily_scores_task,
        )
        from backend.celery_task.daily_report_task import (
            generate_daily_report,
        )

        workflow = chain(
            store_daily_scores_task.s(user_id),
            generate_daily_report.si(user_id),  # ✅ CORRECT
        )

        workflow.apply_async()

        logger.info(
            f"✅ Onboarding pipeline succesvol gestart voor user_id={user_id}"
        )

    except Exception as e:
        logger.error(
            f"❌ Fout in onboarding pipeline user_id={user_id}: {e}",
            exc_info=True,
        )
        raise
