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

    Deze task wordt ÉÉN keer aangeroepen na onboarding
    (of handmatig voor herstel/debug).

    Volgorde:
    1️⃣ Daily scores berekenen
    2️⃣ Daily report genereren (incl AI agents)
    """

    logger.info("=================================================")
    logger.info(f"🚀 Onboarding pipeline START voor user_id={user_id}")
    logger.info("=================================================")

    try:
        # ⚠️ BELANGRIJK:
        # Imports hierbinnen om circular imports te voorkomen
        from backend.celery_task.daily_scores_task import (
            calculate_daily_scores,
        )
        from backend.celery_task.daily_report_task import (
            generate_daily_report,
        )

        # 🔗 Chain: scores → report
        workflow = chain(
            calculate_daily_scores.s(user_id=user_id),
            generate_daily_report.s(user_id=user_id),
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
