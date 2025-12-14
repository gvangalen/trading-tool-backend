from celery import shared_task, chain
import logging

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
    logger.info(f"🚀 Onboarding pipeline START voor user_id={user_id}")
    logger.info("=================================================")

    try:
        from backend.celery_task.daily_scores_task import calculate_daily_scores
        from backend.celery_task.daily_report_task import generate_daily_report

        workflow = chain(
            calculate_daily_scores.s(user_id=user_id),
            generate_daily_report.si(user_id=user_id),  # 🔥 FIX
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
