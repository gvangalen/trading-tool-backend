import logging
from celery import shared_task

from backend.ai_core.regime_memory import store_regime_memory

logger = logging.getLogger(__name__)


@shared_task(
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=60,        # exponential backoff
    retry_backoff_max=600,
    retry_kwargs={"max_retries": 5},
)
def run_regime_memory(self, user_id: int):

    logger.info("🧠 Running regime memory for user=%s", user_id)

    try:
        store_regime_memory(user_id)

        logger.info("✅ Regime memory stored for user=%s", user_id)

    except Exception as e:

        logger.exception("❌ Regime memory failed for user=%s", user_id)

        raise self.retry(exc=e)
