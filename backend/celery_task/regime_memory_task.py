from celery import shared_task
from backend.ai_core.regime_memory import store_regime_memory


@shared_task
def run_regime_memory(user_id: int):
    store_regime_memory(user_id)
