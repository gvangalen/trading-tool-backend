import logging
from celery import shared_task

from backend.ai_agents.score_ai_agent import generate_master_score

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# =========================================================
# 🧠 DAILY SCORE ORCHESTRATOR (CELERY ENTRYPOINT)
# =========================================================
@shared_task(
    name="backend.celery_task.store_daily_scores_task.run_master_score_ai"
)
def run_master_score_ai():
    """
    🎯 ENIGE taak van deze Celery task:

    - Triggeren van de MASTER score AI
    - Voor ALLE users
    - Score-logica zit VOLLEDIG in:
        backend.ai_agents.score_ai_agent

    Deze task:
    - bevat GEEN business logic
    - bevat GEEN AI prompt
    - bevat GEEN scoring regels
    """

    logger.info("🚀 Celery task gestart: MASTER Score AI (daily)")

    try:
        generate_master_score()
        logger.info("✅ MASTER Score AI succesvol afgerond")

    except Exception as e:
        logger.error("❌ Fout tijdens MASTER Score AI run", exc_info=True)
        raise e
