# backend/celery_task/portfolio_snapshot_task.py

import logging

from backend.celery_app import celery_app
from backend.services.portfolio_snapshot_service import snapshot_all_for_user

logger = logging.getLogger(__name__)


# =========================================================
# 📊 PORTFOLIO SNAPSHOT TASK
# =========================================================
@celery_app.task(name="backend.celery_task.portfolio_snapshot_task.run_portfolio_snapshot")
def run_portfolio_snapshot(user_id: int):
    """
    Maakt een portfolio snapshot voor een specifieke gebruiker.

    Deze task wordt meestal via de dispatcher aangeroepen:
    dispatch_for_all_users → run_portfolio_snapshot(user_id)
    """

    try:
        snapshot_all_for_user(user_id, bucket="1h")

        logger.info(
            f"📊 Portfolio snapshot succesvol voor user_id={user_id}"
        )

        return {"status": "ok", "user_id": user_id}

    except Exception as e:

        logger.exception(
            f"❌ Portfolio snapshot mislukt voor user_id={user_id}"
        )

        return {
            "status": "error",
            "user_id": user_id,
            "error": str(e),
        }
