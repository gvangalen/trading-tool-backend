import logging
from celery import shared_task

from backend.services.portfolio_snapshot_service import snapshot_all_for_user

logger = logging.getLogger(__name__)


# =====================================================
# 📊 PORTFOLIO SNAPSHOT TASK
# =====================================================
@shared_task
def run_portfolio_snapshot(user_id: int):
    """
    Maakt een portfolio snapshot voor een specifieke gebruiker.

    Wordt aangeroepen via:
    dispatcher.dispatch_for_all_users
    """

    try:
        snapshot_all_for_user(user_id, bucket="1h")

        logger.info(
            f"📊 Portfolio snapshot succesvol | user_id={user_id}"
        )

        return {
            "status": "ok",
            "user_id": user_id,
        }

    except Exception as e:

        logger.exception(
            f"❌ Portfolio snapshot mislukt | user_id={user_id}"
        )

        return {
            "status": "error",
            "user_id": user_id,
            "error": str(e),
        }
