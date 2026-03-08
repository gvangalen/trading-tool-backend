import logging
from celery import shared_task

from backend.tasks.market_task import fetch_market_data
from backend.tasks.macro_task import generate_macro_scores
from backend.tasks.technical_task import generate_technical_scores
from backend.tasks.setup_validation_task import validate_setups
from backend.tasks.ai_insights_task import generate_ai_category_insights
from backend.services.portfolio_snapshot_service import snapshot_all_for_user

logger = logging.getLogger(__name__)


@shared_task(bind=True)
def bootstrap_agents_task(self, user_id: int):
    """
    Run initial AI agents for a new user environment.

    Runs agents sequentially to avoid race conditions.
    """

    logger.info(f"🚀 Bootstrapping agents for user {user_id}")

    try:

        # 1️⃣ Market data
        logger.info("📊 Fetching market data")
        fetch_market_data()

        # 2️⃣ Macro scores
        logger.info("🌍 Generating macro scores")
        generate_macro_scores(user_id=user_id)

        # 3️⃣ Technical scores
        logger.info("📈 Generating technical scores")
        generate_technical_scores(user_id=user_id)

        # 4️⃣ Setup validation
        logger.info("🧠 Validating setups")
        validate_setups(user_id=user_id)

        # 5️⃣ AI insights
        logger.info("🤖 Generating AI insights")
        generate_ai_category_insights(user_id=user_id)

        # 6️⃣ Portfolio snapshot
        logger.info("💰 Creating portfolio snapshot")
        snapshot_all_for_user(user_id=user_id)

        logger.info(f"✅ Bootstrap complete for user {user_id}")

        return {"status": "complete"}

    except Exception as e:
        logger.exception("❌ Bootstrap failed")
        raise e
