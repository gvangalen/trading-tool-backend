import logging
from celery import shared_task

from backend.celery_task.market_task import fetch_market_data
from backend.celery_task.macro_task import fetch_macro_data
from backend.celery_task.technical_task import fetch_technical_data_day
from backend.celery_task.setup_task import run_setup_agent_daily
from backend.celery_task.market_task import run_market_agent_daily
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

        # 2️⃣ Macro ingest
        logger.info("🌍 Fetching macro indicators")
        fetch_macro_data(user_id=user_id)

        # 3️⃣ Technical ingest
        logger.info("📈 Fetching technical indicators")
        fetch_technical_data_day(user_id=user_id)

        # 4️⃣ Setup validation
        logger.info("🧠 Running setup agent")
        run_setup_agent_daily(user_id=user_id)

        # 5️⃣ AI insights
        logger.info("🤖 Running market AI agent")
        run_market_agent_daily(user_id=user_id)

        # 6️⃣ Portfolio snapshot
        logger.info("💰 Creating portfolio snapshot")
        snapshot_all_for_user(user_id=user_id)

        logger.info(f"✅ Bootstrap complete for user {user_id}")

        return {"status": "complete"}

    except Exception as e:
        logger.exception("❌ Bootstrap failed")
        raise e
