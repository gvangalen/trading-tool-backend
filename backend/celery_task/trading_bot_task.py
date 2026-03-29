# backend/celery_task/trading_bot_task.py

import logging
from datetime import date
from typing import Optional

from celery import shared_task

from backend.ai_agents.trading_bot_agent import run_trading_bot_agent
from backend.services.portfolio_snapshot_service import snapshot_all_for_user
from backend.celery_task.strategy_task import run_daily_strategy_snapshot

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@shared_task(
    name="backend.celery_task.trading_bot_task.run_daily_trading_bot",
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_kwargs={"max_retries": 3},
)
def run_daily_trading_bot(self, user_id: int, report_date: Optional[str] = None):

    try:
        run_date = date.fromisoformat(report_date) if report_date else date.today()
    except Exception:
        run_date = date.today()

    logger.info(f"🤖 START | user={user_id} | date={run_date}")

    try:
        # 1️⃣ Strategy snapshot
        try:
            run_daily_strategy_snapshot(user_id=user_id)
        except Exception:
            logger.exception("Strategy snapshot failed")

        # 2️⃣ Bot agent (ALLE LOGIC HIERIN)
        result = run_trading_bot_agent(
            user_id=user_id,
            report_date=run_date,
        )

        if not result.get("ok"):
            return result

        # 3️⃣ Portfolio snapshot
        try:
            snapshot_all_for_user(user_id, bucket="1h")
            snapshot_all_for_user(user_id, bucket="1d")
        except Exception:
            logger.exception("Portfolio snapshot failed")

        logger.info("✅ DONE")

        return {
            "ok": True,
            "user_id": user_id,
            "date": str(run_date),
            "bots": result.get("bots"),
            "decisions": len(result.get("decisions", [])),
        }

    except Exception as e:
        logger.exception("❌ CRASH")
        return {"ok": False, "error": str(e)}
