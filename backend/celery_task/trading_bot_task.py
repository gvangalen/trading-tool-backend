# backend/celery_task/trading_bot_task.py

import logging
from datetime import date
from typing import Optional

from celery import shared_task

from backend.ai_agents.trading_bot_agent import run_trading_bot_agent
from backend.services.portfolio_snapshot_service import snapshot_all_for_user

# 🔥 NIEUW: snapshot generator import
from backend.celery_task.strategy_task import run_daily_strategy_snapshot

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@shared_task(name="backend.celery_task.trading_bot_task.run_daily_trading_bot")
def run_daily_trading_bot(user_id: int, report_date: Optional[str] = None):

    try:
        run_date = date.fromisoformat(report_date) if report_date else None

        logger.info(
            f"🤖 Trading Bot Celery task gestart | user_id={user_id} | "
            f"date={run_date or 'today'}"
        )

        # =====================================================
        # 🔥 1️⃣ FORCE STRATEGY SNAPSHOT GENERATION
        # =====================================================
        try:
            logger.info(f"🧠 Generating strategy snapshot | user_id={user_id}")
            run_daily_strategy_snapshot(user_id=user_id)
        except Exception:
            logger.exception(
                f"⚠️ Strategy snapshot generatie mislukt | user_id={user_id}"
            )

        # =====================================================
        # 🔁 2️⃣ Run AI trading bot agent
        # =====================================================
        result = run_trading_bot_agent(
            user_id=user_id,
            report_date=run_date,
        )

        # =====================================================
        # ⚠️ Result check
        # =====================================================
        if not isinstance(result, dict):
            logger.error(
                f"❌ Trading bot gaf ongeldig resultaat | user_id={user_id}"
            )
            return {"ok": False, "error": "invalid_result_type"}

        if not result.get("ok"):
            logger.warning(
                f"⚠️ Trading bot gaf geen ok-result | user_id={user_id}"
            )
            return result

        decisions_count = len(result.get("decisions", []))
        bots_count = result.get("bots", 0)

        # =====================================================
        # 📊 3️⃣ Portfolio Snapshots
        # =====================================================
        try:
            snapshot_all_for_user(user_id, bucket="1h")
            snapshot_all_for_user(user_id, bucket="1d")

            logger.info(
                f"📊 Portfolio snapshots opgeslagen | user_id={user_id}"
            )

        except Exception:
            logger.exception(
                f"⚠️ Portfolio snapshot mislukt | user_id={user_id}"
            )

        logger.info(
            f"✅ Trading Bot klaar | user_id={user_id} | "
            f"bots={bots_count} | decisions={decisions_count}"
        )

        return {
            "ok": True,
            "user_id": user_id,
            "date": str(run_date) if run_date else None,
            "bots": bots_count,
            "decisions": decisions_count,
        }

    except Exception as e:
        logger.exception(
            f"❌ Trading Bot Celery task gecrasht | user_id={user_id}"
        )
        return {
            "ok": False,
            "user_id": user_id,
            "error": str(e),
        }
