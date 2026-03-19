# backend/celery_task/trading_bot_task.py

import logging
from datetime import date
from typing import Optional

from celery import shared_task

from backend.ai_agents.trading_bot_agent import run_trading_bot_agent
from backend.services.portfolio_snapshot_service import snapshot_all_for_user
from backend.celery_task.strategy_task import run_daily_strategy_snapshot
from backend.db import get_db_connection  # jouw db helper

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

    # -------------------------------------------------
    # 🗓️ DATE NORMALIZATION
    # -------------------------------------------------
    try:
        run_date = date.fromisoformat(report_date) if report_date else date.today()
    except Exception:
        run_date = date.today()

    logger.info(
        f"🤖 Trading Bot START | user_id={user_id} | date={run_date}"
    )

    try:
        # =====================================================
        # 🔥 1️⃣ STRATEGY SNAPSHOT
        # =====================================================
        try:
            logger.info(
                f"🧠 Strategy snapshot start | user_id={user_id}"
            )

            run_daily_strategy_snapshot(user_id=user_id)

            logger.info(
                f"🧠 Strategy snapshot DONE | user_id={user_id}"
            )

        except Exception:
            logger.exception(
                f"⚠️ Strategy snapshot FAILED | user_id={user_id}"
            )

        # =====================================================
        # 🔁 2️⃣ RUN BOT AGENT
        # =====================================================
        result = run_trading_bot_agent(
            user_id=user_id,
            report_date=run_date,
        )

        if not isinstance(result, dict):
            logger.error(
                f"❌ Invalid bot result type | user_id={user_id}"
            )
            return {"ok": False, "error": "invalid_result_type"}

        if not result.get("ok"):
            logger.warning(
                f"⚠️ Bot returned not ok | user_id={user_id}"
            )
            return result

        decisions_count = len(result.get("decisions", []))
        bots_count = result.get("bots", 0)

        # 🔥 BELANGRIJK: bot_ids ophalen (moet uit je agent komen)
        bot_ids = result.get("bot_ids", [])

        # =====================================================
        # 📊 3️⃣ PORTFOLIO SNAPSHOT
        # =====================================================
        try:
            logger.info(
                f"📊 Portfolio snapshot start | user_id={user_id}"
            )

            snapshot_all_for_user(user_id, bucket="1h")
            snapshot_all_for_user(user_id, bucket="1d")

            logger.info(
                f"📊 Portfolio snapshot DONE | user_id={user_id}"
            )

        except Exception:
            logger.exception(
                f"⚠️ Portfolio snapshot FAILED | user_id={user_id}"
            )

        # =====================================================
        # 🔥 4️⃣ LAST RUN UPDATE (CORRECT)
        # =====================================================
        try:
            conn = get_db_connection()
            cur = conn.cursor()

            if bot_ids:
                # ✅ update per bot
                for bot_id in bot_ids:
                    cur.execute(
                        """
                        UPDATE bot_configs
                        SET last_run = NOW(),
                            updated_at = NOW()
                        WHERE id = %s
                        """,
                        (bot_id,),
                    )
            else:
                # ⚠️ fallback (als agent geen bot_ids terugstuurt)
                cur.execute(
                    """
                    UPDATE bot_configs
                    SET last_run = NOW(),
                        updated_at = NOW()
                    WHERE user_id = %s
                    """,
                    (user_id,),
                )

            conn.commit()
            cur.close()
            conn.close()

            logger.info(
                f"⏱️ Last run updated | user_id={user_id} | bots={len(bot_ids)}"
            )

        except Exception:
            logger.exception(
                f"⚠️ Last run update FAILED | user_id={user_id}"
            )

        # =====================================================
        # ✅ DONE
        # =====================================================
        logger.info(
            f"✅ Trading Bot DONE | user_id={user_id} | bots={bots_count} | decisions={decisions_count}"
        )

        return {
            "ok": True,
            "user_id": user_id,
            "date": str(run_date),
            "bots": bots_count,
            "decisions": decisions_count,
        }

    except Exception as e:
        logger.exception(
            f"❌ Trading Bot CRASH | user_id={user_id}"
        )

        return {
            "ok": False,
            "user_id": user_id,
            "error": str(e),
        }
