import logging
from datetime import date

from celery import shared_task

from backend.ai_agents.trading_bot_agent import run_trading_bot_agent

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# =====================================================
# 🤖 Trading Bot – Daily Run
# =====================================================
@shared_task(name="backend.celery_task.trading_bot_task.run_daily_trading_bot")
def run_daily_trading_bot(user_id: int, report_date: str | None = None):
    """
    Draait de trading bot agent voor één user.

    Verwachting:
    - daily_scores zijn al berekend
    - bot_configs bestaan
    - schrijft:
        - bot_decisions (planned)
        - bot_orders (planned)

    Wordt aangeroepen:
    - na daily_scores_task
    - of handmatig via API (force refresh)
    """

    try:
        d = date.fromisoformat(report_date) if report_date else None

        logger.info(
            f"🤖 Trading Bot Celery task gestart | user_id={user_id} | date={d or 'today'}"
        )

        result = run_trading_bot_agent(
            user_id=user_id,
            report_date=d,
        )

        if not result.get("ok"):
            logger.warning(
                f"⚠️ Trading bot gaf geen ok-result | user_id={user_id} | result={result}"
            )
            return result

        logger.info(
            f"✅ Trading Bot klaar | user_id={user_id} | bots={len(result.get('decisions', []))}"
        )

        return result

    except Exception as e:
        logger.exception(
            f"❌ Trading Bot Celery task gecrasht | user_id={user_id}"
        )
        return {"ok": False, "error": str(e)}
