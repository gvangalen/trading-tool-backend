# backend/celery_task/trading_bot_task.py

import logging
from datetime import date, datetime
from typing import Optional

from celery import shared_task

from backend.ai_agents.trading_bot_agent import run_trading_bot_agent
from backend.utils.db import get_db_connection
from backend.services.price_service import get_latest_btc_price

# =====================================================
# 🪵 Logging
# =====================================================
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# =====================================================
# 🕒 Helpers
# =====================================================
def floor_to_hour(dt: datetime) -> datetime:
    return dt.replace(minute=0, second=0, microsecond=0)


def snapshot_portfolio_equity(user_id: int, bucket: str = "1h"):
    """
    Slaat globale portfolio equity snapshot op
    in portfolio_balance_snapshots.

    Equity = (net_qty * current_price) + net_cash
    """

    ts = floor_to_hour(datetime.utcnow())

    # Laatste BTC prijs ophalen (single source)
    price = get_latest_btc_price()

    with get_db_connection() as conn:
        with conn.cursor() as cur:

            # =====================================
            # 📊 Ledger-based portfolio berekening
            # =====================================
            cur.execute("""
                SELECT
                    COALESCE(SUM(qty_delta), 0),
                    COALESCE(SUM(cash_delta_eur), 0)
                FROM bot_ledger
                WHERE user_id = %s
            """, (user_id,))

            net_qty, net_cash = cur.fetchone()

            equity = float(net_qty) * float(price) + float(net_cash)

            # =====================================
            # 📝 Snapshot insert/update
            # =====================================
            cur.execute("""
                INSERT INTO portfolio_balance_snapshots
                (user_id, bucket, ts, equity_eur)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (user_id, bucket, ts)
                DO UPDATE SET equity_eur = EXCLUDED.equity_eur
            """, (user_id, bucket, ts, equity))

        conn.commit()


# =====================================================
# 🤖 Trading Bot – Daily Run
# =====================================================
@shared_task(name="backend.celery_task.trading_bot_task.run_daily_trading_bot")
def run_daily_trading_bot(user_id: int, report_date: Optional[str] = None):
    """
    Draait de trading bot agent voor één user.

    Verwachting:
    - daily_scores zijn al berekend
    - bot_configs bestaan
    - agent schrijft:
        - bot_decisions (status = planned)
        - bot_orders (status = ready)
    """

    try:
        run_date = date.fromisoformat(report_date) if report_date else None

        logger.info(
            f"🤖 Trading Bot Celery task gestart | user_id={user_id} | "
            f"date={run_date or 'today'}"
        )

        # =====================================
        # 🔁 Run AI trading bot agent
        # =====================================
        result = run_trading_bot_agent(
            user_id=user_id,
            report_date=run_date,
        )

        # =====================================
        # ⚠️ Result check
        # =====================================
        if not isinstance(result, dict):
            logger.error(
                f"❌ Trading bot gaf ongeldig resultaat | user_id={user_id} | "
                f"type={type(result)}"
            )
            return {"ok": False, "error": "invalid_result_type"}

        if not result.get("ok"):
            logger.warning(
                f"⚠️ Trading bot gaf geen ok-result | user_id={user_id} | "
                f"result={result}"
            )
            return result

        decisions_count = len(result.get("decisions", []))
        bots_count = result.get("bots", 0)

        # =====================================
        # 📊 Portfolio Snapshot
        # =====================================
        try:
            snapshot_portfolio_equity(user_id)
            logger.info(
                f"📊 Portfolio snapshot opgeslagen | user_id={user_id}"
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
