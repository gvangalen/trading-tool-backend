# backend/services/portfolio_snapshot_service.py

import logging
from datetime import datetime
from typing import Literal

from backend.utils.db import get_db_connection
from backend.services.price_service import get_latest_btc_price

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

BucketType = Literal["1h", "1d"]


# =====================================================
# 🕒 Helpers
# =====================================================

def floor_timestamp(dt: datetime, bucket: BucketType) -> datetime:
    """
    Rond timestamp af op bucket-level.
    """
    if bucket == "1h":
        return dt.replace(minute=0, second=0, microsecond=0)

    if bucket == "1d":
        return dt.replace(hour=0, minute=0, second=0, microsecond=0)

    return dt


# =====================================================
# 📊 GLOBAL PORTFOLIO SNAPSHOT
# =====================================================

def snapshot_global_portfolio(
    user_id: int,
    bucket: BucketType = "1h",
) -> None:
    """
    Slaat totale portfolio equity op in:

        portfolio_balance_snapshots

    Equity = (net_qty * current_price) + net_cash
    """

    ts = floor_timestamp(datetime.utcnow(), bucket)

    price = float(get_latest_btc_price())

    with get_db_connection() as conn:
        with conn.cursor() as cur:

            cur.execute("""
                SELECT
                    COALESCE(SUM(qty_delta), 0),
                    COALESCE(SUM(cash_delta_eur), 0)
                FROM bot_ledger
                WHERE user_id = %s
            """, (user_id,))

            net_qty, net_cash = cur.fetchone()

            equity = float(net_qty) * price + float(net_cash)

            cur.execute("""
                INSERT INTO portfolio_balance_snapshots
                (user_id, bucket, ts, equity_eur)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (user_id, bucket, ts)
                DO UPDATE SET equity_eur = EXCLUDED.equity_eur
            """, (user_id, bucket, ts, equity))

        conn.commit()

    logger.info(
        f"📊 Global snapshot opgeslagen | user={user_id} | bucket={bucket}"
    )


# =====================================================
# 🤖 BOT-LEVEL SNAPSHOT
# =====================================================

def snapshot_bot_portfolio(
    user_id: int,
    bot_id: int,
    bucket: BucketType = "1h",
) -> None:
    """
    Slaat snapshot op per bot in:

        bot_portfolio_snapshots
    """

    ts = floor_timestamp(datetime.utcnow(), bucket)
    price = float(get_latest_btc_price())

    with get_db_connection() as conn:
        with conn.cursor() as cur:

            cur.execute("""
                SELECT
                    COALESCE(SUM(qty_delta), 0),
                    COALESCE(SUM(cash_delta_eur), 0)
                FROM bot_ledger
                WHERE user_id = %s
                AND bot_id = %s
            """, (user_id, bot_id))

            net_qty, net_cash = cur.fetchone()

            equity = float(net_qty) * price + float(net_cash)

            cur.execute("""
                INSERT INTO bot_portfolio_snapshots
                (user_id, bot_id, bucket, ts, equity_eur)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (user_id, bot_id, bucket, ts)
                DO UPDATE SET equity_eur = EXCLUDED.equity_eur
            """, (user_id, bot_id, bucket, ts, equity))

        conn.commit()

    logger.info(
        f"📊 Bot snapshot opgeslagen | user={user_id} | bot={bot_id} | bucket={bucket}"
    )


# =====================================================
# 🚀 MASTER HELPER (MOST USED)
# =====================================================

def snapshot_all_for_user(
    user_id: int,
    bucket: BucketType = "1h",
) -> None:
    """
    1️⃣ Global snapshot
    2️⃣ Snapshot per actieve bot
    """

    snapshot_global_portfolio(user_id, bucket=bucket)

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id
                FROM bot_configs
                WHERE user_id = %s
                AND is_active = true
            """, (user_id,))
            bots = cur.fetchall()

    for (bot_id,) in bots:
        snapshot_bot_portfolio(
            user_id=user_id,
            bot_id=bot_id,
            bucket=bucket,
        )

    logger.info(
        f"📊 Alle snapshots opgeslagen | user={user_id} | bucket={bucket}"
    )
