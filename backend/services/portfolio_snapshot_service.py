import logging
from datetime import datetime
from typing import Literal, List

from backend.utils.db import get_db_connection

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
# 💰 Haal laatste BTC prijs uit market_data
# =====================================================

def _get_latest_btc_price(cur) -> float:
    cur.execute("""
        SELECT price
        FROM market_data
        WHERE symbol = 'BTC'
          AND price IS NOT NULL
        ORDER BY timestamp DESC
        LIMIT 1
    """)
    row = cur.fetchone()

    if not row:
        raise RuntimeError("Geen BTC prijs gevonden in market_data")

    return float(row[0])


# =====================================================
# 🚀 MASTER SNAPSHOT SERVICE
# =====================================================

def snapshot_all_for_user(
    user_id: int,
    bucket: BucketType = "1h",
) -> None:
    """
    Maakt snapshots voor:

        1️⃣ Global portfolio
        2️⃣ Per bot portfolio

    Alles binnen één DB-transaction.
    """

    ts = floor_timestamp(datetime.utcnow(), bucket)

    with get_db_connection() as conn:
        with conn.cursor() as cur:

            # =====================================================
            # 📈 BTC PRIJS
            # =====================================================
            try:
                price = _get_latest_btc_price(cur)
            except Exception:
                logger.exception("❌ Kon BTC prijs niet ophalen voor snapshot")
                return

            # =====================================================
            # 📊 GLOBAL PORTFOLIO SNAPSHOT
            # =====================================================
            cur.execute("""
                SELECT
                    COALESCE(SUM(qty_delta), 0),
                    COALESCE(SUM(cash_delta_eur), 0)
                FROM bot_ledger
                WHERE user_id = %s
            """, (user_id,))

            net_qty, net_cash = cur.fetchone() or (0, 0)

            global_equity = float(net_qty or 0) * price + float(net_cash or 0)

            cur.execute("""
                INSERT INTO portfolio_balance_snapshots
                (user_id, bucket, ts, equity_eur)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (user_id, bucket, ts)
                DO UPDATE SET equity_eur = EXCLUDED.equity_eur
            """, (user_id, bucket, ts, global_equity))

            logger.info(
                f"📊 Global snapshot | user={user_id} | bucket={bucket} | equity={round(global_equity,2)}"
            )

            # =====================================================
            # 🤖 BOT-LEVEL SNAPSHOTS
            # =====================================================
            cur.execute("""
                SELECT id
                FROM bot_configs
                WHERE user_id = %s
                  AND is_active = TRUE
            """, (user_id,))

            bots: List[int] = [r[0] for r in cur.fetchall()]

            for bot_id in bots:

                cur.execute("""
                    SELECT
                        COALESCE(SUM(qty_delta), 0),
                        COALESCE(SUM(cash_delta_eur), 0)
                    FROM bot_ledger
                    WHERE user_id = %s
                      AND bot_id = %s
                """, (user_id, bot_id))

                net_qty, net_cash = cur.fetchone() or (0, 0)

                bot_equity = float(net_qty or 0) * price + float(net_cash or 0)

                cur.execute("""
                    INSERT INTO bot_portfolio_snapshots
                    (user_id, bot_id, bucket, ts, equity_eur)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (user_id, bot_id, bucket, ts)
                    DO UPDATE SET equity_eur = EXCLUDED.equity_eur
                """, (user_id, bot_id, bucket, ts, bot_equity))

                logger.info(
                    f"📊 Bot snapshot | user={user_id} | bot={bot_id} | bucket={bucket} | equity={round(bot_equity,2)}"
                )

        conn.commit()

    logger.info(
        f"📊 Snapshot complete | user={user_id} | bucket={bucket}"
    )
