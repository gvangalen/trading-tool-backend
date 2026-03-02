import logging
from datetime import datetime
from typing import Literal, List, Tuple

from backend.utils.db import get_db_connection

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

BucketType = Literal["1h", "1d"]


# =====================================================
# 🕒 Helpers
# =====================================================

def floor_timestamp(dt: datetime, bucket: BucketType) -> datetime:
    if bucket == "1h":
        return dt.replace(minute=0, second=0, microsecond=0)
    if bucket == "1d":
        return dt.replace(hour=0, minute=0, second=0, microsecond=0)
    return dt


# =====================================================
# 💰 BTC Price
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
        raise RuntimeError("Geen BTC prijs gevonden")
    return float(row[0])


# =====================================================
# 🚀 SNAPSHOT SERVICE (FULL VERSION)
# =====================================================

def snapshot_all_for_user(
    user_id: int,
    bucket: BucketType = "1h",
) -> None:

    ts = floor_timestamp(datetime.utcnow(), bucket)

    with get_db_connection() as conn:
        with conn.cursor() as cur:

            # =====================================================
            # 📈 BTC PRIJS
            # =====================================================
            try:
                price = _get_latest_btc_price(cur)
            except Exception:
                logger.exception("❌ BTC prijs ophalen mislukt")
                return

            # =====================================================
            # 🤖 BOTS + BUDGET
            # =====================================================
            cur.execute("""
                SELECT id, COALESCE(budget_total_eur,0)
                FROM bot_configs
                WHERE user_id=%s
            """, (user_id,))

            bots: List[Tuple[int, float]] = cur.fetchall()

            global_equity = 0.0

            # =====================================================
            # 🔁 PER BOT
            # =====================================================
            for bot_id, budget_total in bots:

                # Ledger totals
                cur.execute("""
                    SELECT
                        COALESCE(SUM(qty_delta),0),
                        COALESCE(SUM(cash_delta_eur),0),
                        COALESCE(SUM(
                            CASE
                                WHEN entry_type='execute'
                                     AND cash_delta_eur < 0
                                THEN ABS(cash_delta_eur)
                                ELSE 0
                            END
                        ),0)
                    FROM bot_ledger
                    WHERE user_id=%s
                      AND bot_id=%s
                """, (user_id, bot_id))

                net_qty, net_cash_delta, invested_eur = cur.fetchone() or (0,0,0)

                net_qty = float(net_qty or 0)
                net_cash_delta = float(net_cash_delta or 0)
                invested_eur = float(invested_eur or 0)
                budget_total = float(budget_total or 0)

                # 🔥 Cash = start budget + ledger delta
                cash_eur = budget_total + net_cash_delta

                # 🔥 Position value
                position_value = net_qty * price

                # 🔥 Equity
                bot_equity = cash_eur + position_value

                global_equity += bot_equity

                # =====================================================
                # 🤖 BOT SNAPSHOT
                # =====================================================
                cur.execute("""
                    INSERT INTO bot_portfolio_snapshots
                    (
                        user_id,
                        bot_id,
                        bucket,
                        ts,
                        symbol,
                        net_qty,
                        cash_eur,
                        price_eur,
                        equity_eur,
                        invested_eur
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (user_id, bot_id, bucket, ts)
                    DO UPDATE SET
                        net_qty      = EXCLUDED.net_qty,
                        cash_eur     = EXCLUDED.cash_eur,
                        price_eur    = EXCLUDED.price_eur,
                        equity_eur   = EXCLUDED.equity_eur,
                        invested_eur = EXCLUDED.invested_eur
                """, (
                    user_id,
                    bot_id,
                    bucket,
                    ts,
                    "BTC",
                    net_qty,
                    cash_eur,
                    price,
                    bot_equity,
                    invested_eur
                ))

                logger.info(
                    f"📊 Bot snapshot | bot={bot_id} | equity={round(bot_equity,2)}"
                )

            # =====================================================
            # 🌍 GLOBAL SNAPSHOT (uitgebreid)
            # =====================================================
            
            global_cash = 0.0
            global_qty = 0.0
            
            for bot_id, budget_total in bots:
            
                cur.execute("""
                    SELECT
                        COALESCE(SUM(qty_delta),0),
                        COALESCE(SUM(cash_delta_eur),0)
                    FROM bot_ledger
                    WHERE user_id=%s
                      AND bot_id=%s
                """, (user_id, bot_id))
            
                net_qty, net_cash_delta = cur.fetchone() or (0, 0)
            
                net_qty = float(net_qty or 0)
                net_cash_delta = float(net_cash_delta or 0)
                budget_total = float(budget_total or 0)
            
                cash_eur = budget_total + net_cash_delta
            
                global_cash += cash_eur
                global_qty += net_qty
            
            global_btc_value = global_qty * price
            global_equity = global_cash + global_btc_value
            
            cur.execute("""
                INSERT INTO portfolio_balance_snapshots
                (
                    user_id,
                    bucket,
                    ts,
                    equity_eur,
                    cash_eur,
                    btc_qty,
                    btc_value_eur
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (user_id, bucket, ts)
                DO UPDATE SET
                    equity_eur    = EXCLUDED.equity_eur,
                    cash_eur      = EXCLUDED.cash_eur,
                    btc_qty       = EXCLUDED.btc_qty,
                    btc_value_eur = EXCLUDED.btc_value_eur
            """, (
                user_id,
                bucket,
                ts,
                global_equity,
                global_cash,
                global_qty,
                global_btc_value
            ))
            
            logger.info(
                f"📊 Global snapshot | equity={round(global_equity,2)} "
                f"| cash={round(global_cash,2)} "
                f"| btc={round(global_qty,6)}"
            )
