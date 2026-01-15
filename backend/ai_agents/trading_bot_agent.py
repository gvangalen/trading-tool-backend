# backend/ai_agents/trading_bot_agent.py
import logging
import json
from datetime import date
from typing import Any, Dict, List, Optional

from backend.utils.db import get_db_connection

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

DEFAULT_SYMBOL = "BTC"

ACTIONS = ("buy", "sell", "hold", "observe")
CONFIDENCE_LEVELS = ("low", "medium", "high")


# =====================================================
# 🔧 Helpers
# =====================================================
def _table_exists(conn, table: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema='public'
              AND table_name=%s
            """,
            (table,),
        )
        return cur.fetchone() is not None


def _safe_json(v, fallback):
    if v is None:
        return fallback
    if isinstance(v, (dict, list)):
        return v
    try:
        return json.loads(v)
    except Exception:
        return fallback


def _normalize_action(v: str) -> str:
    v = (v or "").lower().strip()
    return v if v in ACTIONS else "hold"


def _normalize_confidence(v: str) -> str:
    v = (v or "").lower().strip()
    return v if v in CONFIDENCE_LEVELS else "low"


# =====================================================
# 📦 Actieve bots + strategy context
# =====================================================
def _get_active_bots(conn, user_id: int) -> List[Dict[str, Any]]:
    if not _table_exists(conn, "bot_configs"):
        return []

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
              b.id              AS bot_id,
              b.name            AS bot_name,
              b.mode,
              b.strategy_id,

              s.strategy_type,
              st.id             AS setup_id,
              st.symbol,
              st.timeframe

            FROM bot_configs b
            JOIN strategies s ON s.id = b.strategy_id
            JOIN setups st    ON st.id = s.setup_id
            WHERE b.user_id = %s
              AND b.is_active = TRUE
            ORDER BY b.id ASC
            """,
            (user_id,),
        )
        rows = cur.fetchall()

    bots = []
    for r in rows:
        (
            bot_id,
            bot_name,
            mode,
            strategy_id,
            strategy_type,
            setup_id,
            symbol,
            timeframe,
        ) = r

        bots.append(
            {
                "bot_id": bot_id,
                "bot_name": bot_name,
                "mode": mode,
                "strategy_id": strategy_id,
                "strategy_type": strategy_type,
                "setup_id": setup_id,
                "symbol": (symbol or DEFAULT_SYMBOL).upper(),
                "timeframe": timeframe,
            }
        )

    return bots


# =====================================================
# 📊 Daily scores (single source of truth)
# =====================================================
def _get_daily_scores(conn, user_id: int, report_date: date) -> Dict[str, float]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT macro_score, technical_score, market_score, setup_score
            FROM daily_scores
            WHERE user_id=%s
              AND report_date=%s
            LIMIT 1
            """,
            (user_id, report_date),
        )
        row = cur.fetchone()

    if not row:
        return dict(macro=10, technical=10, market=10, setup=10)

    macro, technical, market, setup = row
    return {
        "macro": float(macro or 10),
        "technical": float(technical or 10),
        "market": float(market or 10),
        "setup": float(setup or 10),
    }


# =====================================================
# 📸 Active strategy snapshot (VANDAAG)
# =====================================================
def _get_active_strategy_snapshot(
    conn,
    user_id: int,
    strategy_id: int,
    report_date: date,
) -> Optional[Dict[str, Any]]:
    if not _table_exists(conn, "active_strategy_snapshot"):
        return None

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
              entry,
              targets,
              stop_loss,
              confidence_score,
              adjustment_reason
            FROM active_strategy_snapshot
            WHERE user_id=%s
              AND strategy_id=%s
              AND snapshot_date=%s
            LIMIT 1
            """,
            (user_id, strategy_id, report_date),
        )
        row = cur.fetchone()

    if not row:
        return None

    entry, targets, stop_loss, confidence, reason = row
    return {
        "entry": entry,
        "targets": targets,
        "stop_loss": stop_loss,
        "confidence": float(confidence or 0),
        "reason": reason,
    }

# =====================================================
# 📸 BOT BUDGET
# =====================================================
def check_bot_budget(
    *,
    bot_config: dict,
    today_spent_eur: float,
    proposed_amount_eur: float,
) -> bool:
    """
    True = mag order plaatsen
    False = budget overschreden
    """

    # totaal budget check
    if bot_config["budget"]["total_eur"] > 0:
        if today_spent_eur + proposed_amount_eur > bot_config["budget"]["total_eur"]:
            return False

    # daglimiet check
    if bot_config["budget"]["daily_limit_eur"] > 0:
        if today_spent_eur + proposed_amount_eur > bot_config["budget"]["daily_limit_eur"]:
            return False

    # min / max order check
    if bot_config["budget"]["min_order_eur"] > 0:
        if proposed_amount_eur < bot_config["budget"]["min_order_eur"]:
            return False

    if bot_config["budget"]["max_order_eur"] > 0:
        if proposed_amount_eur > bot_config["budget"]["max_order_eur"]:
            return False

    return True

# =====================================================
# 📸 BOT TODAY SPENT
# =====================================================
def get_today_spent_eur(conn, user_id: int, bot_id: int, report_date):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COALESCE(SUM(quote_amount_eur),0)
            FROM bot_orders
            WHERE user_id=%s
              AND bot_id=%s
              AND DATE(created_at)=%s
              AND status IN ('filled','ready')
            """,
            (user_id, bot_id, report_date),
        )
        return float(cur.fetchone()[0] or 0)


# =====================================================
# 📸 BOT RECORD LEDGER
# =====================================================
def record_bot_ledger_entry(
    *,
    conn,
    user_id,
    bot_id,
    amount_eur,
    type_: str,   # 'reserve', 'execute', 'release'
    ref_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO bot_ledger (
                user_id,
                bot_id,
                type,
                amount_eur,
                ref_id,
                created_at
            )
            VALUES (%s,%s,%s,%s,%s,NOW())
            """,
            (user_id, bot_id, type_, amount_eur, ref_id),
        )


# =====================================================
# 📸 BOT BALANCE
# =====================================================
def get_bot_balance(conn, user_id, bot_id):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
              COALESCE(SUM(
                CASE
                  WHEN type='execute' THEN -amount_eur
                  WHEN type='release' THEN amount_eur
                  ELSE 0
                END
              ),0)
            FROM bot_ledger
            WHERE user_id=%s AND bot_id=%s
            """,
            (user_id, bot_id),
        )
        return float(cur.fetchone()[0] or 0)


# =====================================================
# 🧠 Decision logic (strategy-driven)
# =====================================================
def _decide(
    bot: Dict[str, Any],
    snapshot: Optional[Dict[str, Any]],
    scores: Dict[str, float],
) -> Dict[str, Any]:
    strategy_type = bot["strategy_type"]
    symbol = bot["symbol"]

    if not snapshot:
        return {
            "symbol": symbol,
            "action": "observe",
            "confidence": "low",
            "amount_eur": 0,
            "reasons": ["Geen actieve strategy snapshot"],
        }

    confidence_score = snapshot["confidence"]
    reasons = []

    if confidence_score < 40:
        action = "observe"
        confidence = "low"
    elif confidence_score < 60:
        action = "hold"
        confidence = "medium"
    else:
        action = "buy"
        confidence = "high"

    reasons.append(f"Strategy confidence: {confidence_score}")
    reasons.append(f"Market score: {scores['market']}")
    reasons.append(f"Macro score: {scores['macro']}")

    amount = 0
    if strategy_type == "dca" and action == "buy":
        if scores["market"] < 35:
            amount = 150
        elif scores["market"] < 55:
            amount = 125
        else:
            amount = 100

    return {
        "symbol": symbol,
        "action": action,
        "confidence": confidence,
        "amount_eur": amount,
        "reasons": reasons[:4],
    }


# =====================================================
# 💾 Persist decision + paper order
# =====================================================
def _persist_decision_and_order(
    conn,
    user_id: int,
    bot_id: int,
    strategy_id: int,
    setup_id: int,
    report_date: date,
    decision: Dict[str, Any],
    scores: Dict[str, float],
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO bot_decisions (
              user_id, bot_id, symbol, decision_date, decision_ts,
              action, confidence, scores_json, reason_json,
              setup_id, strategy_id, status, created_at, updated_at
            )
            VALUES (
              %s,%s,%s,%s,NOW(),
              %s,%s,%s::jsonb,%s::jsonb,
              %s,%s,'planned',NOW(),NOW()
            )
            ON CONFLICT (user_id, bot_id, decision_date)
            DO UPDATE SET
              action=EXCLUDED.action,
              confidence=EXCLUDED.confidence,
              scores_json=EXCLUDED.scores_json,
              reason_json=EXCLUDED.reason_json,
              setup_id=EXCLUDED.setup_id,
              strategy_id=EXCLUDED.strategy_id,
              status='planned',
              decision_ts=NOW(),
              updated_at=NOW()
            RETURNING id
            """,
            (
                user_id,
                bot_id,
                decision["symbol"],
                report_date,
                decision["action"],
                decision["confidence"],
                json.dumps(scores),
                json.dumps(decision["reasons"]),
                setup_id,
                strategy_id,
            ),
        )
        decision_id = cur.fetchone()[0]

    if (
        decision["action"] in ("buy", "sell")
        and decision.get("amount_eur", 0) > 0
        and _table_exists(conn, "bot_orders")
    ):
        with conn.cursor() as cur:
            cur.execute("DELETE FROM bot_orders WHERE decision_id=%s", (decision_id,))
            cur.execute(
                """
                INSERT INTO bot_orders (
                  user_id, bot_id, decision_id, symbol,
                  side, order_type, quote_amount_eur,
                  dry_run_payload, status, created_at, updated_at
                )
                VALUES (
                  %s,%s,%s,%s,
                  %s,'market',%s,
                  %s::jsonb,'ready',NOW(),NOW()
                )
                """,
                (
                    user_id,
                    bot_id,
                    decision_id,
                    decision["symbol"],
                    decision["action"],
                    decision["amount_eur"],
                    json.dumps(
                        {
                            "symbol": decision["symbol"],
                            "action": decision["action"],
                            "amount_eur": decision["amount_eur"],
                        }
                    ),
                ),
            )

    return decision_id


# =====================================================
# 🚀 PUBLIC ENTRYPOINT
# =====================================================
def run_trading_bot_agent(
    user_id: int,
    report_date: Optional[date] = None,
) -> Dict[str, Any]:
    report_date = report_date or date.today()
    conn = get_db_connection()
    if not conn:
        return {"ok": False, "error": "db_unavailable"}

    try:
        bots = _get_active_bots(conn, user_id)
        if not bots:
            return {"ok": True, "date": str(report_date), "bots": 0, "decisions": []}

        scores = _get_daily_scores(conn, user_id, report_date)
        decisions = []

        for bot in bots:
            snapshot = _get_active_strategy_snapshot(
                conn,
                user_id,
                bot["strategy_id"],
                report_date,
            )

            decision = _decide(bot, snapshot, scores)

            decision_id = _persist_decision_and_order(
                conn=conn,
                user_id=user_id,
                bot_id=bot["bot_id"],
                strategy_id=bot["strategy_id"],
                setup_id=bot["setup_id"],
                report_date=report_date,
                decision=decision,
                scores=scores,
            )

            decisions.append(
                {
                    "bot_id": bot["bot_id"],
                    "bot_name": bot["bot_name"],
                    "strategy_id": bot["strategy_id"],
                    "action": decision["action"],
                    "confidence": decision["confidence"],
                    "amount_eur": decision["amount_eur"],
                    "reasons": decision["reasons"],
                    "decision_id": decision_id,
                }
            )

        conn.commit()
        return {
            "ok": True,
            "date": str(report_date),
            "bots": len(decisions),
            "decisions": decisions,
        }

    except Exception:
        conn.rollback()
        logger.exception("❌ trading_bot_agent crash")
        return {"ok": False, "error": "crash"}
    finally:
        conn.close()
