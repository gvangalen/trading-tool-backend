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

              COALESCE(b.budget_total_eur, 0),
              COALESCE(b.budget_daily_limit_eur, 0),
              COALESCE(b.budget_min_order_eur, 0),
              COALESCE(b.budget_max_order_eur, 0),

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
            budget_total_eur,
            budget_daily_limit_eur,
            budget_min_order_eur,
            budget_max_order_eur,
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
                "budget": {
                    "total_eur": float(budget_total_eur or 0),
                    "daily_limit_eur": float(budget_daily_limit_eur or 0),
                    "min_order_eur": float(budget_min_order_eur or 0),
                    "max_order_eur": float(budget_max_order_eur or 0),
                },
            }
        )

    return bots

# =====================================================
# 📦 Bot Proposal
# =====================================================
def build_order_proposal(
    *,
    conn,
    bot: dict,
    decision: dict,
    today_spent_eur: float,
    total_balance_eur: float,
) -> Optional[dict]:
    """
    Builds a concrete order preview for today.
    Returns None if no order should be proposed.
    """

    if decision["action"] != "buy" or decision.get("amount_eur", 0) <= 0:
        return None

    symbol = decision["symbol"]

    # 1️⃣ haal indicatieve marktprijs op
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT price_eur
            FROM market_data
            WHERE symbol=%s
            ORDER BY ts DESC
            LIMIT 1
            """,
            (symbol,),
        )
        row = cur.fetchone()

    if not row:
        return None

    price_eur = float(row[0])
    amount_eur = float(decision["amount_eur"])

    # 2️⃣ bereken hoeveelheid
    estimated_qty = round(amount_eur / price_eur, 8)

    # 3️⃣ budget impact simuleren
    daily_limit = float(bot["budget"].get("daily_limit_eur", 0))
    total_budget = float(bot["budget"].get("total_eur", 0))

    daily_remaining = (
        max(0, daily_limit - (today_spent_eur + amount_eur))
        if daily_limit > 0
        else None
    )

    total_remaining = (
        max(0, total_budget - (total_balance_eur + amount_eur))
        if total_budget > 0
        else None
    )

    return {
        "symbol": symbol,
        "side": "buy",
        "quote_amount_eur": amount_eur,
        "estimated_price": price_eur,
        "estimated_qty": estimated_qty,
        "status": "ready",
        "budget_after": {
            "daily_remaining": daily_remaining,
            "total_remaining": total_remaining,
        },
    }


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
):
    budget = bot_config.get("budget") or {}

    total_eur = float(budget.get("total_eur") or 0)
    daily_limit_eur = float(budget.get("daily_limit_eur") or 0)
    min_order_eur = float(budget.get("min_order_eur") or 0)
    max_order_eur = float(budget.get("max_order_eur") or 0)

    if proposed_amount_eur <= 0:
        return True, None

    if daily_limit_eur > 0 and (today_spent_eur + proposed_amount_eur) > daily_limit_eur:
        return False, "Daglimiet overschreden"

    if min_order_eur > 0 and proposed_amount_eur < min_order_eur:
        return False, "Onder minimum orderbedrag"

    if max_order_eur > 0 and proposed_amount_eur > max_order_eur:
        return False, "Boven maximum orderbedrag"

    return True, None


# =====================================================
# 📸 BOT TODAY SPENT
# =====================================================
def get_today_spent_eur(
    conn,
    user_id: int,
    bot_id: int,
    report_date: date,
) -> float:
    """
    Total EUR spent TODAY by this bot (from ledger)
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COALESCE(SUM(cash_delta_eur), 0)
            FROM bot_ledger
            WHERE user_id = %s
              AND bot_id = %s
              AND cash_delta_eur < 0
              AND DATE(ts) = %s
            """,
            (user_id, bot_id, report_date),
        )
        return abs(float(cur.fetchone()[0] or 0.0))

# =====================================================
# 📸 BOT RECORD LEDGER
# =====================================================
def record_bot_ledger_entry(
    *,
    conn,
    user_id: int,
    bot_id: int,
    entry_type: str,
    cash_delta_eur: float = 0.0,
    qty_delta: float = 0.0,
    symbol: str = DEFAULT_SYMBOL,
    decision_id: Optional[int] = None,
    order_id: Optional[int] = None,
    note: Optional[str] = None,
    meta: Optional[dict] = None,
):
    """
    Single source of truth for all bot balance changes
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO bot_ledger (
                user_id,
                bot_id,
                decision_id,
                order_id,
                entry_type,
                symbol,
                cash_delta_eur,
                qty_delta,
                note,
                meta,
                ts
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
            """,
            (
                user_id,
                bot_id,
                decision_id,
                order_id,
                entry_type,
                symbol,
                cash_delta_eur,
                qty_delta,
                note,
                json.dumps(meta or {}),
            ),
        )

# =====================================================
# 📸 BOT BALANCE
# =====================================================
def get_bot_balance(conn, user_id: int, bot_id: int) -> float:
    """
    Net EUR balance delta for this bot (ledger-based)
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COALESCE(SUM(cash_delta_eur), 0)
            FROM bot_ledger
            WHERE user_id = %s
              AND bot_id = %s
            """,
            (user_id, bot_id),
        )
        return float(cur.fetchone()[0] or 0.0)


# =====================================================
# 🧠 Decision logic (strategy-driven)
# =====================================================
def _decide(
    bot: Dict[str, Any],
    snapshot: Optional[Dict[str, Any]],
    scores: Dict[str, float],
) -> Dict[str, Any]:
    symbol = bot["symbol"]

    if not snapshot:
        return {
            "symbol": symbol,
            "action": "observe",
            "confidence": "low",
            "reasons": ["Geen actieve strategy snapshot"],
        }

    confidence_score = float(snapshot.get("confidence", 0))
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
    reasons.append(f"Market score: {scores.get('market')}")
    reasons.append(f"Macro score: {scores.get('macro')}")

    return {
        "symbol": symbol,
        "action": action,
        "confidence": confidence,
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
# 🚀 PUBLIC ENTRYPOINT (PER BOT ONDERSTEUND)
# =====================================================
# =====================================================
# 🚀 PUBLIC ENTRYPOINT (PER BOT ONDERSTEUND)
# =====================================================
def run_trading_bot_agent(
    user_id: int,
    report_date: Optional[date] = None,
    bot_id: Optional[int] = None,
) -> Dict[str, Any]:

    report_date = report_date or date.today()
    conn = get_db_connection()
    if not conn:
        return {"ok": False, "error": "db_unavailable"}

    try:
        # ==========================================
        # 📦 Actieve bots
        # ==========================================
        bots = _get_active_bots(conn, user_id)

        if bot_id is not None:
            bots = [b for b in bots if b["bot_id"] == bot_id]

        if not bots:
            return {
                "ok": True,
                "date": str(report_date),
                "bots": 0,
                "decisions": [],
            }

        # ==========================================
        # 📊 Daily scores (single source)
        # ==========================================
        scores = _get_daily_scores(conn, user_id, report_date)
        results = []

        # ==========================================
        # 🔁 PER BOT
        # ==========================================
        for bot in bots:
            # --------------------------------------
            # 📸 Strategy snapshot
            # --------------------------------------
            snapshot = _get_active_strategy_snapshot(
                conn,
                user_id,
                bot["strategy_id"],
                report_date,
            )

            # --------------------------------------
            # 🧠 Decision (abstract)
            # --------------------------------------
            decision = _decide(bot, snapshot, scores)

            # --------------------------------------
            # 💰 Amount bepalen
            # --------------------------------------
            amount = 0.0
            if decision["action"] == "buy" and snapshot:
                strategy_amount = float(
                    snapshot.get("amount_per_trade", 0) or 0
                )

                min_eur = float(bot["budget"].get("min_order_eur", 0) or 0)
                max_eur = float(bot["budget"].get("max_order_eur", 0) or 0)

                amount = strategy_amount
                if min_eur > 0:
                    amount = max(amount, min_eur)
                if max_eur > 0:
                    amount = min(amount, max_eur)

            decision["amount_eur"] = float(amount)

            # --------------------------------------
            # 📊 Budget checks
            # --------------------------------------
            today_spent = get_today_spent_eur(
                conn,
                user_id,
                bot["bot_id"],
                report_date,
            )

            total_balance = abs(
                get_bot_balance(conn, user_id, bot["bot_id"])
            )

            ok, reason = check_bot_budget(
                bot_config=bot,
                today_spent_eur=today_spent,
                proposed_amount_eur=decision["amount_eur"],
            )

            if not ok:
                decision["action"] = "observe"
                decision["confidence"] = "low"
                decision["amount_eur"] = 0.0
                decision.setdefault("reasons", [])
                decision["reasons"].append(
                    f"Budget blokkeert order: {reason}"
                )

            # --------------------------------------
            # 🧾 ORDER PROPOSAL (nieuw)
            # --------------------------------------
            order_proposal = build_order_proposal(
                conn=conn,
                bot=bot,
                decision=decision,
                today_spent_eur=today_spent,
                total_balance_eur=total_balance,
            )

            # --------------------------------------
            # 💾 Persist decision (+ paper order)
            # --------------------------------------
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

            # --------------------------------------
            # 📒 Ledger reserve (alleen bij voorstel)
            # --------------------------------------
            if order_proposal:
                record_bot_ledger_entry(
                    conn=conn,
                    user_id=user_id,
                    bot_id=bot["bot_id"],
                    entry_type="reserve",
                    cash_delta_eur=-decision["amount_eur"],
                    qty_delta=0.0,
                    symbol=decision["symbol"],
                    decision_id=decision_id,
                    note="Reserved by bot proposal",
                    meta={
                        "estimated_price": order_proposal.get("estimated_price"),
                        "estimated_qty": order_proposal.get("estimated_qty"),
                    },
                )

            # --------------------------------------
            # 📤 Result voor frontend
            # --------------------------------------
            results.append(
                {
                    "bot_id": bot["bot_id"],
                    "decision_id": decision_id,
                    "symbol": decision["symbol"],
                    "action": decision["action"],
                    "confidence": decision["confidence"],
                    "amount_eur": decision["amount_eur"],
                    "reasons": decision.get("reasons", []),
                    "order": order_proposal,  # ⭐ dit is de UI-truth
                }
            )

        conn.commit()

        return {
            "ok": True,
            "date": str(report_date),
            "bots": len(results),
            "decisions": results,
        }

    except Exception:
        conn.rollback()
        logger.exception("❌ trading_bot_agent crash")
        return {"ok": False, "error": "crash"}

    finally:
        conn.close()
