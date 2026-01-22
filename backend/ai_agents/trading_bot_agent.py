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
# 📦 Risk profiles
# =====================================================
def _get_risk_thresholds(risk_profile: str) -> dict:
    """
    Defines decision thresholds per risk profile.
    """
    profile = (risk_profile or "balanced").lower()

    if profile == "conservative":
        return {
            "buy": 75,
            "hold": 55,
            "min_confidence": "high",
        }

    if profile == "aggressive":
        return {
            "buy": 35,
            "hold": 20,
            "min_confidence": "medium",
        }

    # default: balanced
    return {
        "buy": 55,
        "hold": 40,
        "min_confidence": "medium",
    }

def _build_setup_match(
    *,
    bot: Dict[str, Any],
    scores: Dict[str, float],
) -> Dict[str, Any]:
    """
    ALTIJD teruggeven zodat frontend altijd de card kan tonen.
    Dit is 'strategy/setup match vandaag' (bot score vs totale marktscore).
    """

    macro = float(scores.get("macro", 10))
    technical = float(scores.get("technical", 10))
    market = float(scores.get("market", 10))
    setup = float(scores.get("setup", 10))

    combined_score = round((macro + technical + market + setup) / 4, 1)

    thresholds = _get_risk_thresholds(bot.get("risk_profile", "balanced"))

    return {
        "name": bot.get("strategy_type") or bot.get("bot_name") or "Strategy",
        "symbol": bot.get("symbol", DEFAULT_SYMBOL),
        "timeframe": bot.get("timeframe") or "—",
        "score": combined_score,
        "components": {
            "macro": macro,
            "technical": technical,
            "market": market,
            "setup": setup,
        },
        "thresholds": {
            "buy": float(thresholds["buy"]),
            "hold": float(thresholds["hold"]),
        },
    }


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
              b.risk_profile,
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
            risk_profile,        # ✅ FIX
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
                "risk_profile": risk_profile or "balanced",
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
# 📦 Bot Proposal (MARKET_DATA FIXED)
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

    # Alleen BUY voorstellen
    if decision.get("action") != "buy":
        return None

    amount_eur = float(decision.get("amount_eur") or 0)
    if amount_eur <= 0:
        return None

    symbol = decision.get("symbol", DEFAULT_SYMBOL)

    # -------------------------------------------------
    # 📈 Laatste marktprijs ophalen (CORRECT SCHEMA)
    # -------------------------------------------------
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT price
            FROM market_data
            WHERE symbol = %s
              AND price IS NOT NULL
            ORDER BY timestamp DESC
            LIMIT 1
            """,
            (symbol,),
        )
        row = cur.fetchone()

    if not row or row[0] is None:
        logger.warning(f"⚠️ Geen marktprijs gevonden voor {symbol}")
        return None

    price_eur = float(row[0])

    # -------------------------------------------------
    # 📐 Geschatte hoeveelheid
    # -------------------------------------------------
    estimated_qty = round(amount_eur / price_eur, 8)

    # -------------------------------------------------
    # 💰 Budget impact simulatie
    # -------------------------------------------------
    budget = bot.get("budget", {})

    daily_limit = float(budget.get("daily_limit_eur") or 0)
    total_budget = float(budget.get("total_eur") or 0)

    daily_remaining = (
        round(max(0.0, daily_limit - (today_spent_eur + amount_eur)), 2)
        if daily_limit > 0
        else None
    )

    total_remaining = (
        round(max(0.0, total_budget - (total_balance_eur + amount_eur)), 2)
        if total_budget > 0
        else None
    )

    # -------------------------------------------------
    # 📦 Proposal object (frontend truth)
    # -------------------------------------------------
    return {
        "symbol": symbol,
        "side": "buy",
        "quote_amount_eur": round(amount_eur, 2),
        "estimated_price": round(price_eur, 2),
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
              adjustment_reason,
              amount_per_trade
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

    entry, targets, stop_loss, confidence, reason, amount_per_trade = row
    return {
        "entry": entry,
        "targets": targets,
        "stop_loss": stop_loss,
        "confidence": float(confidence or 0),
        "reason": reason,
        "amount_per_trade": float(amount_per_trade or 0),
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
    risk_profile = bot.get("risk_profile", "balanced")
    thresholds = _get_risk_thresholds(risk_profile)

    # Scores
    macro = float(scores.get("macro", 10))
    technical = float(scores.get("technical", 10))
    market = float(scores.get("market", 10))
    setup = float(scores.get("setup", 10))

    combined_score = round((macro + technical + market + setup) / 4, 1)

    # Strategy confidence
    strategy_confidence = float(snapshot.get("confidence", 0)) if snapshot else 0

    # Default
    action = "observe"
    confidence = "low"

    if snapshot:
        if combined_score >= thresholds["buy"] and strategy_confidence >= thresholds["buy"]:
            action = "buy"
            confidence = "high"
        elif combined_score >= thresholds["hold"]:
            action = "hold"
            confidence = thresholds["min_confidence"]

    return {
        "symbol": symbol,
        "action": action,
        "confidence": confidence,
        "score": combined_score,   # 🔥 DIT MISSTE
        "setup_match": {           # 🔥 DIT MISSTE
            "name": bot["strategy_type"],
            "symbol": bot["symbol"],
            "timeframe": bot["timeframe"],
            "score": combined_score,
            "min_required": thresholds["buy"],
        },
    }

# =====================================================
# 🚀 PUBLIC ENTRYPOINT (PER BOT ONDERSTEUND)
# =====================================================
def run_trading_bot_agent(
    user_id: int,
    report_date: Optional[date] = None,
    bot_id: Optional[int] = None,
    auto_execute: bool = False,
) -> Dict[str, Any]:

    report_date = report_date or date.today()
    conn = get_db_connection()
    if not conn:
        return {"ok": False, "error": "db_unavailable"}

    try:
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

        # ✅ single source scores
        scores = _get_daily_scores(conn, user_id, report_date)
        results = []

        for bot in bots:
            snapshot = _get_active_strategy_snapshot(
                conn,
                user_id,
                bot["strategy_id"],
                report_date,
            )

            # ✅ ALTIJD: setup/strategy match card data
            setup_match = _build_setup_match(bot=bot, scores=scores)

            # ✅ decision (buy/hold/observe) op basis van snapshot + scores
            decision = _decide(bot, snapshot, scores)

            # 💰 amount bepalen (alleen relevant bij BUY)
            amount = 0.0
            if decision["action"] == "buy" and snapshot:
                amount = float(snapshot.get("amount_per_trade") or 0)

                min_eur = float(bot["budget"].get("min_order_eur") or 0)
                max_eur = float(bot["budget"].get("max_order_eur") or 0)

                if min_eur > 0:
                    amount = max(amount, min_eur)
                if max_eur > 0:
                    amount = min(amount, max_eur)

            decision["amount_eur"] = float(amount)

            # 📊 budget checks
            today_spent = get_today_spent_eur(conn, user_id, bot["bot_id"], report_date)
            total_balance = abs(get_bot_balance(conn, user_id, bot["bot_id"]))

            ok, reason = check_bot_budget(
                bot_config=bot,
                today_spent_eur=today_spent,
                proposed_amount_eur=decision["amount_eur"],
            )

            if not ok:
                decision["action"] = "observe"
                decision["confidence"] = "low"
                decision["amount_eur"] = 0.0
                decision.setdefault("reasons", []).append(f"Budget blokkeert order: {reason}")

            # 🧾 order preview (alleen bij BUY)
            order_proposal = build_order_proposal(
                conn=conn,
                bot=bot,
                decision=decision,
                today_spent_eur=today_spent,
                total_balance_eur=total_balance,
            )

            # 💾 persist decision + order
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

            # 📒 reserve ledger (alleen bij proposal)
            if order_proposal:
                record_bot_ledger_entry(
                    conn=conn,
                    user_id=user_id,
                    bot_id=bot["bot_id"],
                    entry_type="reserve",
                    cash_delta_eur=-decision["amount_eur"],
                    symbol=decision["symbol"],
                    decision_id=decision_id,
                    meta={
                        "estimated_price": order_proposal.get("estimated_price"),
                        "estimated_qty": order_proposal.get("estimated_qty"),
                    },
                )

            # 🤖 AUTO MODE: alleen uitvoeren als bot.mode == auto
            executed_by = None
            status = "planned"
            if bot.get("mode") == "auto" and auto_execute and order_proposal:
                _auto_execute_decision(
                    conn=conn,
                    user_id=user_id,
                    bot_id=bot["bot_id"],
                    decision_id=decision_id,
                    order=order_proposal,
                )
                executed_by = "auto"
                status = "executed"

            # ✅ Return payload voor frontend (setup_match altijd!)
            results.append(
                {
                    "bot_id": bot["bot_id"],
                    "decision_id": decision_id,
                    "symbol": decision["symbol"],
                    "action": decision["action"],
                    "confidence": decision["confidence"],
                    "amount_eur": decision["amount_eur"],
                    "reasons": decision.get("reasons", []),

                    # ⭐ CRUCIAAL: card data altijd aanwezig
                    "setup_match": setup_match,

                    # order (kan None zijn)
                    "order": order_proposal,

                    # status info voor UI
                    "status": status if executed_by else "planned",
                    "executed_by": executed_by,
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

# =====================================================
# 🚀 Bot exute decision functie
# =====================================================
def _auto_execute_decision(
    *,
    conn,
    user_id: int,
    bot_id: int,
    decision_id: int,
    order: dict,
):
    """
    Marks decision + order as executed by AUTO.
    Single source of truth.
    """

    with conn.cursor() as cur:
        # ✅ decision executed
        cur.execute(
            """
            UPDATE bot_decisions
            SET
              status='executed',
              executed_by='auto',
              executed_at=NOW(),
              updated_at=NOW()
            WHERE id=%s
              AND user_id=%s
              AND bot_id=%s
              AND status='planned'
            """,
            (decision_id, user_id, bot_id),
        )

        # ✅ order filled
        if _table_exists(conn, "bot_orders"):
            cur.execute(
                """
                UPDATE bot_orders
                SET
                  status='filled',
                  updated_at=NOW()
                WHERE decision_id=%s
                  AND user_id=%s
                  AND bot_id=%s
                RETURNING id
                """,
                (decision_id, user_id, bot_id),
            )
            row = cur.fetchone()
            order_id = row[0] if row else None

        else:
            order_id = None

        # ✅ ledger execution
        if order_id:
            record_bot_ledger_entry(
                conn=conn,
                user_id=user_id,
                bot_id=bot_id,
                entry_type="execute",
                cash_delta_eur=-float(order.get("quote_amount_eur") or 0),
                qty_delta=float(order.get("estimated_qty") or 0),
                symbol=order.get("symbol", DEFAULT_SYMBOL),
                decision_id=decision_id,
                order_id=order_id,
                note="Auto executed by bot",
                meta={"mode": "auto"},
            )
