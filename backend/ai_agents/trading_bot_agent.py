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

def _clamp_score(v: Any, *, default: float = 10.0, lo: float = 10.0, hi: float = 100.0) -> float:
    """
    Scores mogen NOOIT 0 zijn.
    - None/NaN/invalid -> default
    - < lo -> lo
    - > hi -> hi
    """
    try:
        x = float(v)
        if x != x:  # NaN check
            x = default
    except Exception:
        x = default

    if x < lo:
        return float(lo)
    if x > hi:
        return float(hi)
    return float(x)


def _confidence_from_score(score: float) -> str:
    s = float(score or 0)
    if s >= 70:
        return "high"
    if s >= 40:
        return "medium"
    return "low"


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

def _get_strategy_trade_amount_eur(
    conn,
    *,
    user_id: int,
    strategy_id: int,
) -> float:
    """
    Single source of truth voor trade sizing:
    → strategies.data -> trade_amount_eur (of amount_eur als fallback)

    Verwachte JSON:
      {"trade_amount_eur": 250}
    """
    if not _table_exists(conn, "strategies"):
        return 0.0

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT data
            FROM strategies
            WHERE id=%s AND user_id=%s
            LIMIT 1
            """,
            (strategy_id, user_id),
        )
        row = cur.fetchone()

    if not row:
        return 0.0

    data = row[0] or {}
    if isinstance(data, str):
        data = _safe_json(data, {})

    if not isinstance(data, dict):
        return 0.0

    val = data.get("trade_amount_eur", None)
    if val is None:
        val = data.get("amount_eur", None)  # fallback key

    try:
        return float(val or 0.0)
    except Exception:
        return 0.0


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

# =====================================================
# 📦 Build setup match
# =====================================================
def _build_setup_match(
    *,
    bot: Dict[str, Any],
    scores: Dict[str, float],
    snapshot: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    ALWAYS return so frontend can always show the card.
    Also: score can NEVER be 0.
    """

    macro = _clamp_score(scores.get("macro", 10), default=10)
    technical = _clamp_score(scores.get("technical", 10), default=10)
    market = _clamp_score(scores.get("market", 10), default=10)
    setup = _clamp_score(scores.get("setup", 10), default=10)

    combined_score = round((macro + technical + market + setup) / 4, 1)
    combined_score = _clamp_score(combined_score, default=10)

    thresholds = _get_risk_thresholds(bot.get("risk_profile", "balanced"))
    buy_th = float(thresholds["buy"])
    hold_th = float(thresholds["hold"])

    has_snapshot = snapshot is not None
    strategy_confidence = _clamp_score(snapshot.get("confidence", 0) if snapshot else 0, default=10)

    match_buy = has_snapshot and (combined_score >= buy_th) and (strategy_confidence >= buy_th)
    match_hold = has_snapshot and (combined_score >= hold_th)

    # -----------------------------
    # Status + reason (UI truth)
    # -----------------------------
    if not has_snapshot:
        status = "no_snapshot"
        reason = "Geen active_strategy_snapshot voor vandaag"
    elif match_buy:
        status = "match_buy"
        reason = "Voldoet aan buy voorwaarden"
    elif match_hold:
        status = "match_hold"
        reason = f"Voldoet aan hold, maar niet aan buy (score {combined_score} < buy {buy_th} of confidence {strategy_confidence} < buy {buy_th})"
    else:
        status = "below_threshold"
        reason = f"Score te laag voor hold: {combined_score} < {hold_th}"

    return {
        "name": bot.get("strategy_type") or bot.get("bot_name") or "Strategy",
        "symbol": bot.get("symbol", DEFAULT_SYMBOL),
        "timeframe": bot.get("timeframe") or "—",
        "score": combined_score,
        "confidence": _confidence_from_score(combined_score),

        "components": {
            "macro": macro,
            "technical": technical,
            "market": market,
            "setup": setup,
        },
        "thresholds": {
            "buy": buy_th,
            "hold": hold_th,
        },

        "strategy_confidence": strategy_confidence,
        "has_snapshot": bool(has_snapshot),
        "match_buy": bool(match_buy),
        "match_hold": bool(match_hold),

        "status": status,
        "reason": reason,
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
    """
    Single source of truth. Returned scores are ALWAYS in [10..100].
    """
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
        return dict(macro=10.0, technical=10.0, market=10.0, setup=10.0)

    macro, technical, market, setup = row

    return {
        "macro": _clamp_score(macro, default=10),
        "technical": _clamp_score(technical, default=10),
        "market": _clamp_score(market, default=10),
        "setup": _clamp_score(setup, default=10),
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
    """
    Snapshot = alleen "wat is vandaag de actuele strategy context".
    NIET sizing. Sizing komt uit strategies.data.
    """
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
    risk_profile = bot.get("risk_profile", "balanced")
    thresholds = _get_risk_thresholds(risk_profile)

    # -------------------------------------------------
    # Scores (NOOIT 0, NOOIT None)
    # -------------------------------------------------
    macro = _clamp_score(scores.get("macro", 10), default=10)
    technical = _clamp_score(scores.get("technical", 10), default=10)
    market = _clamp_score(scores.get("market", 10), default=10)
    setup = _clamp_score(scores.get("setup", 10), default=10)

    combined_score = round((macro + technical + market + setup) / 4, 1)
    combined_score = _clamp_score(combined_score, default=10)

    strategy_confidence = _clamp_score(
        snapshot.get("confidence", 0) if snapshot else 0,
        default=10,
    )

    # -------------------------------------------------
    # Explainable reasons (debug + UI)
    # -------------------------------------------------
    reasons = [
        f"Macro score: {macro}",
        f"Technical score: {technical}",
        f"Market score: {market}",
        f"Setup score: {setup}",
        f"Combined score: {combined_score}",
        f"Strategy confidence: {strategy_confidence}",
        f"Risk profile: {risk_profile}",
        f"Thresholds → buy ≥ {thresholds['buy']} | hold ≥ {thresholds['hold']}",
    ]

    # -------------------------------------------------
    # Geen snapshot = nooit traden
    # -------------------------------------------------
    if not snapshot:
        return {
            "symbol": symbol,
            "action": "observe",
            "confidence": "low",
            "score": combined_score,
            "reasons": reasons[:6],
        }

    # -------------------------------------------------
    # Decision logic
    # -------------------------------------------------
    if (
        combined_score >= thresholds["buy"]
        and strategy_confidence >= thresholds["buy"]
    ):
        action = "buy"
        confidence = "high" if risk_profile != "aggressive" else "medium"

    elif combined_score >= thresholds["hold"]:
        action = "hold"
        confidence = thresholds["min_confidence"]

    else:
        action = "observe"
        confidence = "low"

    return {
        "symbol": symbol,
        "action": action,
        "confidence": confidence,
        "score": combined_score,
        "reasons": reasons[:6],
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
            return {"ok": True, "date": str(report_date), "bots": 0, "decisions": []}

        scores = _get_daily_scores(conn, user_id, report_date)
        results = []

        for bot in bots:
            snapshot = _get_active_strategy_snapshot(
                conn, user_id, bot["strategy_id"], report_date
            )

            # ✅ card data altijd
            setup_match = _build_setup_match(bot=bot, scores=scores, snapshot=snapshot)

            # ✅ decision
            decision = _decide(bot, snapshot, scores)

            # ✅ sizing komt uit strategies.data (niet uit snapshot)
            strategy_amount = _get_strategy_trade_amount_eur(
                conn, user_id=user_id, strategy_id=bot["strategy_id"]
            )

            amount = 0.0
            if decision["action"] == "buy":
                amount = float(strategy_amount or 0)

                min_eur = float(bot["budget"].get("min_order_eur") or 0)
                max_eur = float(bot["budget"].get("max_order_eur") or 0)

                if min_eur > 0:
                    amount = max(amount, min_eur)
                if max_eur > 0:
                    amount = min(amount, max_eur)

            decision["amount_eur"] = float(amount)

            # budget checks
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
                decision.setdefault("reasons", [])
                decision["reasons"].append(f"Budget blokkeert order: {reason}")

            # order preview alleen bij BUY
            order_proposal = build_order_proposal(
                conn=conn,
                bot=bot,
                decision=decision,
                today_spent_eur=today_spent,
                total_balance_eur=total_balance,
            )

            # ⭐ push setup_match in decision object (zodat _persist het kan opslaan)
            decision["setup_match"] = setup_match

            # persist decision + order
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

            # reserve ledger bij proposal
            if order_proposal:
                record_bot_ledger_entry(
                    conn=conn,
                    user_id=user_id,
                    bot_id=bot["bot_id"],
                    entry_type="reserve",
                    cash_delta_eur=-float(decision["amount_eur"]),
                    symbol=decision["symbol"],
                    decision_id=decision_id,
                    meta={
                        "estimated_price": order_proposal.get("estimated_price"),
                        "estimated_qty": order_proposal.get("estimated_qty"),
                    },
                )

            # auto execute enkel als bot.mode == auto + auto_execute flag + proposal
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

            results.append(
                {
                    "bot_id": bot["bot_id"],
                    "decision_id": decision_id,
                    "symbol": decision["symbol"],
                    "action": decision["action"],
                    "confidence": decision["confidence"],
                    "amount_eur": decision["amount_eur"],
                    "reasons": decision.get("reasons", []),

                    # ⭐ altijd aanwezig voor de card
                    "setup_match": setup_match,

                    "order": order_proposal,

                    # status info voor UI
                    "status": status,
                    "executed_by": executed_by,
                }
            )

        conn.commit()
        return {"ok": True, "date": str(report_date), "bots": len(results), "decisions": results}

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
