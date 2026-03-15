# backend/ai_agents/trading_bot_agent.py
import logging
import json
from datetime import date
from typing import Any, Dict, List, Optional

from backend.utils.db import get_db_connection
from backend.engine.guardrails_engine import apply_guardrails

# ✅ Engine brain (single source of truth)
from backend.engine.bot_brain import run_bot_brain


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
        if x != x:  # NaN
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


def _normalize_action(v: str) -> str:
    v = (v or "").lower().strip()
    return v if v in ACTIONS else "hold"


def _normalize_confidence(v: str) -> str:
    v = (v or "").lower().strip()
    return v if v in CONFIDENCE_LEVELS else "low"


def _safe_json(v, fallback):
    if v is None:
        return fallback
    if isinstance(v, (dict, list)):
        return v
    try:
        return json.loads(v)
    except Exception:
        return fallback

# =====================================================
# 📊 Asset position value (per symbol)
# =====================================================
def get_asset_position_value(
    conn,
    user_id: int,
    bot_id: int,
    symbol: str,
) -> float:
    """
    Returns current EUR value of a specific asset position.
    Used for asset exposure guardrails.
    """

    symbol = (symbol or DEFAULT_SYMBOL).upper()

    with conn.cursor() as cur:

        # current qty
        cur.execute(
            """
            SELECT COALESCE(SUM(qty_delta), 0)
            FROM bot_ledger
            WHERE user_id = %s
              AND bot_id = %s
              AND symbol = %s
            """,
            (user_id, bot_id, symbol),
        )

        qty = float(cur.fetchone()[0] or 0)

        if qty <= 0:
            return 0.0

        # latest market price
        cur.execute(
            """
            SELECT price
            FROM market_data
            WHERE symbol=%s
              AND price IS NOT NULL
            ORDER BY timestamp DESC
            LIMIT 1
            """,
            (symbol,),
        )

        row = cur.fetchone()

        if not row:
            return 0.0

        price = float(row[0] or 0)

    return round(qty * price, 2)


# =====================================================
# ✅ Default trade plan
# =====================================================
def _default_trade_plan(
    symbol: str,
    action: str,
    reason: str = "no_trade_today",
    watch_levels: Optional[dict] = None,
    snapshot: Optional[dict] = None,
) -> dict:
    """
    Hard UI contract:
    - Elke decision heeft een trade_plan
    - Ook observe / hold
    - Als brain geen levels geeft → gebruik snapshot levels
    """

    symbol = (symbol or DEFAULT_SYMBOL).upper()
    side = (action or "observe").lower()

    entry_plan = []
    targets = []
    stop_loss = {"price": None}

    # 1️⃣ Brain watch levels
    if watch_levels:
        pullback = watch_levels.get("pullback_zone")
        breakout = watch_levels.get("breakout_trigger")
        entry = watch_levels.get("entry")

        if pullback:
            entry_plan.append({
                "type": "watch",
                "label": "Observe pullback zone",
                "price": pullback,
            })

        if breakout:
            entry_plan.append({
                "type": "watch",
                "label": "Watch breakout",
                "price": breakout,
            })

        if entry and not entry_plan:
            entry_plan.append({
                "type": "watch",
                "label": "Potential entry",
                "price": entry,
            })

        stop = watch_levels.get("stop_loss")
        if stop:
            stop_loss = {"price": stop}

        watch_targets = watch_levels.get("targets") or []
        for i, t in enumerate(watch_targets):
            if t is not None:
                targets.append({
                    "label": f"TP{i+1}",
                    "price": t,
                })

    # 2️⃣ Snapshot fallback
    if snapshot:
        entry = snapshot.get("entry")
        stop = snapshot.get("stop_loss")
        tps = snapshot.get("targets") or []

        if entry is not None and not entry_plan:
            entry_plan.append({
                "type": "watch",
                "label": "Potential entry",
                "price": entry,
            })

        if stop is not None and stop_loss.get("price") is None:
            stop_loss = {"price": stop}

        if not targets:
            for i, t in enumerate(tps):
                if t is not None:
                    targets.append({
                        "label": f"TP{i+1}",
                        "price": t,
                    })

    return {
        "symbol": symbol,
        "side": side,
        "entry_plan": entry_plan,
        "stop_loss": stop_loss,
        "targets": targets,
        "risk": {
            "rr": None,
            "risk_eur": None,
        },
        "notes": [reason],
    }

# =====================================================
# ✅ Strategy setup payload (from DB) — single truth for bot_brain
# =====================================================
def _get_strategy_setup_payload(
    conn,
    *,
    user_id: int,
    strategy_id: int,
    setup_id: Optional[int] = None,
    setup_name: Optional[str] = None,
    symbol: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Builds the 'setup' dict for run_bot_brain() from strategies table.
    Expected by bot_brain/decision_engine:
      - base_amount
      - execution_mode
      - decision_curve (optional)
    """

    if not _table_exists(conn, "strategies"):
        return {
            "id": setup_id,
            "name": setup_name or "Strategy",
            "symbol": (symbol or DEFAULT_SYMBOL).upper(),
            "base_amount": 0.0,
            "execution_mode": "none",
        }

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT base_amount, execution_mode, decision_curve
            FROM strategies
            WHERE id=%s AND user_id=%s
            LIMIT 1
            """,
            (strategy_id, user_id),
        )
        row = cur.fetchone()

    if not row:
        return {
            "id": setup_id,
            "name": setup_name or "Strategy",
            "symbol": (symbol or DEFAULT_SYMBOL).upper(),
            "base_amount": 0.0,
            "execution_mode": "none",
        }

    base_amount, execution_mode, decision_curve = row

    try:
        base_amount = float(base_amount or 0.0)
    except Exception:
        base_amount = 0.0

    execution_mode = (execution_mode or "fixed").lower().strip()
    curve = _safe_json(decision_curve, {}) if decision_curve is not None else {}

    payload = {
        "id": setup_id,
        "name": setup_name or "Strategy",
        "symbol": (symbol or DEFAULT_SYMBOL).upper(),
        "base_amount": base_amount,
        "execution_mode": execution_mode,
    }

    # only include curve when custom
    if execution_mode == "custom":
        payload["decision_curve"] = curve or {}

    return payload


# =====================================================
# 📦 Risk profiles
# =====================================================
def _get_risk_thresholds(risk_profile: str) -> dict:
    """
    Defines decision thresholds per risk profile.
    (UI + legacy behavior: still used for setup_match text)
    """
    profile = (risk_profile or "balanced").lower()

    if profile == "conservative":
        return {"buy": 75, "hold": 55, "min_confidence": "high"}

    if profile == "aggressive":
        return {"buy": 35, "hold": 20, "min_confidence": "medium"}

    return {"buy": 55, "hold": 40, "min_confidence": "medium"}


# =====================================================
# 📦 Build setup match (UI CONTRACT)
# =====================================================
def _build_setup_match(
    *,
    bot: Dict[str, Any],
    scores: Dict[str, float],
    snapshot: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    UI-CONTRACT (KEIHARD):
    - setup_match bestaat ALTIJD
    - score is NOOIT 0
    - status + UI-tekst komen UITSLUITEND uit de backend
    - frontend mag NIETS interpreteren
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
    strategy_confidence = _clamp_score(
        snapshot.get("confidence", 0) if snapshot else 0,
        default=10,
    )

    match_buy = has_snapshot and combined_score >= buy_th and strategy_confidence >= buy_th
    match_hold = has_snapshot and combined_score >= hold_th

    if not has_snapshot:
        status = "no_snapshot"
        summary = "Geen actueel strategy-plan beschikbaar voor vandaag."
        detail = (
            "De marktcondities zijn bekend, maar deze strategie heeft vandaag "
            "onvoldoende context om gecontroleerd uitgevoerd te worden."
        )
        reason = "no_active_strategy_snapshot"

    elif match_buy:
        status = "match_buy"
        summary = "Strategie voldoet aan buy-voorwaarden."
        detail = (
            f"Totale score ({combined_score}) en strategie-confidence "
            f"({strategy_confidence}) liggen boven de buy-drempel ({buy_th})."
        )
        reason = "buy_conditions_met"

    elif match_hold:
        status = "no_match"
        summary = "Strategie actief, maar geen buy-signaal."
        detail = (
            f"Score ({combined_score}) is voldoende om vast te houden "
            f"(≥ {hold_th}), maar nog onder de buy-drempel ({buy_th})."
        )
        reason = "hold_conditions_only"

    else:
        status = "no_match"
        summary = "Strategie onder minimumdrempel."
        detail = (
            f"Score ({combined_score}) ligt onder de hold-drempel ({hold_th}). "
            "De strategie blijft inactief."
        )
        reason = "below_hold_threshold"

    return {
        "name": bot.get("strategy_type") or bot.get("bot_name") or "Strategy",
        "symbol": bot.get("symbol", DEFAULT_SYMBOL),
        "timeframe": bot.get("timeframe") or "—",
        "score": combined_score,
        "confidence": _confidence_from_score(combined_score),
        "components": {"macro": macro, "technical": technical, "market": market, "setup": setup},
        "thresholds": {"buy": buy_th, "hold": hold_th},
        "strategy_confidence": strategy_confidence,
        "has_snapshot": has_snapshot,
        "match_buy": bool(match_buy),
        "match_hold": bool(match_hold),
        "status": status,
        "summary": summary,
        "detail": detail,
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
            risk_profile,
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

    if decision.get("action") != "buy":
        return None

    amount_eur = float(decision.get("amount_eur") or 0)
    if amount_eur <= 0:
        return None

    symbol = decision.get("symbol", DEFAULT_SYMBOL)

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
        logger.warning("⚠️ Geen marktprijs gevonden voor %s", symbol)
        return None

    price_eur = float(row[0])
    estimated_qty = round(amount_eur / price_eur, 8)

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

    return {
        "symbol": symbol,
        "side": "buy",
        "quote_amount_eur": round(amount_eur, 2),
        "estimated_price": round(price_eur, 2),
        "estimated_qty": estimated_qty,
        "status": "ready",
        "budget_after": {"daily_remaining": daily_remaining, "total_remaining": total_remaining},
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

    entry, targets_raw, stop_loss, confidence, reason = row

    parsed_targets = []

    if isinstance(targets_raw, str) and targets_raw.strip():
        try:
            parsed = json.loads(targets_raw)
            if isinstance(parsed, list):
                parsed_targets = [
                    float(t) for t in parsed
                    if t is not None
                ]
        except Exception:
            try:
                parsed_targets = [
                    float(t.strip())
                    for t in targets_raw.split(",")
                    if t and t.strip()
                ]
            except Exception:
                parsed_targets = []

    elif isinstance(targets_raw, list):
        parsed_targets = [
            float(t) for t in targets_raw
            if t is not None
        ]

    return {
        "entry": float(entry) if entry is not None else None,
        "targets": parsed_targets,
        "stop_loss": float(stop_loss) if stop_loss is not None else None,
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
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COALESCE(SUM(ABS(cash_delta_eur)), 0)
            FROM bot_ledger
            WHERE user_id = %s
              AND bot_id = %s
              AND entry_type = 'execute'
              AND cash_delta_eur < 0
              AND DATE(ts) = %s
            """,
            (user_id, bot_id, report_date),
        )
        return float(cur.fetchone()[0] or 0.0)


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
# Persist bot order
# =====================================================
def _persist_bot_order(
    *,
    conn,
    user_id: int,
    bot_id: int,
    decision_id: int,
    order: dict,
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO bot_orders (
                user_id,
                bot_id,
                decision_id,
                symbol,
                side,
                order_type,
                quote_amount_eur,
                estimated_price_eur,
                estimated_qty,
                status,
                created_at,
                updated_at
            )
            VALUES (
                %s,%s,%s,
                %s,%s,
                'market',
                %s,%s,%s,
                'ready',
                NOW(), NOW()
            )
            RETURNING id
            """,
            (
                user_id,
                bot_id,
                decision_id,
                order["symbol"],
                order["side"],
                order["quote_amount_eur"],
                order["estimated_price"],
                order["estimated_qty"],
            ),
        )
        bot_order_id = cur.fetchone()[0]

        cur.execute(
            """
            INSERT INTO bot_executions (
                user_id,
                bot_order_id,
                status,
                created_at,
                updated_at
            )
            VALUES (%s, %s, 'pending', NOW(), NOW())
            """,
            (user_id, bot_order_id),
        )

        return int(bot_order_id)

# =====================================================
# 🧱 Build Trade Plan from snapshot + brain
# =====================================================
def _build_trade_plan(
    *,
    symbol: str,
    action: str,
    snapshot: Optional[Dict[str, Any]],
    brain: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    """
    Creates structured execution plan.

    Used for:
    - TradePlanCard UI
    - paper trading
    - TP hit detection
    - pyramiding engine
    - exchange execution
    """

    if not snapshot:
        return None

    entry = snapshot.get("entry")
    stop = snapshot.get("stop_loss")
    targets = snapshot.get("targets") or []

    if not entry and not targets:
        return None

    rr = None
    if brain:
        rr = brain.get("rr_ratio") or brain.get("rr")

    return {
        "symbol": symbol,
        "side": action,
        "entry_plan": [{"type": "limit", "price": entry}] if entry else [],
        "stop_loss": {"price": stop} if stop else None,
        "targets": [
            {"label": f"TP{i+1}", "price": t}
            for i, t in enumerate(targets)
        ],
        "risk": {
            "rr": rr,
        },
    }


# =====================================================
# Persist trade plan
# =====================================================
def _persist_trade_plan(
    *,
    conn,
    user_id: int,
    bot_id: int,
    decision_id: int,
    symbol: str,
    side: str,
    plan: dict,
):
    """
    Store structured trade plan for execution & UI.

    HARD CONTRACT:
    - trade_plan bestaat altijd
    - ook voor observe/hold
    """

    if not plan:
        return

    symbol = (symbol or DEFAULT_SYMBOL).upper()
    side = (side or "observe").lower()

    entry_plan = plan.get("entry_plan") or []
    stop_loss = plan.get("stop_loss") or {"price": None}
    targets = plan.get("targets") or []
    risk = plan.get("risk") or {}

    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO bot_trade_plans (
                user_id,
                bot_id,
                decision_id,
                symbol,
                side,
                entry_plan,
                stop_loss,
                targets,
                risk_json,
                status,
                created_at,
                updated_at
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,'planned',NOW(),NOW())
            ON CONFLICT (decision_id)
            DO UPDATE SET
                entry_plan = EXCLUDED.entry_plan,
                stop_loss  = EXCLUDED.stop_loss,
                targets    = EXCLUDED.targets,
                risk_json  = EXCLUDED.risk_json,
                status     = 'planned',
                updated_at = NOW()
        """, (
            user_id,
            bot_id,
            decision_id,
            symbol,
            side,
            json.dumps(entry_plan),
            json.dumps(stop_loss),
            json.dumps(targets),
            json.dumps(risk),
        ))


# =====================================================
# Persist decision
# =====================================================
def _persist_decision_and_order(
    *,
    conn,
    user_id: int,
    bot_id: int,
    strategy_id: int,
    setup_id: Optional[int],
    report_date: date,
    decision: Dict[str, Any],
    scores: Dict[str, float],
) -> int:

    logger.info("🧠 Persist decision start")
    logger.info("🧠 Decision RAW: %s", decision)
    logger.info("📊 Scores RAW: %s", scores)

    action = _normalize_action(decision.get("action"))
    confidence = _normalize_confidence(decision.get("confidence") or "low")

    symbol = decision.get("symbol", DEFAULT_SYMBOL)

    requested_amount = float(decision.get("requested_amount_eur") or 0.0)
    amount_eur = float(decision.get("amount_eur") or 0.0)

    logger.info("⚙️ Action normalized: %s", action)
    logger.info("⚙️ Confidence normalized: %s", confidence)
    logger.info("💰 Requested EUR: %s", requested_amount)
    logger.info("💰 Final EUR: %s", amount_eur)

    reasons = decision.get("reasons", [])
    if not isinstance(reasons, list):
        reasons = [str(reasons)]

    setup_match = decision.get("setup_match") or {}

    watch_levels = decision.get("watch_levels")
    monitoring = bool(decision.get("monitoring", False))
    alerts_active = bool(decision.get("alerts_active", False))

    # =====================================================
    # POSITION SIZE
    # =====================================================

    position_size = float(
        decision.get("position_size")
        or decision.get("exposure_multiplier")
        or 1.0
    )

    logger.info("📏 Position size: %s", position_size)

    # =====================================================
    # GUARDRAILS
    # =====================================================

    guardrails_result = decision.get("guardrails_result") or {}

    guardrails_payload = {
        "kill_switch": True,
        "max_trade_risk_eur": decision.get("max_risk_per_trade"),
        "daily_allocation_eur": decision.get("max_daily_allocation"),
        "position_size": position_size,
    }

    logger.info("🛡 Guardrails result: %s", guardrails_result)

    # =====================================================
    # SCORES PAYLOAD
    # =====================================================

    scores_payload = {

        "macro": _clamp_score(scores.get("macro", 10)),
        "technical": _clamp_score(scores.get("technical", 10)),
        "market": _clamp_score(scores.get("market", 10)),
        "setup": _clamp_score(scores.get("setup", 10)),

        "combined": _clamp_score(decision.get("score", 10)),

        "setup_match": setup_match,

        "market_health": float(decision.get("market_health") or 50),
        "market_pressure": float(decision.get("market_pressure") or 50),
        "transition_risk": float(decision.get("transition_risk") or 50),

        "volatility_state": decision.get("volatility_state"),
        "structure_bias": decision.get("structure_bias"),
        "trend_strength": decision.get("trend_strength"),

        "position_size": position_size,
        "exposure_multiplier": position_size,

        "regime": decision.get("regime"),
        "risk_state": decision.get("risk_state"),

        "trade_quality": decision.get("trade_quality"),

        "warnings": decision.get("warnings")
        if isinstance(decision.get("warnings"), list)
        else [],

        # legacy simple guardrail limits
        "guardrails": guardrails_payload,

        # 🔴 CRUCIAAL VOOR FRONTEND
        "requested_amount_eur": requested_amount,
        "amount_eur": amount_eur,
        "guardrails_result": guardrails_result,
        "guardrail_reason": decision.get("guardrail_reason"),

        "watch_levels": watch_levels,
        "monitoring": monitoring,
        "alerts_active": alerts_active,
    }

    logger.info("📦 Scores payload: %s", scores_payload)

    scores_json = json.dumps(scores_payload)
    reasons_json = json.dumps(reasons)

    logger.info("📦 scores_json serialized: %s", scores_json)

    with conn.cursor() as cur:

        cur.execute(
            """
            INSERT INTO bot_decisions (
                user_id,
                bot_id,
                strategy_id,
                setup_id,
                symbol,
                decision_date,
                decision_ts,
                action,
                confidence,
                amount_eur,
                scores_json,
                reason_json,
                status,
                created_at,
                updated_at
            )
            VALUES (
                %s,%s,%s,%s,
                %s,%s,
                NOW(),
                %s,%s,%s,
                %s::jsonb,%s::jsonb,
                'planned',
                NOW(), NOW()
            )
            ON CONFLICT (user_id, bot_id, decision_date)
            DO UPDATE SET
                action        = EXCLUDED.action,
                confidence    = EXCLUDED.confidence,
                amount_eur    = EXCLUDED.amount_eur,
                scores_json   = EXCLUDED.scores_json,
                reason_json   = EXCLUDED.reason_json,
                status        = 'planned',
                executed_by   = NULL,
                executed_at   = NULL,
                updated_at    = NOW()
            RETURNING id
            """,
            (
                user_id,
                bot_id,
                strategy_id,
                setup_id,
                symbol,
                report_date,
                action,
                confidence,
                amount_eur,
                scores_json,
                reasons_json,
            ),
        )

        row = cur.fetchone()

        if not row:
            logger.error("❌ Failed to persist bot decision")
            raise RuntimeError("Failed to persist bot decision")

        decision_id = int(row[0])

        logger.info("✅ Decision persisted | decision_id=%s", decision_id)

    # =====================================================
    # TRADE PLAN
    # =====================================================

    plan = decision.get("trade_plan")

    if not plan:
        logger.info("⚠️ No trade plan provided, using default")
        plan = _default_trade_plan(symbol, action, "fallback_plan")

    logger.info("📊 Trade plan: %s", plan)

    _persist_trade_plan(
        conn=conn,
        user_id=user_id,
        bot_id=bot_id,
        decision_id=decision_id,
        symbol=symbol,
        side=action,
        plan=plan,
    )

    logger.info("✅ Trade plan persisted")

    return decision_id

# =====================================================
# 🚀 Run Trading Bot Agent
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
        bots = _get_active_bots(conn, user_id)

        if bot_id is not None:
            bots = [b for b in bots if b["bot_id"] == bot_id]

        if not bots:
            return {"ok": True, "date": str(report_date), "bots": 0, "decisions": []}

        scores = _get_daily_scores(conn, user_id, report_date)
        results = []

        for bot in bots:

            # =====================================================
            # 📸 SNAPSHOT LOOKUP
            # =====================================================
            snapshot = _get_active_strategy_snapshot(
                conn,
                user_id,
                bot["strategy_id"],
                report_date,
            )

            # =====================================================
            # 🔴 SELF HEALING FALLBACK
            # =====================================================
            if not snapshot:
                logger.warning(
                    "⚠️ No strategy snapshot found | strategy=%s | generating snapshot",
                    bot["strategy_id"],
                )

                try:
                    from backend.celery_task.strategy_task import run_daily_strategy_snapshot

                    run_daily_strategy_snapshot(user_id=user_id)

                    snapshot = _get_active_strategy_snapshot(
                        conn,
                        user_id,
                        bot["strategy_id"],
                        report_date,
                    )

                    if snapshot:
                        logger.info(
                            "✅ Snapshot generated successfully | strategy=%s",
                            bot["strategy_id"],
                        )
                    else:
                        logger.warning(
                            "⚠️ Snapshot still missing after generation | strategy=%s",
                            bot["strategy_id"],
                        )

                except Exception:
                    logger.exception("❌ Failed to generate strategy snapshot")

            # =====================================================
            # SETUP MATCH
            # =====================================================
            setup_match = _build_setup_match(
                bot=bot,
                scores=scores,
                snapshot=snapshot,
            )

            setup_payload = _get_strategy_setup_payload(
                conn,
                user_id=user_id,
                strategy_id=bot["strategy_id"],
                setup_id=bot.get("setup_id"),
                setup_name=bot.get("strategy_type"),
                symbol=bot.get("symbol"),
            )

            if snapshot:
                if snapshot.get("entry") is not None:
                    setup_payload["entry"] = snapshot.get("entry")

                if snapshot.get("stop_loss") is not None:
                    setup_payload["stop_loss"] = snapshot.get("stop_loss")

                if snapshot.get("targets"):
                    setup_payload["targets"] = snapshot.get("targets")

            # =====================================================
            # BOT BRAIN
            # =====================================================
            brain = run_bot_brain(
                user_id=user_id,
                setup=setup_payload,
                scores={
                    "macro_score": scores.get("macro"),
                    "technical_score": scores.get("technical"),
                    "market_score": scores.get("market"),
                    "setup_score": scores.get("setup"),
                },
            )

            engine_action = _normalize_action(brain.get("action"))

            # =====================================================
            # CONFIDENCE
            # =====================================================
            confidence_value = brain.get("confidence")

            if isinstance(confidence_value, (int, float)):
                if confidence_value >= 0.7:
                    confidence_label = "high"
                elif confidence_value >= 0.4:
                    confidence_label = "medium"
                else:
                    confidence_label = "low"
            else:
                confidence_label = "low"

            # =====================================================
            # POSITION SIZE
            # =====================================================
            metrics = brain.get("metrics") or {}

            position_size = float(
                metrics.get("position_size")
                or brain.get("position_size")
                or brain.get("exposure_multiplier")
                or 1.0
            )

            # =====================================================
            # AMOUNT FROM BRAIN
            # =====================================================
            requested_amount = float(
                brain.get("amount_eur")
                or brain.get("final_amount")
                or brain.get("sized_amount")
                or brain.get("base_amount")
                or 0
            )

            logger.info(
                "💰 Requested amount resolved | amount=%s | brain_keys=%s",
                requested_amount,
                list(brain.keys()),
            )

            # =====================================================
            # CURRENT PORTFOLIO STATE
            # =====================================================
            portfolio_value_eur = get_bot_balance(
                conn,
                user_id,
                bot["bot_id"],
            )

            asset_value_eur = get_asset_position_value(
                conn,
                user_id,
                bot["bot_id"],
                bot["symbol"],
            )

            today_spent_eur = get_today_spent_eur(
                conn,
                user_id,
                bot["bot_id"],
                report_date,
            )

            # =====================================================
            # GUARDRAILS
            # =====================================================
            guard = apply_guardrails(
                proposed_amount_eur=requested_amount,
                portfolio_value_eur=portfolio_value_eur,
                current_asset_value_eur=asset_value_eur,
                today_allocated_eur=today_spent_eur,
                kill_switch=True,
                max_trade_risk_eur=float(
                    bot.get("budget", {}).get("max_order_eur") or 0
                ),
                daily_allocation_eur=float(
                    bot.get("budget", {}).get("daily_limit_eur") or 0
                ),
                max_asset_exposure_pct=float(
                    bot.get("budget", {}).get("max_asset_exposure_pct") or 40
                ),
            )

            adjusted_amount = float(guard.get("adjusted_amount_eur") or 0)
            warnings = guard.get("warnings") or []

            if adjusted_amount <= 0:
                logger.info("⚠️ Guardrails reduced allocation to zero → switching to HOLD")
                engine_action = "hold"

                guard = {
                    "blocked": False,
                    "warnings": ["allocation_zero"],
                    "adjusted_amount_eur": 0,
                }

            # =====================================================
            # TRADE PLAN
            # =====================================================
            trade_plan = None

            if snapshot:
                entry = snapshot.get("entry")
                stop = snapshot.get("stop_loss")
                targets = snapshot.get("targets") or []

                if engine_action in ["buy", "sell"] and entry and stop:
                    trade_plan = {
                        "symbol": bot["symbol"],
                        "side": engine_action,
                        "entry_plan": [{"type": "limit", "price": entry}],
                        "stop_loss": {"price": stop},
                        "targets": [
                            {"label": f"TP{i+1}", "price": t}
                            for i, t in enumerate(targets)
                        ],
                        "risk": {"rr": brain.get("rr_ratio")},
                    }

            # 🔴 Ook bij HOLD / OBSERVE altijd levels tonen
            if not trade_plan:
                trade_plan = _default_trade_plan(
                    bot["symbol"],
                    engine_action,
                    "engine_watch_mode",
                    watch_levels=brain.get("watch_levels"),
                    snapshot=snapshot,
                )

            logger.info("📊 Final trade plan built: %s", trade_plan)

            market_health = round(
                float(scores.get("macro", 10)) * 0.4
                + float(scores.get("technical", 10)) * 0.3
                + float(scores.get("market", 10)) * 0.3,
                1,
            )

            decision = {
                "symbol": bot["symbol"],
                "action": engine_action,
                "confidence": confidence_label,
                "score": _clamp_score(scores.get("market", 10)),
                "requested_amount_eur": round(requested_amount, 2),
                "amount_eur": round(adjusted_amount, 2),
                "position_size": position_size,
                "exposure_multiplier": position_size,
                "setup_match": setup_match,
                "reasons": [brain.get("reason") or "engine_decision"],
                "regime": brain.get("regime"),
                "risk_state": brain.get("risk_state"),
                "market_health": market_health,
                "market_pressure": float(brain.get("market_pressure") or 50),
                "transition_risk": float(brain.get("transition_risk") or 50),
                "max_risk_per_trade": float(
                    bot.get("budget", {}).get("max_order_eur") or 0
                ) or None,
                "max_daily_allocation": float(
                    bot.get("budget", {}).get("daily_limit_eur") or 0
                ) or None,
                "guardrails_result": guard,
                "guardrail_reason": (
                    "allocation_zero"
                    if adjusted_amount <= 0
                    else guard.get("blocked_by")
                    or (guard.get("warnings")[0] if guard.get("warnings") else None)
                ),
                "warnings": warnings,
                "trade_plan": trade_plan,
                "watch_levels": brain.get("watch_levels"),
                "monitoring": brain.get("monitoring", False),
                "alerts_active": brain.get("alerts_active", False),
            }

            decision_id = _persist_decision_and_order(
                conn=conn,
                user_id=user_id,
                bot_id=bot["bot_id"],
                strategy_id=bot["strategy_id"],
                setup_id=bot.get("setup_id"),
                report_date=report_date,
                decision=decision,
                scores=scores,
            )

            results.append(
                {
                    "bot_id": bot["bot_id"],
                    "decision_id": decision_id,
                    "action": engine_action,
                }
            )

        conn.commit()

        return {
            "ok": True,
            "date": str(report_date),
            "bots": len(bots),
            "decisions": results,
        }

    except Exception as e:
        logger.exception("❌ trading_bot_agent failed")

        return {
            "ok": False,
            "error": str(e),
        }

    finally:
        conn.close()


# =====================================================
# 📊 Ledger delta calculator
# =====================================================
def _ledger_deltas(side: str, qty: float, price: float):
    """
    Calculates ledger deltas for a trade.

    Returns:
        cash_delta_eur
        qty_delta
        notional_eur
    """

    side = (side or "").lower().strip()

    notional = round(qty * price, 2)

    if side == "buy":
        cash_delta = -notional
        qty_delta = qty

    elif side == "sell":
        cash_delta = notional
        qty_delta = -qty

    else:
        raise ValueError(f"Unsupported side for ledger: {side}")

    return cash_delta, qty_delta, notional


# =====================================================
# 🚀 Bot execute decision functie
# =====================================================
def _auto_execute_decision(
    *,
    conn,
    user_id: int,
    bot_id: int,
    decision_id: int,
    order: dict,
):
    symbol = (order.get("symbol") or DEFAULT_SYMBOL).upper()
    side = (order.get("side") or "buy").lower().strip()

    qty = float(order.get("estimated_qty") or 0.0)
    price = float(order.get("estimated_price") or 0.0)

    if qty <= 0 or price <= 0:
        raise RuntimeError("Invalid execution parameters")

    cash_delta, qty_delta, notional = _ledger_deltas(side, qty, price)

    with conn.cursor() as cur:
        # 1) Decision -> executed
        cur.execute(
            """
            UPDATE bot_decisions
            SET status='executed',
                executed_by='auto',
                executed_at=NOW(),
                updated_at=NOW()
            WHERE id=%s AND user_id=%s AND bot_id=%s
            """,
            (decision_id, user_id, bot_id),
        )

        # 2) Order -> filled (BELANGRIJK: filter ook op user/bot/decision)
        cur.execute(
            """
            UPDATE bot_orders
            SET status='filled',
                executed_price_eur=%s,
                executed_qty=%s,
                quote_amount_eur=COALESCE(quote_amount_eur, %s),
                updated_at=NOW()
            WHERE user_id=%s
              AND bot_id=%s
              AND decision_id=%s
            RETURNING id
            """,
            (price, qty, notional, user_id, bot_id, decision_id),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("No bot_order found to execute")

        bot_order_id = int(row[0])

        # 3) ✅ Ledger entry (NU CORRECT)
        record_bot_ledger_entry(
            conn=conn,
            user_id=user_id,
            bot_id=bot_id,
            entry_type="execute",
            cash_delta_eur=cash_delta,
            qty_delta=qty_delta,
            symbol=symbol,
            decision_id=decision_id,
            order_id=bot_order_id,
            note="Auto execution",
            meta={
                "side": side,
                "price": price,
                "qty": qty,
                "notional_eur": notional,
            },
        )

        # 4) Execution -> filled
        cur.execute(
            """
            UPDATE bot_executions
            SET status='filled',
                filled_qty=%s,
                avg_fill_price=%s,
                updated_at=NOW()
            WHERE user_id=%s
              AND bot_order_id=%s
            """,
            (qty, price, user_id, bot_order_id),
        )

    logger.info(f"⚡ Auto executed | bot={bot_id} | side={side} | qty={qty} | price={price}")


# =====================================================
# 🚀 Manual execute decision functie
# =====================================================
def execute_manual_decision(
    *,
    conn,
    user_id: int,
    bot_id: int,
    decision_id: int,
):
    # 1) Pak executable order (incl side)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT o.id, o.symbol, o.side, o.estimated_qty, o.estimated_price_eur
            FROM bot_orders o
            JOIN bot_decisions d ON d.id = o.decision_id
            WHERE d.id=%s
              AND d.user_id=%s
              AND d.bot_id=%s
              AND d.status='planned'
              AND o.status IN ('ready','pending')
            """,
            (decision_id, user_id, bot_id),
        )
        row = cur.fetchone()

    if not row:
        raise RuntimeError("No executable order")

    bot_order_id, symbol, side, qty, price = row
    symbol = (symbol or DEFAULT_SYMBOL).upper()
    side = (side or "buy").lower().strip()
    qty = float(qty or 0.0)
    price = float(price or 0.0)

    if qty <= 0 or price <= 0:
        raise RuntimeError("Invalid order execution values")

    cash_delta, qty_delta, notional = _ledger_deltas(side, qty, price)

    with conn.cursor() as cur:
        # 2) Decision -> executed
        cur.execute(
            """
            UPDATE bot_decisions
            SET status='executed',
                executed_by='manual',
                executed_at=NOW(),
                updated_at=NOW()
            WHERE id=%s AND user_id=%s AND bot_id=%s
            """,
            (decision_id, user_id, bot_id),
        )

        # 3) Order -> filled
        cur.execute(
            """
            UPDATE bot_orders
            SET status='filled',
                executed_price_eur=%s,
                executed_qty=%s,
                quote_amount_eur=COALESCE(quote_amount_eur, %s),
                updated_at=NOW()
            WHERE id=%s AND user_id=%s AND bot_id=%s
            """,
            (price, qty, notional, bot_order_id, user_id, bot_id),
        )

        # 4) ✅ Ledger entry (NU CORRECT)
        record_bot_ledger_entry(
            conn=conn,
            user_id=user_id,
            bot_id=bot_id,
            entry_type="execute",
            cash_delta_eur=cash_delta,
            qty_delta=qty_delta,
            symbol=symbol,
            decision_id=decision_id,
            order_id=bot_order_id,
            note="Manual execution",
            meta={
                "side": side,
                "price": price,
                "qty": qty,
                "notional_eur": notional,
            },
        )

        # 5) Execution -> filled
        cur.execute(
            """
            UPDATE bot_executions
            SET status='filled',
                filled_qty=%s,
                avg_fill_price=%s,
                updated_at=NOW()
            WHERE user_id=%s
              AND bot_order_id=%s
            """,
            (qty, price, user_id, bot_order_id),
        )
