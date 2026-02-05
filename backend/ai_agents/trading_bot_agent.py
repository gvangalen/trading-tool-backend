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

def _empty_decision(
    *,
    bot: Dict[str, Any],
    scores: Dict[str, float],
    report_date: date,
    warning: str = "fallback_decision",
) -> Dict[str, Any]:
    """
    Fail-safe decision object.
    -> UI-contract blijft altijd intact
    """

    setup_match = _build_setup_match(bot=bot, scores=scores, snapshot=None)

    return {
        "bot_id": bot["bot_id"],
        "decision_id": None,
        "symbol": bot.get("symbol", DEFAULT_SYMBOL),
        "action": "observe",
        "confidence": "low",
        "amount_eur": 0.0,        # ✅ expliciet
        "reasons": [f"{warning} ({report_date})"],
        "setup_match": setup_match,
        "order": None,
        "status": "planned",
        "executed_by": None,
    }


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
    Single source of truth voor trade sizing.

    Leest UITSLUITEND uit:
      - strategies.trade_amount
      - strategies.trade_amount_unit

    v1:
      - alleen 'eur' toegestaan
      - andere units -> 0 (geen trade)
    """

    if not _table_exists(conn, "strategies"):
        return 0.0

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT trade_amount, trade_amount_unit
            FROM strategies
            WHERE id = %s
              AND user_id = %s
            LIMIT 1
            """,
            (strategy_id, user_id),
        )
        row = cur.fetchone()

    if not row:
        return 0.0

    trade_amount, unit = row

    if trade_amount is None:
        return 0.0

    unit = (unit or "").lower().strip()

    # -------------------------------------------------
    # v1: alleen EUR toegestaan
    # -------------------------------------------------
    if unit != "eur":
        logger.warning(
            "Strategy %s heeft unsupported trade_amount_unit=%s (v1 alleen eur)",
            strategy_id,
            unit,
        )
        return 0.0

    try:
        amount = float(trade_amount)
        return amount if amount > 0 else 0.0
    except Exception:
        return 0.0

ddef _persist_bot_order(
    *,
    conn,
    user_id: int,
    bot_id: int,
    decision_id: int,
    order: dict,
) -> int:
    """
    Persist PLANNED bot order + bot_execution placeholder.
    bot_executions is SINGLE SOURCE OF TRUTH voor UI.
    """

    with conn.cursor() as cur:
        # 1️⃣ bot_orders
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

        # 2️⃣ bot_executions (PENDING = voorstel)
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
    UI-CONTRACT (KEIHARD):
    - setup_match bestaat ALTIJD
    - score is NOOIT 0
    - status + UI-tekst komen UITSLUITEND uit de backend
    - frontend mag NIETS interpreteren
    """

    # -----------------------------
    # Scores (altijd geldig)
    # -----------------------------
    macro = _clamp_score(scores.get("macro", 10), default=10)
    technical = _clamp_score(scores.get("technical", 10), default=10)
    market = _clamp_score(scores.get("market", 10), default=10)
    setup = _clamp_score(scores.get("setup", 10), default=10)

    combined_score = round((macro + technical + market + setup) / 4, 1)
    combined_score = _clamp_score(combined_score, default=10)

    # -----------------------------
    # Risk thresholds
    # -----------------------------
    thresholds = _get_risk_thresholds(bot.get("risk_profile", "balanced"))
    buy_th = float(thresholds["buy"])
    hold_th = float(thresholds["hold"])

    # -----------------------------
    # Snapshot context
    # -----------------------------
    has_snapshot = snapshot is not None
    strategy_confidence = _clamp_score(
        snapshot.get("confidence", 0) if snapshot else 0,
        default=10,
    )

    # -----------------------------
    # Match logic
    # -----------------------------
    match_buy = has_snapshot and combined_score >= buy_th and strategy_confidence >= buy_th
    match_hold = has_snapshot and combined_score >= hold_th

    # -----------------------------
    # STATUS + UI COPY (⭐ ENIGE WAARHEID)
    # -----------------------------
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

    # -----------------------------
    # FINAL UI OBJECT
    # -----------------------------
    return {
        # Identiteit
        "name": bot.get("strategy_type") or bot.get("bot_name") or "Strategy",
        "symbol": bot.get("symbol", DEFAULT_SYMBOL),
        "timeframe": bot.get("timeframe") or "—",

        # Scores
        "score": combined_score,
        "confidence": _confidence_from_score(combined_score),

        "components": {
            "macro": macro,
            "technical": technical,
            "market": market,
            "setup": setup,
        },

        # Thresholds (UI mag deze alleen tonen)
        "thresholds": {
            "buy": buy_th,
            "hold": hold_th,
        },

        # Strategy context
        "strategy_confidence": strategy_confidence,
        "has_snapshot": has_snapshot,

        # Match flags (optioneel voor UI badges)
        "match_buy": bool(match_buy),
        "match_hold": bool(match_hold),

        # ⭐ UI-CONTRACT
        "status": status,      # match_buy | no_match | no_snapshot
        "summary": summary,    # headline (kaarttitel)
        "detail": detail,      # toelichting onder de kaart
        "reason": reason,      # technisch / debug / logging
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
    Total EUR spent TODAY by this bot.
    ❗ Alleen ECHTE uitgevoerde trades (execute).
    Reserve entries tellen NIET mee.
    """
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
# 🧠 Decision and order
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
    """
    Persist bot decision for today.

    BELANGRIJK:
    - Eén decision per bot per dag (UPSERT)
    - 'Nieuwe analyse' reset ALTIJD de decision state
    - status -> planned
    - executed_by / executed_at -> NULL
    """

    action = _normalize_action(decision.get("action"))
    confidence = _normalize_confidence(decision.get("confidence"))
    symbol = decision.get("symbol", DEFAULT_SYMBOL)

    amount_eur = float(decision.get("amount_eur") or 0.0)

    reasons = decision.get("reasons", [])
    if not isinstance(reasons, list):
        reasons = [str(reasons)]

    setup_match = decision.get("setup_match") or {}

    scores_payload = {
        "macro": _clamp_score(scores.get("macro", 10)),
        "technical": _clamp_score(scores.get("technical", 10)),
        "market": _clamp_score(scores.get("market", 10)),
        "setup": _clamp_score(scores.get("setup", 10)),
        "combined": _clamp_score(decision.get("score", 10)),
        "setup_match": setup_match,
    }

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
                %s,%s,
                'planned',
                NOW(), NOW()
            )
            ON CONFLICT (user_id, bot_id, decision_date)
            DO UPDATE SET
                action       = EXCLUDED.action,
                confidence   = EXCLUDED.confidence,
                amount_eur   = EXCLUDED.amount_eur,
                scores_json  = EXCLUDED.scores_json,
                reason_json  = EXCLUDED.reason_json,

                -- 🔑 DE FIX
                status       = 'planned',
                executed_by = NULL,
                executed_at = NULL,

                updated_at   = NOW()
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
                json.dumps(scores_payload),
                json.dumps(reasons),
            ),
        )

        row = cur.fetchone()
        if not row:
            raise RuntimeError("Failed to upsert bot_decisions")

        decision_id = int(row[0])

    return decision_id
    
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

            # 1️⃣ Setup match (altijd)
            setup_match = _build_setup_match(
                bot=bot,
                scores=scores,
                snapshot=snapshot,
            )

            # 2️⃣ Decision (logica)
            decision = _decide(bot, snapshot, scores)

            # 3️⃣ Trade sizing (SINGLE SOURCE OF TRUTH)
            strategy_amount = _get_strategy_trade_amount_eur(
                conn,
                user_id=user_id,
                strategy_id=bot["strategy_id"],
            )

            amount = 0.0
            if decision["action"] == "buy":
                amount = float(strategy_amount or 0.0)

                min_eur = float(bot["budget"].get("min_order_eur") or 0)
                max_eur = float(bot["budget"].get("max_order_eur") or 0)

                if min_eur > 0:
                    amount = max(amount, min_eur)
                if max_eur > 0:
                    amount = min(amount, max_eur)

            decision["amount_eur"] = round(float(amount), 2)
            decision["setup_match"] = setup_match

            # 🔥 HARD RULE 1 — NOOIT BUY MET €0
            if decision["action"] == "buy" and decision["amount_eur"] <= 0:
                decision["action"] = "observe"
                decision["confidence"] = "low"
                decision["amount_eur"] = 0.0
                decision.setdefault("reasons", []).append(
                    "Buy-condities gehaald, maar trade_amount ontbreekt → geen order."
                )

            # 4️⃣ Budget check
            today_spent = get_today_spent_eur(
                conn, user_id, bot["bot_id"], report_date
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
                decision.setdefault("reasons", []).append(
                    f"Budget blokkeert order: {reason}"
                )

            # 5️⃣ Order proposal
            order_proposal = build_order_proposal(
                conn=conn,
                bot=bot,
                decision=decision,
                today_spent_eur=today_spent,
                total_balance_eur=total_balance,
            )

            # 🔥 HARD RULE 2 — BUY ZONDER ORDER BESTAAT NIET
            if decision["action"] == "buy" and not order_proposal:
                decision["action"] = "observe"
                decision["confidence"] = "low"
                decision["amount_eur"] = 0.0
                decision.setdefault("reasons", []).append(
                    "Geen valide order gegenereerd → BUY geannuleerd."
                )

            # 6️⃣ Persist decision (NU PAS!)
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

            # 7️⃣ Persist order + reserve ledger
            order_id = None
            if order_proposal:
                order_id = _persist_bot_order(
                    conn=conn,
                    user_id=user_id,
                    bot_id=bot["bot_id"],
                    decision_id=decision_id,
                    order=order_proposal,
                )

                record_bot_ledger_entry(
                    conn=conn,
                    user_id=user_id,
                    bot_id=bot["bot_id"],
                    entry_type="reserve",
                    cash_delta_eur=-decision["amount_eur"],
                    symbol=decision["symbol"],
                    decision_id=decision_id,
                    order_id=order_id,
                    meta={
                        "estimated_price": order_proposal["estimated_price"],
                        "estimated_qty": order_proposal["estimated_qty"],
                    },
                )

            results.append(
                {
                    "bot_id": bot["bot_id"],
                    "decision_id": decision_id,
                    "action": decision["action"],
                    "confidence": decision["confidence"],
                    "amount_eur": decision["amount_eur"],
                    "order": order_proposal,
                    "status": "planned",
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
        return {"ok": False, "error": "agent_crash"}

    finally:
        conn.close()

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
    """
    Auto execute:
    - reserve is al geboekt
    - execute = qty only
    - bot_executions wordt UPDATE → filled
    """

    symbol = order.get("symbol", DEFAULT_SYMBOL)
    qty = float(order.get("estimated_qty") or 0.0)
    price = float(order.get("estimated_price") or 0.0)

    if qty <= 0 or price <= 0:
        raise RuntimeError("Invalid execution parameters")

    with conn.cursor() as cur:
        # 1️⃣ Decision → executed
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

        # 2️⃣ Order → filled
        cur.execute(
            """
            UPDATE bot_orders
            SET status='filled',
                executed_price_eur=%s,
                executed_qty=%s,
                updated_at=NOW()
            WHERE decision_id=%s
            RETURNING id
            """,
            (price, qty, decision_id),
        )
        bot_order_id = cur.fetchone()[0]

        # 3️⃣ Ledger EXECUTE (qty only)
        record_bot_ledger_entry(
            conn=conn,
            user_id=user_id,
            bot_id=bot_id,
            entry_type="execute",
            cash_delta_eur=0.0,
            qty_delta=qty,
            symbol=symbol,
            decision_id=decision_id,
            order_id=bot_order_id,
            note="Auto execution",
        )

        # 4️⃣ bot_executions → FILLED
        cur.execute(
            """
            UPDATE bot_executions
            SET status='filled',
                filled_qty=%s,
                avg_fill_price=%s,
                updated_at=NOW()
            WHERE bot_order_id=%s
            """,
            (qty, price, bot_order_id),
        )
        
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
    """
    Manual execution.
    UI verwacht bot_executions record.
    """

    with conn.cursor() as cur:
        # 1️⃣ Haal order
        cur.execute(
            """
            SELECT o.id, o.symbol, o.estimated_qty, o.estimated_price_eur
            FROM bot_orders o
            JOIN bot_decisions d ON d.id = o.decision_id
            WHERE d.id=%s AND d.user_id=%s AND d.bot_id=%s AND d.status='planned'
            """,
            (decision_id, user_id, bot_id),
        )
        row = cur.fetchone()

    if not row:
        raise RuntimeError("No executable order")

    bot_order_id, symbol, qty, price = row
    qty = float(qty)
    price = float(price)

    with conn.cursor() as cur:
        # 2️⃣ Decision → executed
        cur.execute(
            """
            UPDATE bot_decisions
            SET status='executed',
                executed_by='manual',
                executed_at=NOW(),
                updated_at=NOW()
            WHERE id=%s
            """,
            (decision_id,),
        )

        # 3️⃣ Order → filled
        cur.execute(
            """
            UPDATE bot_orders
            SET status='filled',
                executed_price_eur=%s,
                executed_qty=%s,
                updated_at=NOW()
            WHERE id=%s
            """,
            (price, qty, bot_order_id),
        )

        # 4️⃣ Ledger EXECUTE
        record_bot_ledger_entry(
            conn=conn,
            user_id=user_id,
            bot_id=bot_id,
            entry_type="execute",
            cash_delta_eur=0.0,
            qty_delta=qty,
            symbol=symbol,
            decision_id=decision_id,
            order_id=bot_order_id,
            note="Manual execution",
        )

        # 5️⃣ bot_executions → FILLED
        cur.execute(
            """
            UPDATE bot_executions
            SET status='filled',
                filled_qty=%s,
                avg_fill_price=%s,
                updated_at=NOW()
            WHERE bot_order_id=%s
            """,
            (qty, price, bot_order_id),
        )
