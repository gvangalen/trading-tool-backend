# backend/ai_agents/trading_bot_agent.py
import logging
import json
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

from backend.utils.db import get_db_connection

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

DEFAULT_SYMBOL = "BTC"

# DB enums (lowercase!)
ACTIONS = ("buy", "sell", "hold", "observe")
CONFIDENCE_LEVELS = ("low", "medium", "high")


# =====================================================
# 🔧 DB helpers
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


def _get_table_columns(conn, table_name: str) -> List[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = %s
            """,
            (table_name,),
        )
        return [r[0] for r in cur.fetchall()]


def _row_to_dict(cols: List[str], row: Tuple[Any, ...]) -> Dict[str, Any]:
    return {cols[i]: row[i] for i in range(len(cols))}


def _safe_json(v, fallback):
    if v is None:
        return fallback
    if isinstance(v, (dict, list)):
        return v
    try:
        return json.loads(v)
    except Exception:
        return fallback


def _normalize_action(action: str) -> str:
    a = (action or "").strip().lower()
    return a if a in ACTIONS else "hold"


def _normalize_confidence(conf: str) -> str:
    c = (conf or "").strip().lower()
    return c if c in CONFIDENCE_LEVELS else "low"


# =====================================================
# 🤖 Active bots ophalen (bot_configs)
# Jouw schema:
# - is_active (bool)
# - bot_type (dca/swing/trade)
# - cadence (daily/hourly/custom)
# - rules_json, allocation_json, risk_json (jsonb)
# =====================================================
def _get_active_bots(conn, user_id: int) -> List[Dict[str, Any]]:
    if not _table_exists(conn, "bot_configs"):
        return []

    cols = _get_table_columns(conn, "bot_configs")
    required = {"id", "user_id", "name"}
    if not required.issubset(set(cols)):
        logger.warning("⚠️ bot_configs mist vereiste kolommen: %s", required - set(cols))
        return []

    pick = [
        c
        for c in [
            "id",
            "user_id",
            "name",
            "bot_type",
            "symbol",
            "is_active",
            "mode",
            "cadence",
            "rules_json",
            "allocation_json",
            "risk_json",
            "created_at",
            "updated_at",
        ]
        if c in cols
    ]

    q = f"SELECT {', '.join(pick)} FROM bot_configs WHERE user_id=%s"
    if "is_active" in cols:
        q += " AND is_active = TRUE"
    q += " ORDER BY id ASC"

    with conn.cursor() as cur:
        cur.execute(q, (user_id,))
        rows = cur.fetchall()

    bots: List[Dict[str, Any]] = []
    for r in rows:
        d = _row_to_dict(pick, r)

        rules = _safe_json(d.get("rules_json"), {})
        allocation = _safe_json(d.get("allocation_json"), {})
        risk = _safe_json(d.get("risk_json"), {})

        d["_cfg"] = {
            "rules": rules if isinstance(rules, dict) else {},
            "allocation": allocation if isinstance(allocation, dict) else {},
            "risk": risk if isinstance(risk, dict) else {},
        }

        # hard defaults
        d["symbol"] = (d.get("symbol") or DEFAULT_SYMBOL).upper()
        d["bot_type"] = (d.get("bot_type") or "dca").lower()
        d["mode"] = (d.get("mode") or "manual").lower()

        bots.append(d)

    return bots


# =====================================================
# 📊 Daily scores (single source of truth)
# =====================================================
def _get_daily_scores(conn, user_id: int, report_date: date) -> Dict[str, float]:
    # daily_scores schema in jullie tool (zoals eerder): macro/technical/market/setup
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT macro_score, technical_score, market_score, setup_score
            FROM daily_scores
            WHERE user_id = %s
              AND report_date = %s
            LIMIT 1
            """,
            (user_id, report_date),
        )
        row = cur.fetchone()

    # Minimum score nooit 0
    if not row:
        return dict(macro=10.0, technical=10.0, market=10.0, setup=10.0)

    macro, technical, market, setup = row
    return {
        "macro": float(macro) if macro is not None else 10.0,
        "technical": float(technical) if technical is not None else 10.0,
        "market": float(market) if market is not None else 10.0,
        "setup": float(setup) if setup is not None else 10.0,
    }


# =====================================================
# 🧩 Setup matching (optioneel)
# Let op: dit matcht op setups tabel kolommen als ze bestaan.
# (jullie setups kunnen verschillen per projectversie)
# =====================================================
def _find_matching_setup_id(
    conn, user_id: int, symbol: str, scores: Dict[str, float]
) -> Optional[int]:
    if not _table_exists(conn, "setups"):
        return None

    cols = _get_table_columns(conn, "setups")
    if "id" not in cols or "user_id" not in cols:
        return None

    conditions = ["user_id = %s"]
    params: List[Any] = [user_id]

    if "symbol" in cols:
        conditions.append("symbol = %s")
        params.append(symbol)

    # sommige projecten hebben active/is_active
    if "active" in cols:
        conditions.append("active = TRUE")
    if "is_active" in cols:
        conditions.append("is_active = TRUE")

    def add_range(prefix: str, key: str):
        mn, mx = f"{prefix}_min", f"{prefix}_max"
        if mn in cols:
            conditions.append(f"(%s >= {mn} OR {mn} IS NULL)")
            params.append(scores.get(key, 10.0))
        if mx in cols:
            conditions.append(f"(%s <= {mx} OR {mx} IS NULL)")
            params.append(scores.get(key, 10.0))

    add_range("macro", "macro")
    add_range("technical", "technical")
    add_range("market", "market")
    add_range("setup", "setup")

    sql = f"""
        SELECT id
        FROM setups
        WHERE {' AND '.join(conditions)}
        ORDER BY id DESC
        LIMIT 1
    """

    try:
        with conn.cursor() as cur:
            cur.execute(sql, tuple(params))
            row = cur.fetchone()
        return int(row[0]) if row else None
    except Exception:
        logger.warning("⚠️ setup match faalde (niet kritisch)", exc_info=True)
        return None


# =====================================================
# 📐 Default DCA ladder
# =====================================================
def _default_dca_ladder() -> List[Dict[str, Any]]:
    # Ladder op market_score: lager = meer buy
    return [
        {"min": 75, "max": 101, "action": "hold", "amount_eur": 0},
        {"min": 55, "max": 75, "action": "buy", "amount_eur": 100},
        {"min": 35, "max": 55, "action": "buy", "amount_eur": 125},
        {"min": 0, "max": 35, "action": "buy", "amount_eur": 150},
    ]


# =====================================================
# 🧠 Decision engine (deterministisch, uitlegbaar)
# Output bevat amount_eur (voor order), maar decision tabel krijgt alleen actie/context.
# =====================================================
def _decide(
    bot: Dict[str, Any],
    scores: Dict[str, float],
    setup_id: Optional[int],
) -> Dict[str, Any]:
    cfg = bot.get("_cfg") or {}
    rules = cfg.get("rules") if isinstance(cfg.get("rules"), dict) else {}
    allocation = cfg.get("allocation") if isinstance(cfg.get("allocation"), dict) else {}

    symbol = (bot.get("symbol") or DEFAULT_SYMBOL).upper()

    ladder = allocation.get("ladder")
    if not isinstance(ladder, list) or not ladder:
        ladder = _default_dca_ladder()

    # gates
    require_setup = bool(rules.get("require_setup", False))
    setup_min = float(rules.get("setup_min", 40))
    macro_min = float(rules.get("macro_min", 25))
    no_buy_above = float(rules.get("no_buy_market", 75))

    market = float(scores.get("market", 10.0))
    macro = float(scores.get("macro", 10.0))
    setup = float(scores.get("setup", 10.0))

    reasons: List[str] = []

    # --- setup gating (optioneel)
    if require_setup and setup_id is None:
        return dict(
            symbol=symbol,
            action="observe",
            amount_eur=0.0,
            confidence="low",
            reasons=["Geen setup match en require_setup=true"],
        )

    # als setup-score te laag: hold (ook als market low is)
    if setup < setup_min:
        return dict(
            symbol=symbol,
            action="hold",
            amount_eur=0.0,
            confidence="low",
            reasons=[f"Setup score {setup} < {setup_min}"],
        )

    # --- no buy zone
    if market >= no_buy_above:
        return dict(
            symbol=symbol,
            action="hold",
            amount_eur=0.0,
            confidence="low",
            reasons=[f"Market score {market} ≥ {no_buy_above} (no-buy)"],
        )

    # --- ladder select
    chosen = None
    for step in ladder:
        try:
            mn = float(step.get("min", 0))
            mx = float(step.get("max", 100))
            if mn <= market < mx:
                chosen = step
                break
        except Exception:
            continue

    if not chosen:
        chosen = {"action": "hold", "amount_eur": 0}

    action = _normalize_action(chosen.get("action") or "hold")
    amount = float(chosen.get("amount_eur") or 0)

    # --- macro risk-off scaling
    if macro < macro_min and action == "buy" and amount > 0:
        amount = round(amount * 0.5, 2)
        reasons.append("Macro risk-off → buy bedrag gehalveerd")

    # --- reasons (compact & explainable)
    reasons.extend(
        [
            f"Market score: {market}",
            f"Macro score: {macro}",
            f"Setup score: {setup}",
        ]
    )
    reasons = reasons[:4]

    # --- confidence
    confidence = "low"
    if action == "buy" and market < 35 and macro >= macro_min:
        confidence = "high"
    elif action == "buy":
        confidence = "medium"

    return dict(
        symbol=symbol,
        action=action,
        amount_eur=amount,
        confidence=_normalize_confidence(confidence),
        reasons=reasons,
    )


# =====================================================
# 💾 Persist decision + order (paper)
# Schema-correct:
# bot_decisions: geen amount, wel setup_id/strategy_id/status/json
# bot_orders: decision_id verplicht, quote_amount_eur, order_payload/dry_run_payload jsonb
# =====================================================
def _persist_decision_and_order(
    conn,
    user_id: int,
    bot_id: int,
    report_date: date,
    decision: Dict[str, Any],
    scores: Dict[str, float],
    setup_id: Optional[int] = None,
    strategy_id: Optional[int] = None,
) -> Optional[int]:
    if not _table_exists(conn, "bot_decisions"):
        logger.warning("⚠️ bot_decisions ontbreekt → skip persist.")
        return None

    symbol = (decision.get("symbol") or DEFAULT_SYMBOL).upper()
    action = _normalize_action(decision.get("action"))
    confidence = _normalize_confidence(decision.get("confidence"))
    reasons = decision.get("reasons") or []
    if not isinstance(reasons, list):
        reasons = [str(reasons)]

    scores_payload = scores if isinstance(scores, dict) else {}

    # 1) upsert decision (idempotent per user/bot/date)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO bot_decisions
              (user_id, bot_id, symbol, decision_date, decision_ts,
               action, confidence, scores_json, reason_json,
               setup_id, strategy_id, status, created_at, updated_at)
            VALUES
              (%s,%s,%s,%s,NOW(),
               %s,%s,%s::jsonb,%s::jsonb,
               %s,%s,'planned',NOW(),NOW())
            ON CONFLICT (user_id, bot_id, decision_date)
            DO UPDATE SET
              symbol = EXCLUDED.symbol,
              action = EXCLUDED.action,
              confidence = EXCLUDED.confidence,
              scores_json = EXCLUDED.scores_json,
              reason_json = EXCLUDED.reason_json,
              setup_id = EXCLUDED.setup_id,
              strategy_id = EXCLUDED.strategy_id,
              status = 'planned',
              decision_ts = NOW(),
              updated_at = NOW()
            RETURNING id
            """,
            (
                user_id,
                bot_id,
                symbol,
                report_date,
                action,
                confidence,
                json.dumps(scores_payload),
                json.dumps(reasons),
                setup_id,
                strategy_id,
            ),
        )
        decision_id = cur.fetchone()[0]

    # 2) orders (paper)
    if not _table_exists(conn, "bot_orders"):
        return decision_id

    amount = float(decision.get("amount_eur") or 0.0)

    # alleen order als buy/sell + amount > 0
    if action in ("buy", "sell") and amount > 0:
        side = action  # buy/sell
        order_type = "market"

        dry_run_payload = {
            "source": "trading_bot_agent",
            "symbol": symbol,
            "side": side,
            "order_type": order_type,
            "quote_amount_eur": amount,
        }

        # idempotent per decision: vervang bestaande order(s)
        with conn.cursor() as cur:
            cur.execute("DELETE FROM bot_orders WHERE decision_id=%s", (decision_id,))
            cur.execute(
                """
                INSERT INTO bot_orders
                  (user_id, bot_id, decision_id, symbol,
                   side, order_type, quote_amount_eur,
                   dry_run_payload, order_payload,
                   status, created_at, updated_at)
                VALUES
                  (%s,%s,%s,%s,
                   %s,%s,%s,
                   %s::jsonb, %s::jsonb,
                   'ready', NOW(), NOW())
                """,
                (
                    user_id,
                    bot_id,
                    decision_id,
                    symbol,
                    side,
                    order_type,
                    amount,
                    json.dumps(dry_run_payload),
                    json.dumps({}),  # later vullen bij exchange koppeling
                ),
            )

    return decision_id


# =====================================================
# 🚀 PUBLIC API
# =====================================================
def run_trading_bot_agent(
    user_id: int,
    report_date: Optional[date] = None,
) -> Dict[str, Any]:
    """
    Genereert per actieve bot een decision + paper-order (ready).
    Geen exchange calls. 100% deterministic.

    Output:
      { ok, date, bots, decisions[] }
    """
    if not user_id:
        raise ValueError("❌ user_id is verplicht")

    report_date = report_date or date.today()
    conn = get_db_connection()

    if not conn:
        return {"ok": False, "error": "db_unavailable"}

    try:
        bots = _get_active_bots(conn, user_id)
        if not bots:
            return {"ok": True, "date": str(report_date), "bots": 0, "decisions": []}

        scores = _get_daily_scores(conn, user_id, report_date)
        results: List[Dict[str, Any]] = []

        for bot in bots:
            bot_id = int(bot.get("id"))
            bot_name = bot.get("name") or f"Bot {bot_id}"
            symbol = (bot.get("symbol") or DEFAULT_SYMBOL).upper()

            setup_id = _find_matching_setup_id(conn, user_id, symbol, scores)
            decision = _decide(bot, scores, setup_id)

            decision_id = _persist_decision_and_order(
                conn=conn,
                user_id=user_id,
                bot_id=bot_id,
                report_date=report_date,
                decision=decision,
                scores=scores,
                setup_id=setup_id,
                strategy_id=None,  # later: strategy koppelen
            )

            results.append(
                {
                    "bot_id": bot_id,
                    "bot_name": bot_name,
                    "symbol": decision.get("symbol"),
                    "action": _normalize_action(decision.get("action")),
                    "amount_eur": float(decision.get("amount_eur") or 0.0),
                    "confidence": _normalize_confidence(decision.get("confidence")),
                    "reasons": decision.get("reasons") or [],
                    "setup_id": setup_id,
                    "strategy_id": None,
                    "decision_id": decision_id,
                    "scores": scores,
                }
            )

        conn.commit()
        logger.info(
            "🤖 Trading bot agent klaar | user=%s | bots=%s | date=%s",
            user_id,
            len(results),
            report_date,
        )

        return {"ok": True, "date": str(report_date), "bots": len(results), "decisions": results}

    except Exception:
        conn.rollback()
        logger.exception("❌ trading_bot_agent crash")
        return {"ok": False, "error": "crash"}

    finally:
        conn.close()
