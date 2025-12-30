# backend/ai_agents/trading_bot_agent.py
import logging
import json
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

from backend.utils.db import get_db_connection

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

DEFAULT_SYMBOL = "BTC"
CONFIDENCE_LEVELS = ("LOW", "MEDIUM", "HIGH")


# =====================================================
# 🔧 DB helpers (kolommen dynamisch → mismatch-proof)
# =====================================================
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


# =====================================================
# 🤖 Active bots ophalen (multi-bot)
# Verwacht: bot_configs met optioneel rules_json/allocation_json/config_json
# We maken dit “kolom-flexibel” zodat je niet vastloopt op mismatch.
# =====================================================
def _get_active_bots(conn, user_id: int) -> List[Dict[str, Any]]:
    if not _table_exists(conn, "bot_configs"):
        return []

    cols = _get_table_columns(conn, "bot_configs")
    if not cols or "id" not in cols or "user_id" not in cols:
        logger.warning("⚠️ bot_configs tabel mist (id/user_id) of bestaat niet.")
        return []

    # Kies alleen kolommen die bestaan (mismatch-proof)
    pick = [
        c
        for c in [
            "id",
            "user_id",
            "name",
            "symbol",
            "active",
            "mode",
            "rules_json",
            "allocation_json",
            "config_json",
            "created_at",
            "updated_at",
            "timestamp",
        ]
        if c in cols
    ]

    q = f"SELECT {', '.join(pick)} FROM bot_configs WHERE user_id=%s"
    if "active" in cols:
        q += " AND active = TRUE"
    q += " ORDER BY id ASC"

    with conn.cursor() as cur:
        cur.execute(q, (user_id,))
        rows = cur.fetchall()

    bots: List[Dict[str, Any]] = []
    for r in rows:
        d = _row_to_dict(pick, r)

        rules = _safe_json(d.get("rules_json"), {})
        allocation = _safe_json(d.get("allocation_json"), {})
        cfg = _safe_json(d.get("config_json"), {})

        # unified config blob voor decision engine
        merged: Dict[str, Any] = {}
        if isinstance(cfg, dict):
            merged.update(cfg)
        if isinstance(rules, dict):
            merged["rules"] = rules
        if isinstance(allocation, dict):
            merged["allocation"] = allocation

        d["_cfg"] = merged
        bots.append(d)

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
            WHERE user_id = %s
              AND report_date = %s
            LIMIT 1
            """,
            (user_id, report_date),
        )
        row = cur.fetchone()

    # Minimum score nooit 0 (zoals jullie voorkeur)
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
# 🧩 Setup matching (optioneel, nooit blokkerend)
# - match op score ranges (macro/technical/market/setup) als kolommen bestaan
# =====================================================
def _find_matching_setup_id(
    conn, user_id: int, symbol: str, scores: Dict[str, float]
) -> Optional[int]:
    if not _table_exists(conn, "setups"):
        return None

    cols = _get_table_columns(conn, "setups")
    if "id" not in cols:
        return None

    conditions = ["user_id = %s"]
    params: List[Any] = [user_id]

    if "symbol" in cols:
        conditions.append("symbol = %s")
        params.append(symbol)

    if "active" in cols:
        conditions.append("active = TRUE")

    def add_range(prefix: str, key: str):
        mn, mx = f"{prefix}_min", f"{prefix}_max"
        if mn in cols:
            conditions.append(f"(%s >= {mn} OR {mn} IS NULL)")
            params.append(scores[key])
        if mx in cols:
            conditions.append(f"(%s <= {mx} OR {mx} IS NULL)")
            params.append(scores[key])

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
        {"min": 75, "max": 100, "action": "HOLD", "amount_eur": 0},
        {"min": 55, "max": 75, "action": "BUY", "amount_eur": 100},
        {"min": 35, "max": 55, "action": "BUY", "amount_eur": 125},
        {"min": 0, "max": 35, "action": "BUY", "amount_eur": 150},
    ]


# =====================================================
# 🧠 Decision engine (deterministisch, uitlegbaar)
# =====================================================
def _decide(
    bot: Dict[str, Any],
    scores: Dict[str, float],
    setup_id: Optional[int],
) -> Dict[str, Any]:
    cfg = bot.get("_cfg") or {}
    rules = cfg.get("rules") if isinstance(cfg.get("rules"), dict) else {}
    allocation = cfg.get("allocation") if isinstance(cfg.get("allocation"), dict) else {}

    symbol = (bot.get("symbol") or cfg.get("symbol") or DEFAULT_SYMBOL).upper()

    ladder = allocation.get("ladder")
    if not isinstance(ladder, list) or not ladder:
        ladder = _default_dca_ladder()

    setup_min = float(rules.get("setup_min", 40))
    macro_min = float(rules.get("macro_min", 25))
    no_buy_above = float(rules.get("no_buy_market", 75))

    market = float(scores.get("market", 10.0))
    macro = float(scores.get("macro", 10.0))
    setup = float(scores.get("setup", 10.0))

    reasons: List[str] = []
    confidence: str = "LOW"

    # --- setup gate
    if setup_id is None:
        return dict(
            symbol=symbol,
            action="OBSERVE",
            amount_eur=0.0,
            confidence="LOW",
            reasons=["Geen actieve setup match (setup_id=None)"],
        )

    if setup < setup_min:
        return dict(
            symbol=symbol,
            action="HOLD",
            amount_eur=0.0,
            confidence="LOW",
            reasons=[f"Setup score {setup} < {setup_min}"],
        )

    # --- no buy zone
    if market >= no_buy_above:
        return dict(
            symbol=symbol,
            action="HOLD",
            amount_eur=0.0,
            confidence="LOW",
            reasons=[f"Market score {market} ≥ {no_buy_above} (no-buy zone)"],
        )

    # --- ladder select
    chosen = None
    for step in ladder:
        try:
            mn = float(step.get("min", 0))
            mx = float(step.get("max", 100))
            if mn <= market < mx or (mx == 100 and mn <= market <= mx):
                chosen = step
                break
        except Exception:
            continue

    if not chosen:
        chosen = {"action": "HOLD", "amount_eur": 0}

    action = (chosen.get("action") or "HOLD").upper()
    amount = float(chosen.get("amount_eur") or 0)

    # --- macro risk-off scaling
    if macro < macro_min and action == "BUY" and amount > 0:
        amount = round(amount * 0.5, 2)
        reasons.append("Macro risk-off → buy bedrag gehalveerd")

    # --- reasons
    reasons.extend(
        [
            f"Market score: {market}",
            f"Macro score: {macro}",
            f"Setup score: {setup}",
        ]
    )
    reasons = reasons[:4]

    # --- confidence
    if action == "BUY" and market < 35 and macro >= macro_min:
        confidence = "HIGH"
    elif action == "BUY":
        confidence = "MEDIUM"

    if confidence not in CONFIDENCE_LEVELS:
        confidence = "LOW"

    return dict(
        symbol=symbol,
        action=action,
        amount_eur=amount,
        confidence=confidence,
        reasons=reasons,
    )


# =====================================================
# 💾 Persist decisions + paper orders
# Status lifecycle:
# - planned   (agent output)
# - executed  (human/exchange)
# - skipped   (manual override)
# =====================================================
def _persist_decision(
    conn,
    user_id: int,
    bot_id: int,
    report_date: date,
    decision: Dict[str, Any],
    scores: Dict[str, float],
):
    # bot_decisions & bot_orders moeten bestaan; zo niet → silently skip
    if not _table_exists(conn, "bot_decisions"):
        logger.warning("⚠️ bot_decisions tabel ontbreekt → geen persist.")
        return

    with conn.cursor() as cur:
        # --- bot_decisions (idempotent per user/bot/date)
        cur.execute(
            """
            INSERT INTO bot_decisions
              (user_id, bot_id, symbol, date, action, amount_eur,
               confidence, reason_json, scores_json, status, created_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,'planned',NOW())
            ON CONFLICT (user_id, bot_id, date)
            DO UPDATE SET
              symbol = EXCLUDED.symbol,
              action = EXCLUDED.action,
              amount_eur = EXCLUDED.amount_eur,
              confidence = EXCLUDED.confidence,
              reason_json = EXCLUDED.reason_json,
              scores_json = EXCLUDED.scores_json,
              status = 'planned',
              updated_at = NOW()
            """,
            (
                user_id,
                bot_id,
                decision.get("symbol"),
                report_date,
                decision.get("action"),
                float(decision.get("amount_eur") or 0),
                decision.get("confidence"),
                json.dumps(decision.get("reasons") or []),
                json.dumps(scores or {}),
            ),
        )

        # --- bot_orders (paper order, klaar voor exchange later)
        if not _table_exists(conn, "bot_orders"):
            return

        action = (decision.get("action") or "").upper()
        amount = float(decision.get("amount_eur") or 0)

        if action in ("BUY", "SELL") and amount > 0:
            # Let op: we maken geen hard ON CONFLICT-key assumptions hier.
            # bot_api is al mismatch-proof; hier houden we het simpel.
            cur.execute(
                """
                INSERT INTO bot_orders
                  (user_id, bot_id, symbol, date, side, amount_eur,
                   status, order_payload_json, created_at)
                VALUES (%s,%s,%s,%s,%s,%s,'planned',%s,NOW())
                ON CONFLICT DO NOTHING
                """,
                (
                    user_id,
                    bot_id,
                    decision.get("symbol"),
                    report_date,
                    action.lower(),
                    amount,
                    json.dumps(
                        {
                            "type": "market",
                            "symbol": decision.get("symbol"),
                            "side": action.lower(),
                            "amount_eur": amount,
                            "created_from": "trading_bot_agent",
                            "exchange": None,  # later koppelen
                        }
                    ),
                ),
            )


# =====================================================
# 🚀 PUBLIC API
# =====================================================
def run_trading_bot_agent(
    user_id: int,
    report_date: Optional[date] = None,
) -> Dict[str, Any]:
    """
    Genereert per actieve bot een decision + paper-order (planned).
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

            symbol = (bot.get("symbol") or bot.get("_cfg", {}).get("symbol") or DEFAULT_SYMBOL).upper()

            setup_id = _find_matching_setup_id(conn, user_id, symbol, scores)
            decision = _decide(bot, scores, setup_id)

            _persist_decision(conn, user_id, bot_id, report_date, decision, scores)

            results.append(
                {
                    "bot_id": bot_id,
                    "bot_name": bot_name,
                    "symbol": decision.get("symbol"),
                    "action": decision.get("action"),
                    "amount_eur": decision.get("amount_eur"),
                    "confidence": decision.get("confidence"),
                    "reasons": decision.get("reasons") or [],
                    "setup_id": setup_id,
                    "scores": scores,
                }
            )

        conn.commit()
        logger.info(f"🤖 Trading bot agent klaar | user={user_id} | bots={len(results)} | date={report_date}")

        return {"ok": True, "date": str(report_date), "bots": len(results), "decisions": results}

    except Exception:
        conn.rollback()
        logger.exception("❌ trading_bot_agent crash")
        return {"ok": False, "error": "crash"}

    finally:
        conn.close()
