import logging
import json
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

from backend.utils.db import get_db_connection

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

DEFAULT_SYMBOL = "BTC"


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

    if not row:
        return dict(macro=10.0, technical=10.0, market=10.0, setup=10.0)

    macro, technical, market, setup = row
    return {
        "macro": float(macro or 10),
        "technical": float(technical or 10),
        "market": float(market or 10),
        "setup": float(setup or 10),
    }


# =====================================================
# 🧩 Setup matching (optioneel, nooit blokkerend)
# =====================================================
def _find_matching_setup_id(
    conn, user_id: int, symbol: str, scores: Dict[str, float]
) -> Optional[int]:
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
    symbol = (bot.get("symbol") or cfg.get("symbol") or DEFAULT_SYMBOL).upper()

    allocation = cfg.get("allocation") or {}
    ladder = allocation.get("ladder") or _default_dca_ladder()

    rules = cfg.get("rules") or {}
    setup_min = float(rules.get("setup_min", 40))
    macro_min = float(rules.get("macro_min", 25))
    no_buy_above = float(rules.get("no_buy_market", 75))

    market = scores["market"]
    macro = scores["macro"]
    setup = scores["setup"]

    reasons: List[str] = []
    confidence = "LOW"

    # --- setup gate
    if setup_id is None:
        return dict(
            symbol=symbol,
            action="OBSERVE",
            amount_eur=0,
            confidence="LOW",
            reasons=["Geen actieve setup match"],
        )

    if setup < setup_min:
        return dict(
            symbol=symbol,
            action="HOLD",
            amount_eur=0,
            confidence="LOW",
            reasons=[f"Setup score {setup} < {setup_min}"],
        )

    # --- no buy zone
    if market >= no_buy_above:
        return dict(
            symbol=symbol,
            action="HOLD",
            amount_eur=0,
            confidence="LOW",
            reasons=[f"Market score {market} ≥ {no_buy_above}"],
        )

    # --- ladder
    chosen = next(
        (
            s
            for s in ladder
            if float(s.get("min", 0)) <= market < float(s.get("max", 100))
        ),
        {"action": "HOLD", "amount_eur": 0},
    )

    action = chosen["action"]
    amount = float(chosen.get("amount_eur", 0))

    # --- macro risk-off
    if macro < macro_min and action == "BUY":
        amount = round(amount * 0.5, 2)
        reasons.append("Macro risk-off → buy verlaagd")

    reasons.extend(
        [
            f"Market score: {market}",
            f"Macro score: {macro}",
            f"Setup score: {setup}",
        ]
    )

    if action == "BUY" and market < 35 and macro >= macro_min:
        confidence = "HIGH"
    elif action == "BUY":
        confidence = "MEDIUM"

    return dict(
        symbol=symbol,
        action=action,
        amount_eur=amount,
        confidence=confidence,
        reasons=reasons[:4],
    )


# =====================================================
# 💾 Persist decisions + paper orders
# =====================================================
def _persist_decision(
    conn,
    user_id: int,
    bot_id: int,
    report_date: date,
    decision: Dict[str, Any],
    scores: Dict[str, float],
):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO bot_decisions
              (user_id, bot_id, symbol, date, action, amount_eur,
               confidence, reason_json, scores_json, status, created_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,'planned',NOW())
            ON CONFLICT (user_id, bot_id, date)
            DO UPDATE SET
              action = EXCLUDED.action,
              amount_eur = EXCLUDED.amount_eur,
              confidence = EXCLUDED.confidence,
              reason_json = EXCLUDED.reason_json,
              scores_json = EXCLUDED.scores_json,
              updated_at = NOW()
            """,
            (
                user_id,
                bot_id,
                decision["symbol"],
                report_date,
                decision["action"],
                decision["amount_eur"],
                decision["confidence"],
                json.dumps(decision["reasons"]),
                json.dumps(scores),
            ),
        )

        if decision["action"] in ("BUY", "SELL") and decision["amount_eur"] > 0:
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
                    decision["symbol"],
                    report_date,
                    decision["action"].lower(),
                    decision["amount_eur"],
                    json.dumps(
                        {
                            "type": "market",
                            "symbol": decision["symbol"],
                            "side": decision["action"].lower(),
                            "amount_eur": decision["amount_eur"],
                            "source": "trading_bot_agent",
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
    if not user_id:
        raise ValueError("user_id is verplicht")

    report_date = report_date or date.today()
    conn = get_db_connection()

    if not conn:
        return {"ok": False, "error": "db_unavailable"}

    try:
        bots = _get_active_bots(conn, user_id)
        if not bots:
            return {"ok": True, "bots": 0, "decisions": []}

        scores = _get_daily_scores(conn, user_id, report_date)
        results = []

        for bot in bots:
            bot_id = int(bot["id"])
            setup_id = _find_matching_setup_id(conn, user_id, DEFAULT_SYMBOL, scores)
            decision = _decide(bot, scores, setup_id)

            _persist_decision(
                conn, user_id, bot_id, report_date, decision, scores
            )

            results.append(
                dict(
                    bot_id=bot_id,
                    bot_name=bot.get("name"),
                    **decision,
                )
            )

        conn.commit()
        logger.info(
            f"🤖 Trading bot agent klaar | user={user_id} | bots={len(results)}"
        )

        return dict(ok=True, date=str(report_date), decisions=results)

    except Exception:
        conn.rollback()
        logger.exception("❌ trading_bot_agent crash")
        return {"ok": False, "error": "crash"}

    finally:
        conn.close()
