# backend/ai_agents/trading_bot_agent.py
import logging
import json
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

from backend.utils.db import get_db_connection

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

DEFAULT_SYMBOL = "BTC"


# =====================================================
# ✅ DB helpers (kolommen dynamisch → minder “mismatch” ellende)
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
# ✅ Scores ophalen (single source of truth)
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
        return {"macro": 10.0, "technical": 10.0, "market": 10.0, "setup": 10.0}

    macro, technical, market, setup = row
    return {
        "macro": float(macro) if macro is not None else 10.0,
        "technical": float(technical) if technical is not None else 10.0,
        "market": float(market) if market is not None else 10.0,
        "setup": float(setup) if setup is not None else 10.0,
    }


# =====================================================
# ✅ Setups ophalen (optioneel, maar bot wil “match” kunnen tonen)
# - We nemen “actieve setups” van vandaag op basis van score-ranges als je dat zo opslaat.
# - Als je setup-score systeem elders zit: bot blijft werken (setup_id blijft None).
# =====================================================
def _find_matching_setup_id(conn, user_id: int, symbol: str, scores: Dict[str, float]) -> Optional[int]:
    # Probeert zo veilig mogelijk. Als jouw setups tabel andere velden heeft → geen crash.
    setup_cols = _get_table_columns(conn, "setups")
    if "id" not in setup_cols:
        return None

    # Veelvoorkomende velden in jullie tool:
    # user_id, symbol, active, macro_min/max, technical_min/max, market_min/max, setup_type/strategy_type etc.
    # We checken alleen als kolommen bestaan.
    conditions = ["user_id = %s"]
    params: List[Any] = [user_id]

    if "symbol" in setup_cols:
        conditions.append("symbol = %s")
        params.append(symbol)

    if "active" in setup_cols:
        conditions.append("active = TRUE")

    # Score ranges (optioneel)
    def add_range(prefix: str, score_key: str):
        mn = f"{prefix}_min"
        mx = f"{prefix}_max"
        if mn in setup_cols:
            conditions.append(f"(%s >= {mn} OR {mn} IS NULL)")
            params.append(scores[score_key])
        if mx in setup_cols:
            conditions.append(f"(%s <= {mx} OR {mx} IS NULL)")
            params.append(scores[score_key])

    add_range("macro", "macro")
    add_range("technical", "technical")
    add_range("market", "market")
    add_range("setup", "setup")

    query = f"""
        SELECT id
        FROM setups
        WHERE {' AND '.join(conditions)}
        ORDER BY id DESC
        LIMIT 1
    """

    try:
        with conn.cursor() as cur:
            cur.execute(query, tuple(params))
            row = cur.fetchone()
        return int(row[0]) if row else None
    except Exception:
        logger.warning("⚠️ setup match query faalde (geen probleem, bot kan zonder setup_id).", exc_info=True)
        return None


# =====================================================
# ✅ Strategie ophalen (optioneel, bot kan zonder)
# =====================================================
def _get_latest_strategy_id(conn, user_id: int, setup_id: Optional[int]) -> Optional[int]:
    if setup_id is None:
        return None

    cols = _get_table_columns(conn, "strategies")
    if "id" not in cols or "setup_id" not in cols:
        return None

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id
                FROM strategies
                WHERE user_id = %s
                  AND setup_id = %s
                ORDER BY created_at DESC NULLS LAST, id DESC
                LIMIT 1
                """,
                (user_id, setup_id),
            )
            row = cur.fetchone()
        return int(row[0]) if row else None
    except Exception:
        logger.warning("⚠️ strategy lookup faalde (bot kan zonder).", exc_info=True)
        return None


# =====================================================
# ✅ Bot configs ophalen (multi-bot)
# Verwacht: bot_configs bevat per bot JSON rules.
# Maar we maken dit “kolom-flexibel” zodat je niet vastloopt op mismatch.
# =====================================================
def _get_active_bots(conn, user_id: int) -> List[Dict[str, Any]]:
    cols = _get_table_columns(conn, "bot_configs")
    if not cols:
        return []

    select_cols = []
    for c in ["id", "user_id", "name", "symbol", "active", "mode", "rules_json", "allocation_json", "config_json", "created_at"]:
        if c in cols:
            select_cols.append(c)

    if "id" not in select_cols or "user_id" not in select_cols:
        logger.error("❌ bot_configs mist minimaal id/user_id kolommen.")
        return []

    q = f"SELECT {', '.join(select_cols)} FROM bot_configs WHERE user_id = %s"
    params: List[Any] = [user_id]

    if "active" in cols:
        q += " AND active = TRUE"

    q += " ORDER BY id ASC"

    with conn.cursor() as cur:
        cur.execute(q, tuple(params))
        rows = cur.fetchall()

    bots = []
    for r in rows:
        d = _row_to_dict(select_cols, r)
        # unified config blob
        rules = _safe_json(d.get("rules_json"), None)
        alloc = _safe_json(d.get("allocation_json"), None)
        cfg = _safe_json(d.get("config_json"), None)

        merged = {}
        if isinstance(cfg, dict):
            merged.update(cfg)
        if isinstance(rules, dict):
            merged.update({"rules": rules})
        if isinstance(alloc, dict):
            merged.update({"allocation": alloc})

        d["_cfg"] = merged
        bots.append(d)

    return bots


# =====================================================
# ✅ Decision logic (deterministisch)
# - Gebruikt market_score + setup_score + macro_score als filters
# - DCA ladder regels uit bot config (of default)
# =====================================================
def _default_dca_ladder() -> List[Dict[str, Any]]:
    # Lager = meer buy (zoals je eerder aangaf)
    return [
        {"min": 75, "max": 100, "action": "HOLD", "amount_eur": 0},
        {"min": 55, "max": 75, "action": "BUY", "amount_eur": 100},
        {"min": 35, "max": 55, "action": "BUY", "amount_eur": 125},
        {"min": 0, "max": 35, "action": "BUY", "amount_eur": 150},
    ]


def _decide(bot: Dict[str, Any], scores: Dict[str, float], setup_id: Optional[int]) -> Dict[str, Any]:
    cfg = bot.get("_cfg") or {}
    symbol = (bot.get("symbol") or cfg.get("symbol") or DEFAULT_SYMBOL).upper()

    # --- ladder
    ladder = cfg.get("allocation", {}).get("ladder") if isinstance(cfg.get("allocation"), dict) else None
    if not isinstance(ladder, list) or not ladder:
        ladder = _default_dca_ladder()

    # --- filters
    rules = cfg.get("rules") if isinstance(cfg.get("rules"), dict) else {}
    setup_min = float(rules.get("setup_min", 40))      # als setup_score < 40 → HOLD
    macro_min = float(rules.get("macro_min", 25))      # risk-off onder 25
    no_buy_above = float(rules.get("no_buy_market", 75))  # boven 75 geen buy

    market = float(scores["market"])
    macro = float(scores["macro"])
    setup = float(scores["setup"])

    reasons: List[str] = []
    confidence = "MIDDEL"

    # 1) Setup gate
    if setup_id is None:
        # geen setup match → observeren/hold
        reasons.append("Geen setup match gevonden (setup_id=None).")
        return {
            "symbol": symbol,
            "action": "OBSERVEREN",
            "amount_eur": 0,
            "confidence": "LAAG",
            "reasons": reasons,
        }

    if setup < setup_min:
        reasons.append(f"Setup-score te laag ({setup} < {setup_min}) → HOLD.")
        return {
            "symbol": symbol,
            "action": "HOLD",
            "amount_eur": 0,
            "confidence": "LAAG",
            "reasons": reasons,
        }

    # 2) Macro risk-off
    if macro < macro_min:
        reasons.append(f"Macro risk-off ({macro} < {macro_min}) → BUY verlaagd / soms HOLD.")
        confidence = "LAAG"

    # 3) No-buy zone
    if market >= no_buy_above:
        reasons.append(f"Market score hoog ({market} ≥ {no_buy_above}) → NO BUY zone.")
        return {
            "symbol": symbol,
            "action": "HOLD",
            "amount_eur": 0,
            "confidence": confidence,
            "reasons": reasons,
        }

    # 4) Ladder beslissing op market_score
    chosen = None
    for step in ladder:
        try:
            mn = float(step.get("min", 0))
            mx = float(step.get("max", 100))
            if mn <= market < mx or (mn <= market <= mx and mx == 100):
                chosen = step
                break
        except Exception:
            continue

    if not chosen:
        chosen = {"action": "HOLD", "amount_eur": 0}

    action = (chosen.get("action") or "HOLD").upper()
    amount = float(chosen.get("amount_eur") or 0)

    # Macro risk-off reduce
    if macro < macro_min and action == "BUY" and amount > 0:
        amount = round(amount * 0.5, 2)
        reasons.append("Macro risk-off → buy bedrag gehalveerd.")

    # Reason bullets (max 3-4)
    reasons.extend(
        [
            f"Market score: {market}",
            f"Macro score: {macro}",
            f"Setup score: {setup}",
        ]
    )
    reasons = reasons[:4]

    # Confidence simple
    if action == "BUY" and market < 35 and macro >= macro_min:
        confidence = "HOOG"
    elif action == "BUY":
        confidence = "MIDDEL"
    else:
        confidence = "LAAG" if confidence != "HOOG" else confidence

    return {
        "symbol": symbol,
        "action": action,
        "amount_eur": amount,
        "confidence": confidence,
        "reasons": reasons,
    }


# =====================================================
# ✅ Persist: bot_decisions + bot_orders (paper-ready)
# =====================================================
def _upsert_bot_decision(
    conn,
    user_id: int,
    bot_id: int,
    report_date: date,
    decision: Dict[str, Any],
    scores: Dict[str, float],
    setup_id: Optional[int],
    strategy_id: Optional[int],
):
    cols = _get_table_columns(conn, "bot_decisions")

    # We bouwen payload dynamisch.
    payload: Dict[str, Any] = {}

    # required-ish
    if "user_id" in cols:
        payload["user_id"] = user_id
    if "bot_id" in cols:
        payload["bot_id"] = bot_id
    if "symbol" in cols:
        payload["symbol"] = decision.get("symbol")
    if "date" in cols:
        payload["date"] = report_date

    # decision fields
    if "action" in cols:
        payload["action"] = decision.get("action")
    if "amount_eur" in cols:
        payload["amount_eur"] = decision.get("amount_eur")
    if "confidence" in cols:
        payload["confidence"] = decision.get("confidence")

    if "reason_json" in cols:
        payload["reason_json"] = json.dumps(decision.get("reasons", []))
    if "reasons_json" in cols and "reason_json" not in cols:
        payload["reasons_json"] = json.dumps(decision.get("reasons", []))

    if "scores_json" in cols:
        payload["scores_json"] = json.dumps(scores)
    if "setup_id" in cols:
        payload["setup_id"] = setup_id
    if "strategy_id" in cols:
        payload["strategy_id"] = strategy_id

    # status
    if "status" in cols:
        payload["status"] = "planned"

    # timestamps
    if "created_at" in cols:
        payload["created_at"] = datetime.utcnow()
    if "updated_at" in cols:
        payload["updated_at"] = datetime.utcnow()
    if "timestamp" in cols and "created_at" not in cols:
        payload["timestamp"] = datetime.utcnow()

    # conflict key: (user_id, bot_id, date) of (user_id, date) etc.
    conflict = None
    if {"user_id", "bot_id", "date"}.issubset(set(cols)):
        conflict = "(user_id, bot_id, date)"
    elif {"user_id", "date"}.issubset(set(cols)):
        conflict = "(user_id, date)"

    keys = list(payload.keys())
    vals = [payload[k] for k in keys]
    placeholders = ", ".join(["%s"] * len(keys))

    if conflict:
        updates = ", ".join([f"{k}=EXCLUDED.{k}" for k in keys if k not in ("user_id", "bot_id", "date")])
        sql = f"""
            INSERT INTO bot_decisions ({", ".join(keys)})
            VALUES ({placeholders})
            ON CONFLICT {conflict}
            DO UPDATE SET {updates}
        """
    else:
        # geen unique constraint bekend → gewoon insert
        sql = f"""
            INSERT INTO bot_decisions ({", ".join(keys)})
            VALUES ({placeholders})
        """

    with conn.cursor() as cur:
        cur.execute(sql, tuple(vals))


def _create_bot_order(
    conn,
    user_id: int,
    bot_id: int,
    report_date: date,
    decision: Dict[str, Any],
):
    # Alleen als BUY/SELL en amount > 0
    action = (decision.get("action") or "").upper()
    amount = float(decision.get("amount_eur") or 0)
    if action not in ("BUY", "SELL") or amount <= 0:
        return

    cols = _get_table_columns(conn, "bot_orders")
    if not cols:
        return

    payload: Dict[str, Any] = {}
    if "user_id" in cols:
        payload["user_id"] = user_id
    if "bot_id" in cols:
        payload["bot_id"] = bot_id
    if "symbol" in cols:
        payload["symbol"] = decision.get("symbol")
    if "date" in cols:
        payload["date"] = report_date
    if "side" in cols:
        payload["side"] = action.lower()
    if "amount_eur" in cols:
        payload["amount_eur"] = amount
    if "status" in cols:
        payload["status"] = "planned"

    # order_payload_json (klaar voor exchange later)
    order_payload = {
        "symbol": decision.get("symbol"),
        "side": action.lower(),
        "type": "market",
        "amount_eur": amount,
        "created_from": "bot_decision",
        "exchange": None,  # later invullen
    }

    if "order_json" in cols:
        payload["order_json"] = json.dumps(order_payload)
    elif "order_payload" in cols:
        payload["order_payload"] = json.dumps(order_payload)
    elif "order_payload_json" in cols:
        payload["order_payload_json"] = json.dumps(order_payload)

    if "created_at" in cols:
        payload["created_at"] = datetime.utcnow()
    if "timestamp" in cols and "created_at" not in cols:
        payload["timestamp"] = datetime.utcnow()

    keys = list(payload.keys())
    vals = [payload[k] for k in keys]
    placeholders = ", ".join(["%s"] * len(keys))

    # conflict key: (user_id, bot_id, date) als dat bestaat
    conflict = None
    if {"user_id", "bot_id", "date", "side"}.issubset(set(cols)):
        conflict = "(user_id, bot_id, date, side)"
    elif {"user_id", "bot_id", "date"}.issubset(set(cols)):
        conflict = "(user_id, bot_id, date)"

    if conflict:
        updates = ", ".join([f"{k}=EXCLUDED.{k}" for k in keys if k not in ("user_id", "bot_id", "date", "side")])
        sql = f"""
            INSERT INTO bot_orders ({", ".join(keys)})
            VALUES ({placeholders})
            ON CONFLICT {conflict}
            DO UPDATE SET {updates}
        """
    else:
        sql = f"""
            INSERT INTO bot_orders ({", ".join(keys)})
            VALUES ({placeholders})
        """

    with conn.cursor() as cur:
        cur.execute(sql, tuple(vals))


# =====================================================
# ✅ PUBLIC API: run_trading_bot_agent(user_id, date)
# =====================================================
def run_trading_bot_agent(user_id: int, report_date: Optional[date] = None) -> Dict[str, Any]:
    """
    Genereert bot decisions + order payloads (paper-ready) voor alle actieve bots van een user.

    Schrijft:
    - bot_decisions (planned)
    - bot_orders (planned)  -> klaar om later aan exchange API te voeren

    Output:
    - dict met summary + decisions per bot
    """
    if user_id is None:
        raise ValueError("❌ trading_bot_agent vereist user_id")

    report_date = report_date or date.today()

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB connectie (trading_bot_agent)")
        return {"ok": False, "error": "no_db"}

    try:
        bots = _get_active_bots(conn, user_id=user_id)
        if not bots:
            logger.info(f"ℹ️ Geen actieve bots voor user_id={user_id}")
            return {"ok": True, "user_id": user_id, "date": str(report_date), "bots": 0, "decisions": []}

        scores = _get_daily_scores(conn, user_id=user_id, report_date=report_date)

        results = []
        for bot in bots:
            bot_id = int(bot.get("id"))
            symbol = (bot.get("symbol") or bot.get("_cfg", {}).get("symbol") or DEFAULT_SYMBOL).upper()

            setup_id = _find_matching_setup_id(conn, user_id=user_id, symbol=symbol, scores=scores)
            strategy_id = _get_latest_strategy_id(conn, user_id=user_id, setup_id=setup_id)

            decision = _decide(bot=bot, scores=scores, setup_id=setup_id)

            _upsert_bot_decision(
                conn=conn,
                user_id=user_id,
                bot_id=bot_id,
                report_date=report_date,
                decision=decision,
                scores=scores,
                setup_id=setup_id,
                strategy_id=strategy_id,
            )

            _create_bot_order(
                conn=conn,
                user_id=user_id,
                bot_id=bot_id,
                report_date=report_date,
                decision=decision,
            )

            results.append(
                {
                    "bot_id": bot_id,
                    "bot_name": bot.get("name") or f"Bot {bot_id}",
                    "symbol": decision.get("symbol"),
                    "action": decision.get("action"),
                    "amount_eur": decision.get("amount_eur"),
                    "confidence": decision.get("confidence"),
                    "reasons": decision.get("reasons", []),
                    "setup_id": setup_id,
                    "strategy_id": strategy_id,
                    "scores": scores,
                }
            )

        conn.commit()
        logger.info(f"✅ Trading Bot Agent klaar | user_id={user_id} | bots={len(results)} | date={report_date}")

        return {
            "ok": True,
            "user_id": user_id,
            "date": str(report_date),
            "bots": len(results),
            "decisions": results,
        }

    except Exception:
        conn.rollback()
        logger.exception("❌ Trading Bot Agent crash")
        return {"ok": False, "error": "crash"}
    finally:
        conn.close()
