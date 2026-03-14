# backend/api/bot_api.py
import os
import json
import logging
from datetime import datetime, date, timedelta

from dotenv import load_dotenv
from fastapi import APIRouter, HTTPException, Request, Depends

from backend.utils.db import get_db_connection
from backend.utils.auth_utils import get_current_user
from backend.ai_agents.trading_bot_agent import execute_manual_decision
from backend.services.portfolio_snapshot_service import snapshot_all_for_user

# (optioneel) onboarding helper — alleen gebruiken als jij dat wil
# from backend.api.onboarding_api import mark_step_completed


logger = logging.getLogger(__name__)
router = APIRouter()

dotenv_path = os.path.join(os.path.dirname(__file__), "..", ".env")
load_dotenv(dotenv_path=dotenv_path)

logger.info("🤖 bot_api.py geladen – Trading Bot endpoints (user_id-systeem).")


# =====================================
# 🔧 Helperfunctie (zelfde stijl als jouw APIs)
# =====================================
def get_db_cursor():
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="❌ [DB01] Geen databaseverbinding.")
    return conn, conn.cursor()


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


def _get_table_columns(conn, table_name: str):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema='public'
              AND table_name=%s
            """,
            (table_name,),
        )
        return [r[0] for r in cur.fetchall()]

def _get_daily_scores_row(conn, user_id: int, report_date: date):
    """
    Haalt de gecombineerde scores op uit daily_scores.
    Return:
      dict {macro, technical, market, setup}  of None
    """
    if not _table_exists(conn, "daily_scores"):
        return None

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
        return None

    macro, technical, market, setup = row
    return {
        "macro": float(macro or 10),
        "technical": float(technical or 10),
        "market": float(market or 10),
        "setup": float(setup or 10),
    }


# =====================================
# 📦 BOT CONFIGS (actieve bots)
# =====================================
@router.get("/bot/configs")
async def get_bot_configs(current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]

    conn, cur = get_db_cursor()
    try:
        cur.execute(
            """
            SELECT
              b.id,
              b.name,
              b.is_active,
              b.mode,
              b.cadence,
              b.risk_profile,

              b.budget_total_eur,
              b.budget_daily_limit_eur,
              b.budget_min_order_eur,
              b.budget_max_order_eur,
              b.max_asset_exposure_pct,

              b.created_at,
              b.updated_at,

              s.id            AS strategy_id,
              s.name          AS strategy_name,
              s.strategy_type AS strategy_type,

              st.id           AS setup_id,
              st.name         AS setup_name,
              st.symbol       AS symbol,
              st.timeframe    AS timeframe

            FROM bot_configs b
            LEFT JOIN strategies s ON s.id = b.strategy_id
            LEFT JOIN setups st    ON st.id = s.setup_id
            WHERE b.user_id = %s
            ORDER BY b.id ASC
            """,
            (user_id,),
        )

        rows = cur.fetchall()
        out = []

        for r in rows:
            (
                bot_id,
                name,
                is_active,
                mode,
                cadence,
                risk_profile,

                budget_total,
                budget_daily,
                budget_min,
                budget_max,
                max_asset_exposure_pct,

                created_at,
                updated_at,

                strategy_id,
                strategy_name,
                strategy_type,

                setup_id,
                setup_name,
                symbol,
                timeframe,
            ) = r

            strategy = None
            if strategy_id:
                strategy = {
                    "id": strategy_id,
                    "type": strategy_type,

                    # 🔹 STRATEGY INFO
                    "name": strategy_name,

                    # 🔹 SETUP INFO (nieuw)
                    "setup": {
                        "id": setup_id,
                        "name": setup_name,
                        "symbol": symbol,
                        "timeframe": timeframe,
                    },
                }

            out.append(
                {
                    "id": bot_id,
                    "name": name,
                    "is_active": bool(is_active),
                    "mode": mode,
                    "cadence": cadence,
                    "risk_profile": risk_profile or "balanced",

                    "budget": {
                        "total_eur": float(budget_total or 0),
                        "daily_limit_eur": float(budget_daily or 0),
                        "min_order_eur": float(budget_min or 0),
                        "max_order_eur": float(budget_max or 0),
                        "max_asset_exposure_pct": float(max_asset_exposure_pct or 100),
                    },

                    "strategy": strategy,
                    "created_at": created_at,
                    "updated_at": updated_at,
                }
            )

        return out

    except Exception:
        logger.error("❌ get_bot_configs error", exc_info=True)
        raise HTTPException(status_code=500, detail="Bot configs ophalen mislukt")
    finally:
        conn.close()
        
# =====================================
# 📄 BOT TODAY (decisions + orders + proposal)
# =====================================
@router.get("/bot/today")
async def get_bot_today(current_user: dict = Depends(get_current_user)):

    from backend.ai_agents.trading_bot_agent import run_trading_bot_agent
    import time

    user_id = current_user["id"]
    today = date.today()

    conn, cur = get_db_cursor()

    try:

        # =====================================================
        # SCORES
        # =====================================================
        daily_scores = _get_daily_scores_row(conn, user_id, today) or {
            "macro": 10,
            "technical": 10,
            "market": 10,
            "setup": 10,
        }

        # =====================================================
        # ACTIEVE BOTS
        # =====================================================
        cur.execute(
            """
            SELECT
              b.id,
              b.name,
              COALESCE(st.symbol,'BTC')  AS symbol,
              COALESCE(st.timeframe,'—') AS timeframe,
              s.strategy_type
            FROM bot_configs b
            LEFT JOIN strategies s ON s.id = b.strategy_id
            LEFT JOIN setups st    ON st.id = s.setup_id
            WHERE b.user_id=%s
              AND b.is_active=TRUE
            ORDER BY b.id ASC
            """,
            (user_id,),
        )

        bot_rows = cur.fetchall()

        if not bot_rows:
            return {
                "date": str(today),
                "scores": daily_scores,
                "decisions": [],
                "orders": [],
                "executions": [],
            }

        bots_by_id = {
            int(r[0]): {
                "bot_id": int(r[0]),
                "bot_name": r[1],
                "symbol": r[2],
                "timeframe": r[3],
                "strategy_type": r[4],
            }
            for r in bot_rows
        }

        # =====================================================
        # DECISIONS VAN VANDAAG
        # =====================================================
        cur.execute(
            """
            SELECT
              id,
              bot_id,
              symbol,
              decision_ts,
              action,
              confidence,
              scores_json,
              reason_json,
              setup_id,
              strategy_id,
              status,
              created_at,
              updated_at
            FROM bot_decisions
            WHERE user_id=%s
              AND decision_date=%s
            ORDER BY bot_id ASC, id DESC
            """,
            (user_id, today),
        )

        rows = cur.fetchall()

        decisions_by_bot = {}
        decision_ids = []

        for r in rows:

            (
                decision_id,
                bot_id,
                symbol,
                decision_ts,
                action,
                confidence,
                scores_json,
                reason_json,
                setup_id,
                strategy_id,
                status,
                created_at,
                updated_at,
            ) = r

            bot_id = int(bot_id)

            if bot_id in decisions_by_bot:
                continue

            bot = bots_by_id.get(bot_id)
            if not bot:
                continue

            scores_payload = _safe_json(scores_json, {})
            reasons_payload = _safe_json(reason_json, [])

            setup_match = scores_payload.get("setup_match") or {
                "status": "no_snapshot",
                "summary": "Geen strategie context",
                "detail": "Er is vandaag geen actief strategy snapshot beschikbaar.",
                "score": 10,
                "confidence": "low",
            }

            guard = scores_payload.get("guardrails_result", {})
            guard_limits = guard.get("guardrails", {})

            decisions_by_bot[bot_id] = {
                "id": decision_id,
                "bot_id": bot_id,
                "bot_name": bot["bot_name"],
                "symbol": symbol,

                "action": action,
                "confidence": confidence,

                # complete payload voor UI
                "scores_json": scores_payload or daily_scores,

                # 🔥 nieuwe velden voor Guardrails UI
                "requested_amount_eur": scores_payload.get("requested_amount_eur"),
                "amount_eur": scores_payload.get("amount_eur"),
                "guardrails_result": guard,
                "guardrail_reason": scores_payload.get("guardrail_reason"),

                "metrics": {
                    "position_size": scores_payload.get("position_size"),
                    "exposure_multiplier": scores_payload.get("exposure_multiplier"),
                    "max_trade_risk_eur": guard_limits.get("max_trade_risk_eur"),
                    "daily_allocation_eur": guard_limits.get("daily_allocation_eur"),
                },

                "reasons": reasons_payload,
                "setup_id": setup_id,
                "strategy_id": strategy_id,
                "status": status,
                "created_at": created_at,
                "updated_at": updated_at,
                "setup_match": setup_match,
                "trade_plan": None,
            }

            decision_ids.append(int(decision_id))

        # =====================================================
        # SAFETY NET – RUN BOT AGENT
        # =====================================================
        missing = [bid for bid in bots_by_id if bid not in decisions_by_bot]

        if missing:

            run_trading_bot_agent(
                user_id=user_id,
                report_date=today,
            )

            conn.commit()
            time.sleep(0.3)

            cur.execute(
                """
                SELECT
                  id,
                  bot_id,
                  symbol,
                  decision_ts,
                  action,
                  confidence,
                  scores_json,
                  reason_json,
                  setup_id,
                  strategy_id,
                  status,
                  created_at,
                  updated_at
                FROM bot_decisions
                WHERE user_id=%s
                  AND decision_date=%s
                ORDER BY bot_id ASC, id DESC
                """,
                (user_id, today),
            )

            rows = cur.fetchall()

            for r in rows:

                (
                    decision_id,
                    bot_id,
                    symbol,
                    decision_ts,
                    action,
                    confidence,
                    scores_json,
                    reason_json,
                    setup_id,
                    strategy_id,
                    status,
                    created_at,
                    updated_at,
                ) = r

                bot_id = int(bot_id)

                if bot_id in decisions_by_bot:
                    continue

                bot = bots_by_id.get(bot_id)
                if not bot:
                    continue

                scores_payload = _safe_json(scores_json, {})
                reasons_payload = _safe_json(reason_json, [])

                guard = scores_payload.get("guardrails_result", {})
                guard_limits = guard.get("guardrails", {})

                decisions_by_bot[bot_id] = {
                    "id": decision_id,
                    "bot_id": bot_id,
                    "bot_name": bot["bot_name"],
                    "symbol": symbol,
                    "action": action,
                    "confidence": confidence,

                    "scores_json": scores_payload,

                    "requested_amount_eur": scores_payload.get("requested_amount_eur"),
                    "amount_eur": scores_payload.get("amount_eur"),
                    "guardrails_result": guard,
                    "guardrail_reason": scores_payload.get("guardrail_reason"),

                    "metrics": {
                        "position_size": scores_payload.get("position_size"),
                        "exposure_multiplier": scores_payload.get("exposure_multiplier"),
                        "max_trade_risk_eur": guard_limits.get("max_trade_risk_eur"),
                        "daily_allocation_eur": guard_limits.get("daily_allocation_eur"),
                    },

                    "reasons": reasons_payload,
                    "setup_id": setup_id,
                    "strategy_id": strategy_id,
                    "status": status,
                    "created_at": created_at,
                    "updated_at": updated_at,
                    "setup_match": scores_payload.get("setup_match"),
                    "trade_plan": None,
                }

                decision_ids.append(int(decision_id))

        return {
            "date": str(today),
            "scores": daily_scores,
            "decisions": list(decisions_by_bot.values()),
            "orders": [],
            "executions": [],
        }

    except Exception:
        logger.error("❌ bot/today error", exc_info=True)
        raise HTTPException(status_code=500, detail="Bot today ophalen mislukt")

    finally:
        conn.close()

# =====================================
# 📜 BOT HISTORY (laatste N dagen)
# =====================================
@router.get("/bot/history")
async def get_bot_history(
    days: int = 30,
    current_user: dict = Depends(get_current_user),
):
    """
    BOT DECISION HISTORY (GEEN TRADES)
    """

    user_id = current_user["id"]

    if days < 1:
        days = 1
    if days > 365:
        days = 365

    end_date = date.today()
    start_date = end_date - timedelta(days=days - 1)

    conn, cur = get_db_cursor()
    try:
        if not _table_exists(conn, "bot_decisions"):
            return []

        cur.execute(
            """
            SELECT
              d.id,
              d.bot_id,
              b.name AS bot_name,
              d.symbol,
              d.decision_ts,
              d.decision_date,
              d.action,
              d.confidence,
              d.scores_json,
              d.reason_json,
              d.status
            FROM bot_decisions d
            JOIN bot_configs b ON b.id = d.bot_id
            WHERE d.user_id=%s
              AND d.decision_date BETWEEN %s AND %s
            ORDER BY d.decision_date DESC, d.bot_id ASC, d.id DESC
            """,
            (user_id, start_date, end_date),
        )

        rows = cur.fetchall()
        out = []

        for (
            decision_id,
            bot_id,
            bot_name,
            symbol,
            decision_ts,
            decision_date,
            action,
            confidence,
            scores_json,
            reason_json,
            status,
        ) in rows:

            scores = _safe_json(scores_json, {})
            reasons = _safe_json(reason_json, [])

            # ✅ HARD DEFAULT setup_match
            setup_match = scores.get("setup_match") or {
                "status": "no_snapshot",
                "summary": "Geen strategie context",
                "detail": "Er is geen actief strategy snapshot beschikbaar.",
                "score": 10,
                "confidence": "low",
            }

            out.append({
                "decision_id": decision_id,
                "bot_id": bot_id,
                "bot_name": bot_name,
                "symbol": symbol,
                "date": decision_date,
                "decision_ts": decision_ts,
                "action": action,
                "confidence": confidence,
                "setup_match": setup_match,
                "reasons": reasons if isinstance(reasons, list) else [str(reasons)],
                "status": status,
            })

        return out

    except Exception:
        logger.error("❌ bot/history error", exc_info=True)
        raise HTTPException(status_code=500, detail="Bot history ophalen mislukt")
    finally:
        conn.close()


# =====================================
# 🔁 FORCE GENERATE (vandaag / datum)
# =====================================
@router.post("/bot/generate/today")
async def generate_bot_today(
    request: Request,
    current_user: dict = Depends(get_current_user),
):
    """
    FORCE GENERATE BOT DECISION (TODAY)

    CONTRACT:
    - Triggert ALLEEN de bot-agent
    - Stuurt GEEN decisions terug
    - Frontend moet NA afloop altijd /bot/today ophalen
    - executed = HARD LOCK
    - skipped = MAG opnieuw gegenereerd worden
    """

    from backend.ai_agents.trading_bot_agent import run_trading_bot_agent

    user_id = current_user["id"]
    body = await request.json()

    bot_id = body.get("bot_id")
    if not bot_id:
        return {
            "ok": False,
            "error": "bot_id ontbreekt",
        }

    report_date = date.today()
    if body.get("report_date"):
        report_date = date.fromisoformat(body["report_date"])

    conn = get_db_connection()
    if not conn:
        return {
            "ok": False,
            "bot_id": bot_id,
            "date": str(report_date),
            "error": "DB niet beschikbaar",
        }

    try:
        with conn.cursor() as cur:

            # ==========================================
            # Check bestaande decision vandaag
            # ==========================================
            cur.execute(
                """
                SELECT id, status
                FROM bot_decisions
                WHERE user_id=%s
                  AND bot_id=%s
                  AND decision_date=%s
                ORDER BY id DESC
                LIMIT 1
                """,
                (user_id, bot_id, report_date),
            )

            row = cur.fetchone()

            if row:
                decision_id, status = row

                # ❌ Executed = definitief klaar
                if status == "executed":
                    return {
                        "ok": False,
                        "bot_id": bot_id,
                        "date": str(report_date),
                        "error": "Decision is al uitgevoerd",
                    }

                # 🔄 Skipped = reset naar planned
                if status == "skipped":
                    cur.execute(
                        """
                        UPDATE bot_decisions
                        SET
                            status = 'planned',
                            updated_at = NOW()
                        WHERE id = %s
                          AND user_id = %s
                        """,
                        (decision_id, user_id),
                    )

        conn.commit()

        # ==========================================
        # 🚀 RUN TRADING BOT AGENT
        # ==========================================

        logger.info(
            f"🤖 run_trading_bot_agent user_id={user_id} bot_id={bot_id}"
        )

        result = run_trading_bot_agent(
            user_id=user_id,
            report_date=report_date,
            bot_id=bot_id,
        )

        # ==========================================
        # FAILSAFE RESPONSE
        # ==========================================

        if not result or not result.get("ok"):
            logger.warning(
                f"⚠️ bot agent failed user_id={user_id} bot_id={bot_id}"
            )
            return {
                "ok": False,
                "bot_id": bot_id,
                "date": str(report_date),
            }

        # ==========================================
        # SUCCESS
        # ==========================================

        return {
            "ok": True,
            "bot_id": bot_id,
            "date": str(report_date),
        }

    except Exception:
        logger.error("❌ generate_bot_today error", exc_info=True)
        return {
            "ok": False,
            "bot_id": bot_id,
            "date": str(report_date),
        }

    finally:
        conn.close()

# =====================================
# ✅ MARK EXECUTED (human-in-the-loop)
# =====================================
@router.post("/bot/mark_executed")
async def mark_bot_executed(
    request: Request,
    current_user: dict = Depends(get_current_user),
):
    """
    HUMAN-IN-THE-LOOP EXECUTION
    → gedelegeerd aan trading_bot_agent
    → identiek aan auto-execute, maar user-triggered
    """

    user_id = current_user["id"]
    body = await request.json()

    bot_id = body.get("bot_id")
    decision_id = body.get("decision_id")

    if not bot_id or not decision_id:
        raise HTTPException(
            status_code=400,
            detail="bot_id en decision_id zijn verplicht"
        )

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="DB niet beschikbaar")

    try:
        execute_manual_decision(
            conn=conn,
            user_id=user_id,
            bot_id=int(bot_id),
            decision_id=int(decision_id),
        )

        conn.commit()

        # 🔥 DIRECT SNAPSHOT NA EXECUTION
        try:
            snapshot_all_for_user(user_id, bucket="1h")
            snapshot_all_for_user(user_id, bucket="1d")
        except Exception:
            logger.exception(
                f"⚠️ Snapshot mislukt na manual execute | user_id={user_id}"
            )

        return {
            "ok": True,
            "bot_id": bot_id,
            "decision_id": decision_id,
            "mode": "manual",
        }

    except Exception as e:
        conn.rollback()
        logger.exception("❌ manual execute failed")
        raise HTTPException(
            status_code=409,
            detail=str(e),
        )
    finally:
        conn.close()


# =====================================
# 🟡 MANUAL ORDER (paper trade / discretionary)
# =====================================
@router.post("/orders/manual")
async def create_manual_order(
    request: Request,
    current_user: dict = Depends(get_current_user),
):
    """
    Handmatige trade invoeren.

    ✔ maakt bot_order
    ✔ maakt execution
    ✔ update bot_ledger
    ✔ portfolio wordt automatisch correct
    ✔ werkt voor paper trading
    ✔ future exchange ready
    """

    user_id = current_user["id"]
    body = await request.json()

    bot_id = body.get("bot_id")
    symbol = body.get("symbol", "BTC")
    side = body.get("side")
    quantity = body.get("quantity")
    price = body.get("price")

    if not bot_id or side not in ("buy", "sell") or not quantity or not price:
        raise HTTPException(
            status_code=400,
            detail="bot_id, side, quantity en price zijn verplicht",
        )

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="DB niet beschikbaar")

    try:
        with conn.cursor() as cur:

            # 1️⃣ ORDER
            cur.execute(
                """
                INSERT INTO bot_orders (
                    user_id,
                    bot_id,
                    decision_id,
                    symbol,
                    side,
                    order_type,
                    quantity,
                    limit_price,
                    status,
                    source,
                    created_at,
                    updated_at
                )
                VALUES (%s,%s,NULL,%s,%s,'market',%s,%s,'filled','manual',NOW(),NOW())
                RETURNING id
                """,
                (user_id, bot_id, symbol, side, quantity, price),
            )
            order_id = cur.fetchone()[0]

            # 2️⃣ EXECUTION
            cur.execute(
                """
                INSERT INTO bot_executions (
                    user_id,
                    bot_order_id,
                    filled_qty,
                    avg_fill_price,
                    status,
                    created_at
                )
                VALUES (%s,%s,%s,%s,'filled',NOW())
                """,
                (user_id, order_id, quantity, price),
            )

            # 3️⃣ LEDGER
            quantity = float(quantity)
            price = float(price)

            if side == "buy":
                cash_delta = -quantity * price
                qty_delta = quantity
            else:
                cash_delta = quantity * price
                qty_delta = -quantity

            cur.execute(
                """
                INSERT INTO bot_ledger (
                    user_id,
                    bot_id,
                    order_id,
                    entry_type,
                    cash_delta_eur,
                    qty_delta,
                    ts
                )
                VALUES (%s,%s,%s,'execute',%s,%s,NOW())
                """,
                (user_id, bot_id, order_id, cash_delta, qty_delta),
            )

        conn.commit()

        # 🔥 DIRECT SNAPSHOT NA TRADE
        try:
            snapshot_all_for_user(user_id, bucket="1h")
            snapshot_all_for_user(user_id, bucket="1d")
        except Exception:
            logger.exception(
                f"⚠️ Snapshot mislukt na manual order | user_id={user_id}"
            )

        return {
            "ok": True,
            "order_id": order_id,
            "mode": "manual",
        }

    except Exception as e:
        conn.rollback()
        logger.error("❌ manual order failed", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        conn.close()

# =====================================
# ⏭️ ADD BOT 
# =====================================
@router.post("/bot/configs")
async def create_bot_config(
    request: Request,
    current_user: dict = Depends(get_current_user),
):
    user_id = current_user["id"]
    body = await request.json()

    name = body.get("name")
    strategy_id = body.get("strategy_id")
    mode = body.get("mode", "manual")
    risk_profile = body.get("risk_profile", "balanced")

    if risk_profile not in ("conservative", "balanced", "aggressive"):
        raise HTTPException(status_code=400, detail="Ongeldig risk_profile")

    budget_total = body.get("budget_total_eur", 0)
    budget_daily = body.get("budget_daily_limit_eur", 0)
    budget_min = body.get("budget_min_order_eur", 0)
    budget_max = body.get("budget_max_order_eur", 0)

    max_asset_exposure_pct = body.get("max_asset_exposure_pct", 100)

    if not name:
        raise HTTPException(status_code=400, detail="Bot naam is verplicht")
    if not strategy_id:
        raise HTTPException(status_code=400, detail="strategy_id is verplicht")

    conn = get_db_connection()

    try:
        with conn.cursor() as cur:

            cur.execute(
                """
                INSERT INTO bot_configs (
                    user_id,
                    name,
                    strategy_id,
                    mode,
                    risk_profile,
                    budget_total_eur,
                    budget_daily_limit_eur,
                    budget_min_order_eur,
                    budget_max_order_eur,
                    max_asset_exposure_pct,
                    created_at,
                    updated_at
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW(),NOW())
                RETURNING id
                """,
                (
                    user_id,
                    name,
                    strategy_id,
                    mode,
                    risk_profile,
                    budget_total,
                    budget_daily,
                    budget_min,
                    budget_max,
                    max_asset_exposure_pct,
                ),
            )

            bot_id = cur.fetchone()[0]

        conn.commit()

        return {
            "ok": True,
            "id": bot_id,
        }

    except Exception:
        conn.rollback()
        logger.error("❌ Bot create failed", exc_info=True)
        raise HTTPException(status_code=500, detail="Bot aanmaken mislukt")

    finally:
        conn.close()

# =====================================
# ⏭️ SKIP TODAY
# =====================================
@router.post("/bot/skip")
async def skip_bot_today(
    request: Request,
    current_user: dict = Depends(get_current_user),
):
    user_id = current_user["id"]
    body = await request.json()

    bot_id = body.get("bot_id")
    if not bot_id:
        raise HTTPException(status_code=400, detail="bot_id is verplicht")

    report_date = date.today()
    if body.get("report_date"):
        report_date = date.fromisoformat(body["report_date"])

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="DB niet beschikbaar")

    try:
        with conn.cursor() as cur:
            # 🔒 HARD LOCK
            cur.execute(
                """
                UPDATE bot_decisions
                SET status='skipped',
                    updated_at=NOW()
                WHERE user_id=%s
                  AND bot_id=%s
                  AND decision_date=%s
                  AND status='planned'
                RETURNING id
                """,
                (user_id, bot_id, report_date),
            )
            row = cur.fetchone()

            if not row:
                raise HTTPException(
                    status_code=409,
                    detail="Decision is al afgehandeld"
                )

            decision_id = row[0]

            # orders → cancelled
            if _table_exists(conn, "bot_orders"):
                cur.execute(
                    """
                    UPDATE bot_orders
                    SET status='cancelled',
                        updated_at=NOW()
                    WHERE user_id=%s
                      AND bot_id=%s
                      AND decision_id=%s
                    """,
                    (user_id, bot_id, decision_id),
                )

        conn.commit()
        return {"ok": True, "status": "skipped"}

    except HTTPException:
        conn.rollback()
        raise
    except Exception:
        conn.rollback()
        logger.error("❌ skip_bot_today error", exc_info=True)
        raise HTTPException(status_code=500, detail="Bot skip mislukt")
    finally:
        conn.close()

# =====================================
# ⏭️ UPDATE BOT 
# =====================================
@router.put("/bot/configs/{bot_id}")
async def update_bot_config(
    bot_id: int,
    request: Request,
    current_user: dict = Depends(get_current_user),
):
    user_id = current_user["id"]
    body = await request.json()

    name = body.get("name")
    mode = body.get("mode")
    risk_profile = body.get("risk_profile")
    is_active = body.get("is_active")

    max_asset_exposure_pct = body.get("max_asset_exposure_pct")

    if risk_profile and risk_profile not in ("conservative", "balanced", "aggressive"):
        raise HTTPException(status_code=400, detail="Ongeldig risk_profile")

    if is_active is not None:
        is_active = bool(is_active)

    budget_total = body.get("budget_total_eur", body.get("total_eur"))
    budget_daily = body.get("budget_daily_limit_eur", body.get("daily_limit_eur"))
    budget_min = body.get("budget_min_order_eur", body.get("min_order_eur"))
    budget_max = body.get("budget_max_order_eur", body.get("max_order_eur"))

    def _num(v):
        if v in (None, ""):
            return None
        try:
            return float(v)
        except Exception:
            return None

    budget_total = _num(budget_total)
    budget_daily = _num(budget_daily)
    budget_min = _num(budget_min)
    budget_max = _num(budget_max)
    max_asset_exposure_pct = _num(max_asset_exposure_pct)

    conn = get_db_connection()

    try:
        with conn.cursor() as cur:

            cur.execute(
                """
                UPDATE bot_configs
                SET
                    name = COALESCE(%s, name),
                    mode = COALESCE(%s, mode),
                    risk_profile = COALESCE(%s, risk_profile),
                    is_active = COALESCE(%s, is_active),

                    budget_total_eur = COALESCE(%s, budget_total_eur),
                    budget_daily_limit_eur = COALESCE(%s, budget_daily_limit_eur),
                    budget_min_order_eur = COALESCE(%s, budget_min_order_eur),
                    budget_max_order_eur = COALESCE(%s, budget_max_order_eur),
                    max_asset_exposure_pct = COALESCE(%s, max_asset_exposure_pct),

                    updated_at = NOW()

                WHERE id = %s
                  AND user_id = %s

                RETURNING id
                """,
                (
                    name,
                    mode,
                    risk_profile,
                    is_active,
                    budget_total,
                    budget_daily,
                    budget_min,
                    budget_max,
                    max_asset_exposure_pct,
                    bot_id,
                    user_id,
                ),
            )

            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Bot niet gevonden")

        conn.commit()

        return {
            "ok": True,
            "bot_id": bot_id,
        }

    except Exception:
        conn.rollback()
        logger.error("❌ update_bot_config error", exc_info=True)
        raise HTTPException(status_code=500, detail="Bot bijwerken mislukt")

    finally:
        conn.close()


# =====================================
# 🗑️ DELETE BOT (HARD DELETE)
# =====================================
@router.delete("/bot/configs/{bot_id}")
async def delete_bot_config(
    bot_id: int,
    current_user: dict = Depends(get_current_user),
):
    user_id = current_user["id"]

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="DB niet beschikbaar")

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM bot_configs
                WHERE id = %s
                  AND user_id = %s
                RETURNING id
                """,
                (bot_id, user_id),
            )

            row = cur.fetchone()
            if not row:
                raise HTTPException(
                    status_code=404,
                    detail="Bot niet gevonden of geen toegang"
                )

        conn.commit()
        return {
            "ok": True,
            "bot_id": bot_id,
            "deleted": True,
        }

    except HTTPException:
        conn.rollback()
        raise
    except Exception:
        conn.rollback()
        logger.error("❌ delete_bot_config error", exc_info=True)
        raise HTTPException(status_code=500, detail="Bot verwijderen mislukt")
    finally:
        conn.close()


# =====================================
# 📦 BOT PORTFOLIOS (UI: Bot cards)
# =====================================
@router.get("/bot/portfolios")
async def get_bot_portfolios(current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]
    today = date.today()

    conn, cur = get_db_cursor()

    try:

        if not _table_exists(conn, "bot_configs"):
            return []

        cur.execute(
            """
            SELECT
              id,
              name,
              is_active,
              mode,
              COALESCE(risk_profile,'balanced'),
              COALESCE(budget_total_eur,0),
              COALESCE(budget_daily_limit_eur,0),
              COALESCE(budget_min_order_eur,0),
              COALESCE(budget_max_order_eur,0)
            FROM bot_configs
            WHERE user_id=%s
            ORDER BY id ASC
            """,
            (user_id,),
        )

        bots = cur.fetchall()

        if not bots:
            return []

        has_ledger = _table_exists(conn, "bot_ledger")
        has_market = _table_exists(conn, "market_data")

        last_price_by_symbol = {}

        out = []

        for (
            bot_id,
            name,
            is_active,
            mode,
            risk_profile,
            budget_total,
            budget_daily,
            budget_min,
            budget_max,
        ) in bots:

            bot_id = int(bot_id)
            symbol = "BTC"

            stats = {
                "net_cash_delta_eur": 0.0,
                "net_executed_cash_delta_eur": 0.0,
                "net_qty": 0.0,
                "today_spent_eur": 0.0,
                "today_reserved_eur": 0.0,
                "today_executed_eur": 0.0,
                "last_price": None,
                "position_value_eur": None,
                "invested_eur": 0.0,
                "available_eur": float(budget_total),
                "remaining_daily_eur": float(budget_daily),
            }

            if has_ledger:

                with conn.cursor() as c2:

                    # net balances
                    c2.execute(
                        """
                        SELECT
                          COALESCE(SUM(cash_delta_eur),0),
                          COALESCE(SUM(qty_delta),0)
                        FROM bot_ledger
                        WHERE user_id=%s
                        AND bot_id=%s
                        """,
                        (user_id, bot_id),
                    )

                    row = c2.fetchone()

                    stats["net_cash_delta_eur"] = float(row[0] or 0)
                    stats["net_qty"] = float(row[1] or 0)

                    # executed cash
                    c2.execute(
                        """
                        SELECT COALESCE(SUM(cash_delta_eur),0)
                        FROM bot_ledger
                        WHERE user_id=%s
                        AND bot_id=%s
                        AND entry_type='execute'
                        """,
                        (user_id, bot_id),
                    )

                    executed_cash = float((c2.fetchone() or [0])[0] or 0)

                    stats["net_executed_cash_delta_eur"] = executed_cash

                    # invested = absolute cash used
                    invested = abs(executed_cash)
                    stats["invested_eur"] = invested

                    # today spent
                    c2.execute(
                        """
                        SELECT COALESCE(SUM(ABS(cash_delta_eur)),0)
                        FROM bot_ledger
                        WHERE user_id=%s
                        AND bot_id=%s
                        AND entry_type='execute'
                        AND cash_delta_eur < 0
                        AND DATE(ts)=%s
                        """,
                        (user_id, bot_id, today),
                    )

                    stats["today_spent_eur"] = float((c2.fetchone() or [0])[0] or 0)

                    # reserve today
                    c2.execute(
                        """
                        SELECT COALESCE(SUM(ABS(cash_delta_eur)),0)
                        FROM bot_ledger
                        WHERE user_id=%s
                        AND bot_id=%s
                        AND entry_type='reserve'
                        AND cash_delta_eur < 0
                        AND DATE(ts)=%s
                        """,
                        (user_id, bot_id, today),
                    )

                    stats["today_reserved_eur"] = float((c2.fetchone() or [0])[0] or 0)

                    stats["today_executed_eur"] = stats["today_spent_eur"]

            # ----------------------------------
            # AVAILABLE BUDGET
            # ----------------------------------

            stats["available_eur"] = max(
                float(budget_total) - stats["invested_eur"],
                0,
            )

            stats["remaining_daily_eur"] = max(
                float(budget_daily) - stats["today_spent_eur"],
                0,
            )

            # ----------------------------------
            # MARKET PRICE
            # ----------------------------------

            if has_market:

                if symbol not in last_price_by_symbol:

                    with conn.cursor() as c3:

                        c3.execute(
                            """
                            SELECT price
                            FROM market_data
                            WHERE symbol=%s
                            ORDER BY timestamp DESC
                            LIMIT 1
                            """,
                            (symbol,),
                        )

                        prow = c3.fetchone()

                        last_price_by_symbol[symbol] = (
                            float(prow[0]) if prow and prow[0] else None
                        )

                stats["last_price"] = last_price_by_symbol.get(symbol)

                if stats["last_price"] is not None:

                    stats["position_value_eur"] = round(
                        stats["net_qty"] * stats["last_price"],
                        2,
                    )

            out.append(
                {
                    "bot_id": bot_id,
                    "name": name,
                    "is_active": bool(is_active),
                    "mode": mode,
                    "risk_profile": risk_profile,
                    "symbol": symbol,
                    "budget": {
                        "total_eur": float(budget_total),
                        "daily_limit_eur": float(budget_daily),
                        "min_order_eur": float(budget_min),
                        "max_order_eur": float(budget_max),
                    },
                    "stats": stats,
                }
            )

        return out

    except Exception:
        logger.error("❌ bot/portfolios error", exc_info=True)
        raise HTTPException(status_code=500, detail="Bot portfolios ophalen mislukt")

    finally:
        conn.close()


# =====================================
# 📊 BOT TRADES (echte uitgevoerde trades)
# - Bron: bot_ledger
# - Alleen entry_type='execute'
# - UI: tabel onder Bot Portfolio
# =====================================
@router.get("/bot/trades")
async def get_bot_trades(
    bot_id: int,
    limit: int = 20,
    current_user: dict = Depends(get_current_user),
):
    """
    Return ECHTE uitgevoerde trades.
    ENIGE BRON:
      - bot_executions (status + fills)
      - bot_orders     (symbol, side, amount)
    """

    user_id = current_user["id"]

    if limit < 1:
        limit = 1
    if limit > 100:
        limit = 100

    conn, cur = get_db_cursor()
    try:
        if not _table_exists(conn, "bot_executions") or not _table_exists(conn, "bot_orders"):
            return []

        cur.execute(
            """
            SELECT
              e.id                    AS execution_id,
              o.id                    AS order_id,
              o.symbol,
              o.side,
              e.filled_qty,
              e.avg_fill_price,
              o.quote_amount_eur,
              e.status,
              e.created_at
            FROM bot_executions e
            JOIN bot_orders o ON o.id = e.bot_order_id
            WHERE e.user_id = %s
              AND o.bot_id = %s
              AND e.status IN ('filled', 'partial')
            ORDER BY e.created_at DESC
            LIMIT %s
            """,
            (user_id, bot_id, limit),
        )

        trades = []
        for (
            execution_id,
            order_id,
            symbol,
            side,
            qty,
            price,
            amount_eur,
            status,
            created_at,
        ) in cur.fetchall():

            trades.append({
                "id": execution_id,
                "bot_id": bot_id,
                "symbol": symbol,
                "side": side,
                "qty": float(qty or 0),
                "price": float(price) if price is not None else None,
                "amount_eur": float(amount_eur) if amount_eur is not None else None,
                "executed_at": created_at,
                "mode": "auto" if status == "filled" else "manual",
            })

        return trades

    except Exception:
        logger.error("❌ bot/trades error", exc_info=True)
        raise HTTPException(status_code=500, detail="Bot trades ophalen mislukt")
    finally:
        conn.close()


# =====================================
# 💾 SAVE / UPSERT BOT Trade Plan
# =====================================
@router.post("/bot/trade-plan/{decision_id}")
async def save_trade_plan(
    decision_id: int,
    request: Request,
    current_user: dict = Depends(get_current_user),
):
    """
    Slaat manual edits op voor een decision trade plan.
    - UPSERT op decision_id
    - Frontend stuurt volledige trade_plan payload
    """

    user_id = current_user["id"]
    body = await request.json()

    entry_plan = body.get("entry_plan") or []
    stop_loss = body.get("stop_loss") or {}
    targets = body.get("targets") or []
    risk = body.get("risk") or {}

    # basic sanity checks
    if not isinstance(entry_plan, list):
        raise HTTPException(status_code=400, detail="entry_plan moet een lijst zijn")

    if not isinstance(targets, list):
        raise HTTPException(status_code=400, detail="targets moet een lijst zijn")

    if not isinstance(stop_loss, dict):
        raise HTTPException(status_code=400, detail="stop_loss moet een object zijn")

    if not isinstance(risk, dict):
        raise HTTPException(status_code=400, detail="risk moet een object zijn")

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="DB niet beschikbaar")

    try:
        with conn.cursor() as cur:

            # controleren of decision bestaat en bij user hoort
            cur.execute(
                """
                SELECT 1
                FROM bot_decisions
                WHERE id=%s
                  AND user_id=%s
                """,
                (decision_id, user_id),
            )

            if not cur.fetchone():
                raise HTTPException(status_code=404, detail="Decision niet gevonden")

            # UPSERT trade plan
            cur.execute(
                """
                INSERT INTO bot_trade_plans (
                    user_id,
                    decision_id,
                    entry_plan,
                    stop_loss,
                    targets,
                    risk_json,
                    created_at,
                    updated_at
                )
                VALUES (%s,%s,%s,%s,%s,%s,NOW(),NOW())
                ON CONFLICT (decision_id)
                DO UPDATE SET
                    entry_plan = EXCLUDED.entry_plan,
                    stop_loss  = EXCLUDED.stop_loss,
                    targets    = EXCLUDED.targets,
                    risk_json  = EXCLUDED.risk_json,
                    updated_at = NOW()
                RETURNING decision_id
                """,
                (
                    user_id,
                    decision_id,
                    json.dumps(entry_plan),
                    json.dumps(stop_loss),
                    json.dumps(targets),
                    json.dumps(risk),
                ),
            )

            _ = cur.fetchone()

        conn.commit()

        return {
            "ok": True,
            "decision_id": decision_id,
            "trade_plan": {
                "entry_plan": entry_plan,
                "stop_loss": stop_loss,
                "targets": targets,
                "risk": risk,
            },
        }

    except HTTPException:
        conn.rollback()
        raise

    except Exception:
        conn.rollback()
        logger.error("❌ save trade-plan error", exc_info=True)
        raise HTTPException(status_code=500, detail="Trade plan opslaan mislukt")

    finally:
        conn.close()


# =====================================
# 📊 BOT Trade Plan (fallback-safe)
# =====================================
@router.get("/bot/trade-plan/{decision_id}")
async def get_trade_plan(
    decision_id: int,
    current_user: dict = Depends(get_current_user),
):
    user_id = current_user["id"]

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="DB niet beschikbaar")

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT entry_plan, stop_loss, targets, risk_json
                FROM bot_trade_plans
                WHERE decision_id=%s
                  AND user_id=%s
                """,
                (decision_id, user_id),
            )

            row = cur.fetchone()

            # ✅ UI contract: nooit 404, altijd een plan-structuur
            if not row:
                return {
                    "entry_plan": [],
                    "stop_loss": {},
                    "targets": [],
                    "risk": {},
                }

            entry_plan, stop_loss, targets, risk_json = row

            return {
                "entry_plan": _safe_json(entry_plan, []),
                "stop_loss": _safe_json(stop_loss, {}),
                "targets": _safe_json(targets, []),
                "risk": _safe_json(risk_json, {}),
            }

    except Exception:
        logger.error("❌ trade-plan fetch error", exc_info=True)
        raise HTTPException(status_code=500, detail="Trade plan ophalen mislukt")
    finally:
        conn.close()


# =====================================
# 📈 PORTFOLIO BALANCE HISTORY (PRO)
# =====================================
@router.get("/portfolio/balance-history")
async def get_portfolio_balance_history(
    bucket: str = "1h",
    limit: int = 500,
    current_user: dict = Depends(get_current_user),
):
    """
    Global portfolio history.

    Nu inclusief 6 professionele metrics:
      - equity
      - cash
      - btc_qty
      - btc_value
      - invested
      - unrealized_pnl
    """

    user_id = current_user["id"]

    if limit < 1:
        limit = 1
    if limit > 2000:
        limit = 2000

    conn, cur = get_db_cursor()
    try:
        if not _table_exists(conn, "portfolio_balance_snapshots"):
            return []

        cur.execute(
            """
            SELECT
                ts,
                equity_eur,
                cash_eur,
                btc_qty,
                btc_value_eur,
                invested_eur,
                unrealized_pnl_eur
            FROM portfolio_balance_snapshots
            WHERE user_id = %s
              AND bucket = %s
            ORDER BY ts ASC
            LIMIT %s
            """,
            (user_id, bucket, limit),
        )

        rows = cur.fetchall()

        return [
            {
                "ts": r[0],
                "equity": float(r[1] or 0),
                "cash": float(r[2] or 0),
                "btc_qty": float(r[3] or 0),
                "btc_value": float(r[4] or 0),
                "invested": float(r[5] or 0),
                "unrealized_pnl": float(r[6] or 0),
            }
            for r in rows
        ]

    except Exception:
        logger.error("❌ portfolio balance history error", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Portfolio history ophalen mislukt",
        )
    finally:
        conn.close()


# =====================================
# 📈 BOT BALANCE HISTORY (PRO)
# =====================================
@router.get("/bot/balance-history")
async def get_bot_balance_history(
    bot_id: int,
    bucket: str = "1h",
    limit: int = 500,
    current_user: dict = Depends(get_current_user),
):
    """
    Per bot portfolio history.

    Inclusief:
      - equity
      - cash
      - btc_qty
      - price
      - invested
      - unrealized_pnl (live berekend)
    """

    user_id = current_user["id"]

    if limit < 1:
        limit = 1
    if limit > 2000:
        limit = 2000

    conn, cur = get_db_cursor()
    try:
        if not _table_exists(conn, "bot_portfolio_snapshots"):
            return []

        cur.execute(
            """
            SELECT
                ts,
                equity_eur,
                cash_eur,
                net_qty,
                price_eur,
                invested_eur
            FROM bot_portfolio_snapshots
            WHERE user_id = %s
              AND bot_id = %s
              AND bucket = %s
            ORDER BY ts ASC
            LIMIT %s
            """,
            (user_id, bot_id, bucket, limit),
        )

        rows = cur.fetchall()

        out = []
        for r in rows:
            ts, equity, cash, qty, price, invested = r

            qty = float(qty or 0)
            price = float(price or 0)
            invested = float(invested or 0)

            btc_value = qty * price
            unrealized = btc_value - invested

            out.append(
                {
                    "ts": ts,
                    "equity": float(equity or 0),
                    "cash": float(cash or 0),
                    "btc_qty": qty,
                    "price": price,
                    "invested": invested,
                    "btc_value": btc_value,
                    "unrealized_pnl": unrealized,
                }
            )

        return out

    except Exception:
        logger.error("❌ bot balance history error", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Bot balance history ophalen mislukt",
        )
    finally:
        conn.close()
