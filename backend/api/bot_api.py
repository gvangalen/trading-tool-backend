# backend/api/bot_api.py
import os
import json
import logging
from datetime import datetime, date, timedelta

from dotenv import load_dotenv
from fastapi import APIRouter, HTTPException, Request, Depends

from backend.utils.db import get_db_connection
from backend.utils.auth_utils import get_current_user

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

              b.budget_total_eur,
              b.budget_daily_limit_eur,
              b.budget_min_order_eur,
              b.budget_max_order_eur,

              b.created_at,
              b.updated_at,

              s.id            AS strategy_id,
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

                budget_total,
                budget_daily,
                budget_min,
                budget_max,

                created_at,
                updated_at,

                strategy_id,
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
                    "setup_id": setup_id,
                    "name": setup_name,
                    "symbol": symbol,
                    "timeframe": timeframe,
                }

            out.append(
                {
                    "id": bot_id,
                    "name": name,
                    "is_active": bool(is_active),
                    "mode": mode,
                    "cadence": cadence,

                    "budget": {
                        "total_eur": float(budget_total or 0),
                        "daily_limit_eur": float(budget_daily or 0),
                        "min_order_eur": float(budget_min or 0),
                        "max_order_eur": float(budget_max or 0),
                    },

                    "strategy": strategy,
                    "created_at": created_at,
                    "updated_at": updated_at,
                }
            )

        return out

    except Exception as e:
        logger.error("❌ get_bot_configs error", exc_info=True)
        raise HTTPException(status_code=500, detail="Bot configs ophalen mislukt")
    finally:
        conn.close()

# =====================================
# 📄 BOT TODAY (decisions + orders)
# =====================================
@router.get("/bot/today")
async def get_bot_today(current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]
    today = date.today()
    logger.info(f"🤖 [get/today] bot today voor user_id={user_id}")

    conn, cur = get_db_cursor()
    try:
        # ✅ altijd scores proberen op te halen (los van decisions)
        daily_scores = _get_daily_scores_row(conn, user_id, today) or {
            "macro": 10,
            "technical": 10,
            "market": 10,
            "setup": 10,
        }

        # Geen bot_decisions? Dan nog steeds scores teruggeven
        if not _table_exists(conn, "bot_decisions"):
            return {
                "date": str(today),
                "scores": daily_scores,  # ✅ nieuw
                "decisions": [],
                "orders": [],
            }

        # --------------------------
        # decisions (bot_decisions)
        # --------------------------
        cur.execute(
            """
            SELECT
              id,
              bot_id,
              symbol,
              decision_ts,
              decision_date,
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
            ORDER BY bot_id ASC NULLS LAST, id DESC
            """,
            (user_id, today),
        )
        drows = cur.fetchall()

        decisions = []
        decision_ids = []

        for r in drows:
            (
                decision_id,
                bot_id,
                symbol,
                decision_ts,
                decision_date,
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

            decision_ids.append(decision_id)

            # ✅ normaliseer jsonb/str/None → dict
            scores_obj = _safe_json(scores_json, daily_scores)
            reasons_obj = _safe_json(reason_json, [])

            decisions.append(
                {
                    "id": decision_id,
                    "bot_id": bot_id,
                    "symbol": symbol,
                    "decision_ts": decision_ts,
                    "date": decision_date,
                    "action": action,
                    "confidence": confidence,
                    "scores": scores_obj,        # ✅ altijd dict
                    "reasons": reasons_obj,      # ✅ altijd list/dict (afhankelijk van jouw opslag)
                    "setup_id": setup_id,
                    "strategy_id": strategy_id,
                    "status": status,
                    "created_at": created_at,
                    "updated_at": updated_at,
                }
            )

        # --------------------------
        # orders (bot_orders) via decision_id
        # --------------------------
        orders = []
        if decision_ids and _table_exists(conn, "bot_orders"):
            cur.execute(
                """
                SELECT
                  id,
                  bot_id,
                  decision_id,
                  symbol,
                  side,
                  order_type,
                  quantity,
                  quote_amount_eur,
                  limit_price,
                  time_in_force,
                  reduce_only,
                  exchange,
                  order_payload,
                  dry_run_payload,
                  status,
                  last_error,
                  created_at,
                  updated_at
                FROM bot_orders
                WHERE user_id=%s
                  AND decision_id = ANY(%s)
                ORDER BY bot_id ASC NULLS LAST, id DESC
                """,
                (user_id, decision_ids),
            )
            orows = cur.fetchall()

            for r in orows:
                (
                    order_id,
                    bot_id,
                    decision_id,
                    symbol,
                    side,
                    order_type,
                    quantity,
                    quote_amount_eur,
                    limit_price,
                    tif,
                    reduce_only,
                    exchange,
                    order_payload,
                    dry_run_payload,
                    status,
                    last_error,
                    created_at,
                    updated_at,
                ) = r

                amount_eur = (
                    float(quote_amount_eur) if quote_amount_eur is not None else None
                )

                orders.append(
                    {
                        "id": order_id,
                        "bot_id": bot_id,
                        "decision_id": decision_id,
                        "symbol": symbol,
                        "side": side,
                        "order_type": order_type,
                        "quantity": float(quantity) if quantity is not None else None,
                        "quote_amount_eur": float(quote_amount_eur) if quote_amount_eur is not None else None,
                        "amount_eur": amount_eur,
                        "limit_price": float(limit_price) if limit_price is not None else None,
                        "time_in_force": tif,
                        "reduce_only": bool(reduce_only) if reduce_only is not None else False,
                        "exchange": exchange,
                        "order_payload": _safe_json(order_payload, {}),
                        "dry_run_payload": _safe_json(dry_run_payload, {}),
                        "status": status,
                        "last_error": last_error,
                        "created_at": created_at,
                        "updated_at": updated_at,
                    }
                )

        # ✅ altijd scores mee teruggeven op top-level
        return {
            "date": str(today),
            "scores": daily_scores,   # ✅ nieuw
            "decisions": decisions,
            "orders": orders,
        }

    except Exception as e:
        logger.error(f"❌ bot/today error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Bot today ophalen mislukt.")
    finally:
        conn.close()

# =====================================
# 📜 BOT HISTORY (laatste N dagen)
# =====================================
@router.get("/bot/history")
async def get_bot_history(days: int = 30, current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]
    logger.info(f"🤖 [get/history] bot history voor user_id={user_id} (days={days})")

    if days < 1:
        days = 1
    if days > 365:
        days = 365

    end = date.today()
    start = end - timedelta(days=days - 1)

    conn, cur = get_db_cursor()
    try:
        if not _table_exists(conn, "bot_decisions"):
            return []

        cur.execute(
            """
            SELECT
              id,
              bot_id,
              symbol,
              decision_ts,
              decision_date,
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
              AND decision_date BETWEEN %s AND %s
            ORDER BY decision_date DESC, bot_id ASC NULLS LAST, id DESC
            """,
            (user_id, start, end),
        )
        rows = cur.fetchall()

        out = []
        for r in rows:
            (
                decision_id,
                bot_id,
                symbol,
                decision_ts,
                decision_date,
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

            out.append(
                {
                    "id": decision_id,
                    "bot_id": bot_id,
                    "symbol": symbol,
                    "decision_ts": decision_ts,
                    "date": decision_date,
                    "action": action,
                    "confidence": confidence,
                    "scores": scores_json or {},
                    "reasons": reason_json or {},
                    "setup_id": setup_id,
                    "strategy_id": strategy_id,
                    "status": status,
                    "created_at": created_at,
                    "updated_at": updated_at,
                }
            )

        return out

    except Exception as e:
        logger.error(f"❌ bot/history error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Bot history ophalen mislukt.")
    finally:
        conn.close()


# =====================================
# 🔁 FORCE GENERATE (vandaag / datum)
# - Lazy import van trading_bot_agent
# =====================================
@router.post("/bot/generate/today")
async def generate_bot_today(
    request: Request,
    current_user: dict = Depends(get_current_user),
):
    from backend.ai_agents.trading_bot_agent import run_trading_bot_agent

    user_id = current_user["id"]
    body = await request.json()

    report_date = date.today()
    if body.get("report_date"):
        report_date = date.fromisoformat(body["report_date"])

    bot_id = body.get("bot_id")

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="DB niet beschikbaar")

    try:
        if bot_id:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COALESCE(budget_total_eur,0)
                    FROM bot_configs
                    WHERE id=%s AND user_id=%s
                    """,
                    (bot_id, user_id),
                )
                row = cur.fetchone()
                if not row or float(row[0]) <= 0:
                    raise HTTPException(
                        status_code=400,
                        detail="Bot heeft geen budget ingesteld",
                    )

        result = run_trading_bot_agent(
            user_id=user_id,
            report_date=report_date,
            bot_id=bot_id,
        )

        if not result or not result.get("ok"):
            raise HTTPException(status_code=500, detail="Trading bot agent mislukt")

        return {
            "ok": True,
            "date": str(report_date),
            "bot_id": bot_id,
            "bots_processed": result.get("bots", 0),
            "decisions": result.get("decisions", []),
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
    user_id = current_user["id"]
    body = await request.json()

    bot_id = body.get("bot_id")
    if not bot_id:
        raise HTTPException(status_code=400, detail="❌ bot_id is verplicht.")

    report_date_str = body.get("report_date")
    run_date = date.today()
    if report_date_str:
        try:
            run_date = date.fromisoformat(report_date_str)
        except Exception:
            raise HTTPException(status_code=400, detail="❌ report_date moet YYYY-MM-DD zijn.")

    exchange = body.get("exchange")
    price = body.get("price")
    notes = body.get("notes")

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="❌ DB niet beschikbaar.")

    try:
        with conn.cursor() as cur:
            # 1) decision -> executed
            cur.execute(
                """
                UPDATE bot_decisions
                SET status='executed',
                    updated_at=NOW()
                WHERE user_id=%s
                  AND bot_id=%s
                  AND decision_date=%s
                """,
                (user_id, bot_id, run_date),
            )

            # 2) haal decision_id (nodig voor orders)
            cur.execute(
                """
                SELECT id
                FROM bot_decisions
                WHERE user_id=%s
                  AND bot_id=%s
                  AND decision_date=%s
                ORDER BY id DESC
                LIMIT 1
                """,
                (user_id, bot_id, run_date),
            )
            drow = cur.fetchone()
            decision_id = drow[0] if drow else None

            bot_order_id = None

            # 3) orders -> filled (NIET executed!)
            if decision_id and _table_exists(conn, "bot_orders"):
                cur.execute(
                    """
                    UPDATE bot_orders
                    SET status='filled',
                        updated_at=NOW()
                    WHERE user_id=%s
                      AND bot_id=%s
                      AND decision_id=%s
                    RETURNING id
                    """,
                    (user_id, bot_id, decision_id),
                )
                orow = cur.fetchone()
                bot_order_id = orow[0] if orow else None

            # 4) executions record (koppelt aan bot_order_id)
            if bot_order_id and _table_exists(conn, "bot_executions"):
                raw = {"notes": notes, "price": price}

                cur.execute(
                    """
                    INSERT INTO bot_executions
                      (user_id, bot_order_id, exchange, status, avg_fill_price, raw_response, created_at, updated_at)
                    VALUES (%s, %s, %s, 'filled', %s, %s, NOW(), NOW())
                    ON CONFLICT (bot_order_id)
                    DO UPDATE SET
                      exchange=EXCLUDED.exchange,
                      status='filled',
                      avg_fill_price=EXCLUDED.avg_fill_price,
                      raw_response=EXCLUDED.raw_response,
                      updated_at=NOW()
                    """,
                    (
                        user_id,
                        bot_order_id,
                        exchange,
                        float(price) if price is not None else None,
                        json.dumps(raw),
                    ),
                )

        conn.commit()
        return {"ok": True, "bot_id": bot_id, "date": str(run_date), "status": "executed"}

    except Exception as e:
        conn.rollback()
        logger.error(f"❌ bot/mark_executed error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Mark executed mislukt.")
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

    budget_total = body.get("budget_total_eur", 0)
    budget_daily = body.get("budget_daily_limit_eur", 0)
    budget_min = body.get("budget_min_order_eur", 0)
    budget_max = body.get("budget_max_order_eur", 0)

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
                    budget_total_eur,
                    budget_daily_limit_eur,
                    budget_min_order_eur,
                    budget_max_order_eur,
                    created_at,
                    updated_at
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,NOW(),NOW())
                RETURNING id
                """,
                (
                    user_id,
                    name,
                    strategy_id,
                    mode,
                    budget_total,
                    budget_daily,
                    budget_min,
                    budget_max,
                ),
            )
            bot_id = cur.fetchone()[0]

        conn.commit()
        return {"ok": True, "id": bot_id}

    except Exception:
        conn.rollback()
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
        raise HTTPException(status_code=400, detail="❌ bot_id is verplicht.")

    report_date_str = body.get("report_date")
    run_date = date.today()
    if report_date_str:
        try:
            run_date = date.fromisoformat(report_date_str)
        except Exception:
            raise HTTPException(status_code=400, detail="❌ report_date moet YYYY-MM-DD zijn.")

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="❌ DB niet beschikbaar.")

    try:
        with conn.cursor() as cur:
            # 1) decision -> skipped
            cur.execute(
                """
                UPDATE bot_decisions
                SET status='skipped',
                    updated_at=NOW()
                WHERE user_id=%s
                  AND bot_id=%s
                  AND decision_date=%s
                """,
                (user_id, bot_id, run_date),
            )

            # 2) decision_id ophalen
            cur.execute(
                """
                SELECT id
                FROM bot_decisions
                WHERE user_id=%s
                  AND bot_id=%s
                  AND decision_date=%s
                ORDER BY id DESC
                LIMIT 1
                """,
                (user_id, bot_id, run_date),
            )
            drow = cur.fetchone()
            decision_id = drow[0] if drow else None

            # 3) orders -> cancelled (NIET skipped!)
            if decision_id and _table_exists(conn, "bot_orders"):
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
        return {"ok": True, "bot_id": bot_id, "date": str(run_date), "status": "skipped"}

    except Exception as e:
        conn.rollback()
        logger.error(f"❌ bot/skip error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Bot skip mislukt.")
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

    budget_total = body.get("budget_total_eur")
    budget_daily = body.get("budget_daily_limit_eur")
    budget_min = body.get("budget_min_order_eur")
    budget_max = body.get("budget_max_order_eur")

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE bot_configs
                SET
                    name = COALESCE(%s, name),
                    mode = COALESCE(%s, mode),
                    budget_total_eur = COALESCE(%s, budget_total_eur),
                    budget_daily_limit_eur = COALESCE(%s, budget_daily_limit_eur),
                    budget_min_order_eur = COALESCE(%s, budget_min_order_eur),
                    budget_max_order_eur = COALESCE(%s, budget_max_order_eur),
                    updated_at = NOW()
                WHERE id = %s
                  AND user_id = %s
                """,
                (
                    name,
                    mode,
                    budget_total,
                    budget_daily,
                    budget_min,
                    budget_max,
                    bot_id,
                    user_id,
                ),
            )

        conn.commit()
        return {"ok": True, "bot_id": bot_id}

    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail="Bot bijwerken mislukt")
    finally:
        conn.close()
        
# =====================================
# ⏭️ DELETE BOT 
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
                WHERE id=%s
                  AND user_id=%s
                """,
                (bot_id, user_id),
            )

        conn.commit()
        return {"ok": True, "bot_id": bot_id}

    except Exception:
        conn.rollback()
        raise HTTPException(status_code=500, detail="Bot verwijderen mislukt")
    finally:
        conn.close()

@router.post("/bot/generate/one")
async def generate_single_bot_today(
    request: Request,
    current_user: dict = Depends(get_current_user),
):
    from backend.ai_agents.trading_bot_agent import run_trading_bot_agent

    user_id = current_user["id"]
    body = await request.json()

    bot_id = body.get("bot_id")
    if not bot_id:
        raise HTTPException(status_code=400, detail="bot_id is verplicht")

    report_date = date.today()
    if body.get("report_date"):
        report_date = date.fromisoformat(body["report_date"])

    result = run_trading_bot_agent(
        user_id=user_id,
        report_date=report_date,
        bot_id=bot_id,  # 🔥 BELANGRIJK
    )

    if not result.get("ok"):
        raise HTTPException(status_code=500, detail="Bot decision generatie mislukt")

    return {
        "ok": True,
        "bot_id": bot_id,
        "date": str(report_date),
        "decision": result.get("decisions", []),
    }


# =====================================
# 📊 BOT PORTFOLIOS / BUDGET DASHBOARD
# =====================================
@router.get("/bot/portfolios")
async def get_bot_portfolios(
    current_user: dict = Depends(get_current_user),
):
    """
    Volledig budget + portfolio overzicht per bot
    """
    user_id = current_user["id"]
    today = date.today()

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="DB niet beschikbaar")

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  b.id,
                  b.name,
                  b.is_active,
                  COALESCE(b.budget_total_eur, 0),
                  COALESCE(b.budget_daily_limit_eur, 0),
                  COALESCE(st.symbol, 'BTC')
                FROM bot_configs b
                LEFT JOIN strategies s ON s.id = b.strategy_id
                LEFT JOIN setups st    ON st.id = s.setup_id
                WHERE b.user_id = %s
                ORDER BY b.id ASC
                """,
                (user_id,),
            )
            bots = cur.fetchall()

        results = []

        for (
            bot_id,
            bot_name,
            is_active,
            total_budget,
            daily_limit,
            symbol,
        ) in bots:

            # =============================
            # SPENT TODAY
            # =============================
            spent_today = 0.0
            if _table_exists(conn, "bot_orders") and _table_exists(conn, "bot_decisions"):
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT COALESCE(SUM(o.quote_amount_eur), 0)
                        FROM bot_orders o
                        JOIN bot_decisions d ON d.id = o.decision_id
                        WHERE o.user_id=%s
                          AND o.bot_id=%s
                          AND d.decision_date=%s
                          AND o.status IN ('ready','filled')
                        """,
                        (user_id, bot_id, today),
                    )
                    spent_today = float(cur.fetchone()[0] or 0.0)

            # =============================
            # PORTFOLIO (filled only)
            # =============================
            units = 0.0
            cost_basis = 0.0

            if _table_exists(conn, "bot_orders") and _table_exists(conn, "bot_executions"):
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT
                          COALESCE(SUM(
                            CASE
                              WHEN o.side='buy'  THEN o.quantity
                              WHEN o.side='sell' THEN -o.quantity
                              ELSE 0
                            END
                          ), 0),
                          COALESCE(SUM(
                            CASE
                              WHEN o.side='buy'  THEN o.quote_amount_eur
                              WHEN o.side='sell' THEN -o.quote_amount_eur
                              ELSE 0
                            END
                          ), 0)
                        FROM bot_orders o
                        JOIN bot_executions e ON e.bot_order_id=o.id
                        WHERE o.user_id=%s
                          AND o.bot_id=%s
                          AND e.status='filled'
                        """,
                        (user_id, bot_id),
                    )
                    row = cur.fetchone()
                    units = float(row[0] or 0.0)
                    cost_basis = float(row[1] or 0.0)

            remaining_budget = max(float(total_budget) - float(cost_basis), 0.0)
            avg_entry = (cost_basis / units) if units > 0 else 0.0

            # =============================
            # CURRENT PRICE
            # =============================
            current_price = 0.0
            if _table_exists(conn, "market_data"):
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT price
                        FROM market_data
                        WHERE symbol=%s
                        ORDER BY timestamp DESC
                        LIMIT 1
                        """,
                        (symbol,),
                    )
                    r = cur.fetchone()
                    current_price = float(r[0]) if r else 0.0

            market_value = units * current_price
            pnl_eur = market_value - cost_basis
            pnl_pct = (pnl_eur / cost_basis * 100) if cost_basis > 0 else 0.0

            results.append(
                {
                    "bot_id": bot_id,
                    "bot_name": bot_name,
                    "symbol": symbol,
                    "status": "active" if is_active else "inactive",
                    "budget": {
                        "total_eur": round(float(total_budget), 2),
                        "remaining_eur": round(remaining_budget, 2),
                        "daily_limit_eur": round(float(daily_limit), 2),
                        "spent_today_eur": round(spent_today, 2),
                    },
                    "portfolio": {
                        "units": round(units, 8),
                        "avg_entry": round(avg_entry, 2),
                        "cost_basis_eur": round(cost_basis, 2),
                        "unrealized_pnl_eur": round(pnl_eur, 2),
                        "unrealized_pnl_pct": round(pnl_pct, 2),
                    },
                }
            )

        return results

    except Exception as e:
        logger.error("❌ get_bot_portfolios error", exc_info=True)
        raise HTTPException(status_code=500, detail="Bot portfolios ophalen mislukt")
    finally:
        conn.close()
