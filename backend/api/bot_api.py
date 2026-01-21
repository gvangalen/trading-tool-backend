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
# 📄 BOT TODAY (decisions + orders + proposal)
# =====================================
@router.get("/bot/today")
async def get_bot_today(current_user: dict = Depends(get_current_user)):
    from backend.ai_agents.trading_bot_agent import build_order_proposal

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

        # --------------------------
        # Bot configs (budget info nodig voor proposal)
        # --------------------------
        bots_by_id = {}
        if _table_exists(conn, "bot_configs"):
            cur.execute(
                """
                SELECT
                  b.id,
                  b.name,
                  COALESCE(b.budget_total_eur, 0)        AS total_eur,
                  COALESCE(b.budget_daily_limit_eur, 0)  AS daily_limit_eur,
                  COALESCE(b.budget_min_order_eur, 0)    AS min_order_eur,
                  COALESCE(b.budget_max_order_eur, 0)    AS max_order_eur
                FROM bot_configs b
                WHERE b.user_id=%s
                """,
                (user_id,),
            )
            for r in cur.fetchall():
                bot_id, name, total_eur, daily_limit_eur, min_order_eur, max_order_eur = r
                bots_by_id[bot_id] = {
                    "bot_id": bot_id,
                    "bot_name": name,
                    "budget": {
                        "total_eur": float(total_eur or 0),
                        "daily_limit_eur": float(daily_limit_eur or 0),
                        "min_order_eur": float(min_order_eur or 0),
                        "max_order_eur": float(max_order_eur or 0),
                    },
                }

        # Geen bot_decisions? Dan nog steeds scores teruggeven
        if not _table_exists(conn, "bot_decisions"):
            return {
                "date": str(today),
                "scores": daily_scores,
                "decisions": [],
                "orders": [],
                "proposals": {},  # ✅ nieuw
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
        decisions_by_bot = {}

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

            scores_obj = _safe_json(scores_json, daily_scores)
            reasons_obj = _safe_json(reason_json, [])

            d = {
                "id": decision_id,
                "bot_id": bot_id,
                "symbol": symbol,
                "decision_ts": decision_ts,
                "date": decision_date,
                "action": action,
                "confidence": confidence,
                "scores": scores_obj,
                "reasons": reasons_obj,
                "setup_id": setup_id,
                "strategy_id": strategy_id,
                "status": status,
                "created_at": created_at,
                "updated_at": updated_at,
            }

            decisions.append(d)
            decisions_by_bot[bot_id] = d  # laatste per bot

        # --------------------------
        # orders (bot_orders) via decision_id
        # --------------------------
        orders = []
        orders_by_bot = {}

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

                o = {
                    "id": order_id,
                    "bot_id": bot_id,
                    "decision_id": decision_id,
                    "symbol": symbol,
                    "side": side,
                    "order_type": order_type,
                    "quantity": float(quantity) if quantity is not None else None,
                    "quote_amount_eur": float(quote_amount_eur) if quote_amount_eur is not None else None,
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

                orders.append(o)
                # laatste order per bot
                orders_by_bot[bot_id] = o

        # --------------------------
        # ✅ Build proposals per bot (FIX: geen user_id argument!)
        # --------------------------
        proposals = {}

        for bot_id, d in decisions_by_bot.items():
            bot = bots_by_id.get(bot_id)
            if not bot:
                continue

            # spent today (ledger)
            today_spent_eur = 0.0
            if _table_exists(conn, "bot_ledger"):
                cur.execute(
                    """
                    SELECT COALESCE(SUM(cash_delta_eur), 0)
                    FROM bot_ledger
                    WHERE user_id=%s
                      AND bot_id=%s
                      AND cash_delta_eur < 0
                      AND DATE(ts)=%s
                    """,
                    (user_id, bot_id, today),
                )
                today_spent_eur = abs(float(cur.fetchone()[0] or 0.0))

            # total balance (ledger)
            total_balance_eur = 0.0
            if _table_exists(conn, "bot_ledger"):
                cur.execute(
                    """
                    SELECT COALESCE(SUM(cash_delta_eur), 0)
                    FROM bot_ledger
                    WHERE user_id=%s
                      AND bot_id=%s
                    """,
                    (user_id, bot_id),
                )
                total_balance_eur = abs(float(cur.fetchone()[0] or 0.0))

            # decision -> proposal (alleen als buy + amount > 0)
            # NOTE: jouw decision bevat amount_eur alleen als bot_agent heeft gedraaid.
            # Als amount_eur niet in decision staat, laten we proposal None.
            proposal = None
            if isinstance(d, dict) and d.get("action") == "buy" and d.get("amount_eur"):
                proposal = build_order_proposal(
                    conn=conn,
                    bot=bot,
                    decision=d,
                    today_spent_eur=today_spent_eur,
                    total_balance_eur=total_balance_eur,
                )

            proposals[bot_id] = proposal

        return {
            "date": str(today),
            "scores": daily_scores,
            "decisions": decisions,
            "orders": orders,
            "proposals": proposals,  # ✅ nieuw voor UI (1 truth)
        }

    except Exception as e:
        logger.error("❌ bot/today error", exc_info=True)
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

    bot_id = body.get("bot_id")
    report_date = date.today()
    if body.get("report_date"):
        report_date = date.fromisoformat(body["report_date"])

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="DB niet beschikbaar")

    try:
        with conn.cursor() as cur:
            # 🧠 bestaande decision annuleren (indien aanwezig)
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

                if status in ("executed", "skipped"):
                    cur.execute(
                        """
                        UPDATE bot_decisions
                        SET status='cancelled',
                            updated_at=NOW()
                        WHERE id=%s
                        """,
                        (decision_id,),
                    )

            # 🔍 bot mode ophalen (AUTO / SEMI / MANUAL)
            cur.execute(
                """
                SELECT mode
                FROM bot_configs
                WHERE id=%s AND user_id=%s
                """,
                (bot_id, user_id),
            )
            brow = cur.fetchone()
            mode = brow[0] if brow else "manual"

        conn.commit()

        # 🚀 run agent
        result = run_trading_bot_agent(
            user_id=user_id,
            report_date=report_date,
            bot_id=bot_id,
            auto_execute=(mode == "auto"),
        )

        if not result or not result.get("ok"):
            raise HTTPException(status_code=500, detail="Bot agent mislukt")

        return {
            "ok": True,
            "bot_id": bot_id,
            "date": str(report_date),
            "mode": mode,
            "decisions": result.get("decisions", []),
        }

    except HTTPException:
        conn.rollback()
        raise
    except Exception:
        conn.rollback()
        logger.error("❌ generate_bot_today error", exc_info=True)
        raise HTTPException(status_code=500, detail="Decision generatie mislukt")
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
        raise HTTPException(status_code=400, detail="bot_id is verplicht")

    report_date = date.today()
    if body.get("report_date"):
        report_date = date.fromisoformat(body["report_date"])

    exchange = body.get("exchange")
    price = body.get("price")
    notes = body.get("notes")

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="DB niet beschikbaar")

    try:
        with conn.cursor() as cur:
            # 🔒 HARD LOCK: alleen planned mag uitgevoerd worden
            cur.execute(
                """
                UPDATE bot_decisions
                SET status='executed',
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

            # orders → filled
            bot_order_id = None
            if _table_exists(conn, "bot_orders"):
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

            # executions log
            if bot_order_id and _table_exists(conn, "bot_executions"):
                cur.execute(
                    """
                    INSERT INTO bot_executions
                      (user_id, bot_order_id, exchange, status, avg_fill_price, raw_response, created_at, updated_at)
                    VALUES (%s,%s,%s,'filled',%s,%s,NOW(),NOW())
                    ON CONFLICT (bot_order_id)
                    DO UPDATE SET
                      exchange=EXCLUDED.exchange,
                      avg_fill_price=EXCLUDED.avg_fill_price,
                      raw_response=EXCLUDED.raw_response,
                      updated_at=NOW()
                    """,
                    (
                        user_id,
                        bot_order_id,
                        exchange,
                        float(price) if price else None,
                        json.dumps({"notes": notes}),
                    ),
                )

        conn.commit()
        return {"ok": True, "status": "executed"}

    except HTTPException:
        conn.rollback()
        raise
    except Exception:
        conn.rollback()
        logger.error("❌ mark_executed error", exc_info=True)
        raise HTTPException(status_code=500, detail="Mark executed mislukt")
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
    """
    Update bot config (incl. budget).

    Accepteert beide payload-stijlen:
    - Nieuwe UI (BotPortfolioCard):
        { total_eur, daily_limit_eur, min_order_eur, max_order_eur }
    - Oude/andere API callers:
        { budget_total_eur, budget_daily_limit_eur, budget_min_order_eur, budget_max_order_eur }

    Name/mode zijn optioneel (dus geen 400 meer).
    """
    user_id = current_user["id"]

    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Ongeldige JSON body")

    # -----------------------------
    # Optional fields
    # -----------------------------
    name = body.get("name")
    mode = body.get("mode")

    # -----------------------------
    # Budget mapping (BELANGRIJK)
    # -----------------------------
    # UI stuurt: total_eur / daily_limit_eur / min_order_eur / max_order_eur
    # DB kolommen: budget_total_eur / budget_daily_limit_eur / budget_min_order_eur / budget_max_order_eur
    budget_total = body.get("budget_total_eur", body.get("total_eur"))
    budget_daily = body.get("budget_daily_limit_eur", body.get("daily_limit_eur"))
    budget_min = body.get("budget_min_order_eur", body.get("min_order_eur"))
    budget_max = body.get("budget_max_order_eur", body.get("max_order_eur"))

    def _num_or_none(v):
        if v is None or v == "":
            return None
        try:
            return float(v)
        except Exception:
            return None

    budget_total = _num_or_none(budget_total)
    budget_daily = _num_or_none(budget_daily)
    budget_min = _num_or_none(budget_min)
    budget_max = _num_or_none(budget_max)

    # -----------------------------
    # Simple validations (optioneel maar handig)
    # -----------------------------
    if budget_min is not None and budget_min < 0:
        raise HTTPException(status_code=400, detail="min_order_eur mag niet negatief zijn")
    if budget_max is not None and budget_max < 0:
        raise HTTPException(status_code=400, detail="max_order_eur mag niet negatief zijn")
    if budget_min is not None and budget_max is not None and budget_max > 0 and budget_min > budget_max:
        raise HTTPException(status_code=400, detail="min_order_eur mag niet hoger zijn dan max_order_eur")

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="DB niet beschikbaar")

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
                RETURNING
                    id,
                    name,
                    mode,
                    budget_total_eur,
                    budget_daily_limit_eur,
                    budget_min_order_eur,
                    budget_max_order_eur,
                    updated_at
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

            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Bot niet gevonden")

        conn.commit()

        (
            rid,
            rname,
            rmode,
            rtotal,
            rdaily,
            rmin,
            rmax,
            rupdated,
        ) = row

        return {
            "ok": True,
            "bot_id": rid,
            "name": rname,
            "mode": rmode,
            "budget": {
                "total_eur": float(rtotal or 0),
                "daily_limit_eur": float(rdaily or 0),
                "min_order_eur": float(rmin or 0),
                "max_order_eur": float(rmax or 0),
            },
            "updated_at": rupdated,
        }

    except HTTPException:
        conn.rollback()
        raise
    except Exception as e:
        conn.rollback()
        logger.error("❌ update_bot_config error", exc_info=True)
        raise HTTPException(status_code=500, detail="Bot bijwerken mislukt")
    finally:
        conn.close()


# =====================================
# 📊 BOT PORTFOLIOS / BUDGET DASHBOARD
# =====================================
@router.get("/bot/portfolios")
async def get_bot_portfolios(
    current_user: dict = Depends(get_current_user),
):
    """
    Volledig budget + portfolio overzicht per bot.
    Inclusief:
    - total / remaining / daily_limit / spent_today
    - min/max per trade (zodat UI dit kan tonen)
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

                  COALESCE(b.budget_total_eur, 0)        AS total_budget,
                  COALESCE(b.budget_daily_limit_eur, 0)  AS daily_limit,
                  COALESCE(b.budget_min_order_eur, 0)    AS min_order,
                  COALESCE(b.budget_max_order_eur, 0)    AS max_order,

                  COALESCE(st.symbol, 'BTC')             AS symbol
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
            min_order,
            max_order,
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
                              WHEN o.side='buy'  THEN COALESCE(o.quantity, 0)
                              WHEN o.side='sell' THEN -COALESCE(o.quantity, 0)
                              ELSE 0
                            END
                          ), 0) AS units,

                          COALESCE(SUM(
                            CASE
                              WHEN o.side='buy'  THEN COALESCE(o.quote_amount_eur, 0)
                              WHEN o.side='sell' THEN -COALESCE(o.quote_amount_eur, 0)
                              ELSE 0
                            END
                          ), 0) AS cost_basis
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

            # remaining alleen relevant als total_budget > 0
            if float(total_budget or 0) > 0:
                remaining_budget = max(float(total_budget) - float(cost_basis), 0.0)
            else:
                remaining_budget = 0.0

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
                    current_price = float(r[0]) if r and r[0] is not None else 0.0

            market_value = units * current_price
            pnl_eur = market_value - cost_basis
            pnl_pct = (pnl_eur / cost_basis * 100) if cost_basis > 0 else 0.0

            results.append(
                {
                    "bot_id": bot_id,
                    "bot_name": bot_name,
                    "symbol": (symbol or "BTC").upper(),
                    "status": "active" if is_active else "inactive",
                    "budget": {
                        "total_eur": round(float(total_budget or 0), 2),
                        "remaining_eur": round(float(remaining_budget or 0), 2),
                        "daily_limit_eur": round(float(daily_limit or 0), 2),
                        "spent_today_eur": round(float(spent_today or 0), 2),
                        "min_order_eur": round(float(min_order or 0), 2),
                        "max_order_eur": round(float(max_order or 0), 2),
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

    except Exception:
        logger.error("❌ get_bot_portfolios error", exc_info=True)
        raise HTTPException(status_code=500, detail="Bot portfolios ophalen mislukt")
    finally:
        conn.close()
