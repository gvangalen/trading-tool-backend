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

from backend.ai_agents.trading_bot_agent import run_trading_bot_agent

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


# =====================================
# 📦 BOT CONFIGS (actieve bots)
# =====================================
@router.get("/bot/configs")
async def get_bot_configs(current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]
    logger.info(f"🤖 [get/configs] bot_configs voor user_id={user_id}")

    conn, cur = get_db_cursor()
    try:
        if not _table_exists(conn, "bot_configs"):
            return []

        cols = _get_table_columns(conn, "bot_configs")
        pick = [c for c in ["id", "name", "symbol", "active", "mode", "rules_json", "allocation_json", "config_json", "created_at"] if c in cols]

        if "id" not in pick:
            return []

        q = f"SELECT {', '.join(pick)} FROM bot_configs WHERE user_id=%s"
        if "active" in cols:
            q += " AND active = TRUE"
        q += " ORDER BY id ASC"

        cur.execute(q, (user_id,))
        rows = cur.fetchall()

        out = []
        for r in rows:
            d = {pick[i]: r[i] for i in range(len(pick))}
            d["rules"] = _safe_json(d.get("rules_json"), {}) if "rules_json" in d else {}
            d["allocation"] = _safe_json(d.get("allocation_json"), {}) if "allocation_json" in d else {}
            d["config"] = _safe_json(d.get("config_json"), {}) if "config_json" in d else {}
            out.append(d)

        return out

    except Exception as e:
        logger.error(f"❌ bot/configs error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Bot configs ophalen mislukt.")
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
        if not _table_exists(conn, "bot_decisions"):
            return {"date": str(today), "decisions": [], "orders": []}

        # decisions
        dcols = _get_table_columns(conn, "bot_decisions")
        date_col = "date" if "date" in dcols else ("report_date" if "report_date" in dcols else None)
        if not date_col:
            return {"date": str(today), "decisions": [], "orders": []}

        pick_d = [c for c in [
            "id", "bot_id", "symbol", date_col, "action", "amount_eur", "confidence",
            "reason_json", "reasons_json", "scores_json", "setup_id", "strategy_id",
            "status", "created_at", "updated_at", "timestamp"
        ] if c in dcols]

        cur.execute(
            f"""
            SELECT {', '.join(pick_d)}
            FROM bot_decisions
            WHERE user_id=%s
              AND {date_col}=%s
            ORDER BY bot_id ASC NULLS LAST, id DESC
            """,
            (user_id, today),
        )
        drows = cur.fetchall()

        decisions = []
        for r in drows:
            d = {pick_d[i]: r[i] for i in range(len(pick_d))}
            d["date"] = d.get(date_col)
            if date_col != "date":
                d.pop(date_col, None)

            reasons = []
            if "reason_json" in d:
                reasons = _safe_json(d.get("reason_json"), [])
            elif "reasons_json" in d:
                reasons = _safe_json(d.get("reasons_json"), [])
            d["reasons"] = reasons or []
            d["scores"] = _safe_json(d.get("scores_json"), {}) if "scores_json" in d else {}

            decisions.append(d)

        # orders
        orders = []
        if _table_exists(conn, "bot_orders"):
            ocols = _get_table_columns(conn, "bot_orders")
            o_date_col = "date" if "date" in ocols else ("report_date" if "report_date" in ocols else None)

            if o_date_col:
                pick_o = [c for c in [
                    "id", "bot_id", "symbol", o_date_col, "side", "amount_eur", "status",
                    "order_json", "order_payload", "order_payload_json", "created_at", "timestamp"
                ] if c in ocols]

                cur.execute(
                    f"""
                    SELECT {', '.join(pick_o)}
                    FROM bot_orders
                    WHERE user_id=%s
                      AND {o_date_col}=%s
                    ORDER BY bot_id ASC NULLS LAST, id DESC
                    """,
                    (user_id, today),
                )
                orows = cur.fetchall()

                for r in orows:
                    o = {pick_o[i]: r[i] for i in range(len(pick_o))}
                    o["date"] = o.get(o_date_col)
                    if o_date_col != "date":
                        o.pop(o_date_col, None)

                    payload = None
                    for k in ["order_payload_json", "order_payload", "order_json"]:
                        if k in o:
                            payload = _safe_json(o.get(k), None)
                            break
                    o["order_payload"] = payload or {}
                    orders.append(o)

        return {"date": str(today), "decisions": decisions, "orders": orders}

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

        cols = _get_table_columns(conn, "bot_decisions")
        date_col = "date" if "date" in cols else ("report_date" if "report_date" in cols else None)
        if not date_col:
            return []

        pick = [c for c in [
            "id", "bot_id", "symbol", date_col, "action", "amount_eur", "confidence",
            "reason_json", "reasons_json", "scores_json", "setup_id", "strategy_id",
            "status", "created_at", "updated_at", "timestamp"
        ] if c in cols]

        cur.execute(
            f"""
            SELECT {', '.join(pick)}
            FROM bot_decisions
            WHERE user_id=%s
              AND {date_col} BETWEEN %s AND %s
            ORDER BY {date_col} DESC, bot_id ASC NULLS LAST, id DESC
            """,
            (user_id, start, end),
        )
        rows = cur.fetchall()

        out = []
        for r in rows:
            d = {pick[i]: r[i] for i in range(len(pick))}
            d["date"] = d.get(date_col)
            if date_col != "date":
                d.pop(date_col, None)

            reasons = []
            if "reason_json" in d:
                reasons = _safe_json(d.get("reason_json"), [])
            elif "reasons_json" in d:
                reasons = _safe_json(d.get("reasons_json"), [])
            d["reasons"] = reasons or []
            d["scores"] = _safe_json(d.get("scores_json"), {}) if "scores_json" in d else {}

            out.append(d)

        return out

    except Exception as e:
        logger.error(f"❌ bot/history error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Bot history ophalen mislukt.")
    finally:
        conn.close()


# =====================================
# 🔁 FORCE GENERATE (vandaag / datum)
# - Celery als beschikbaar, anders sync fallback
# =====================================
@router.post("/bot/generate/today")
async def generate_bot_today(
    request: Request,
    current_user: dict = Depends(get_current_user),
):
    user_id = current_user["id"]
    body = await request.json()
    report_date_str = body.get("report_date")

    run_date = date.today()
    if report_date_str:
        try:
            run_date = date.fromisoformat(report_date_str)
        except Exception:
            raise HTTPException(status_code=400, detail="❌ report_date moet YYYY-MM-DD zijn.")

    logger.info(f"🤖 [post/generate] run bot agent voor user_id={user_id} date={run_date}")

    # 1) Probeer Celery (als jij straks trading_bot_task maakt)
    try:
        from backend.celery_task.trading_bot_task import run_daily_trading_bot

        task = run_daily_trading_bot.delay(user_id=user_id, report_date=str(run_date))
        return {"ok": True, "queued": True, "task_id": getattr(task, "id", None), "date": str(run_date)}
    except Exception:
        logger.warning("⚠️ Celery niet beschikbaar/failed → sync fallback.", exc_info=True)

    # 2) Sync fallback
    result = run_trading_bot_agent(user_id=user_id, report_date=run_date)
    return {"ok": bool(result.get("ok")), "queued": False, "date": str(run_date), "result": result}


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

    symbol = body.get("symbol")
    side = body.get("side")
    amount_eur = body.get("amount_eur")
    price = body.get("price")
    exchange = body.get("exchange")
    notes = body.get("notes")

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="❌ DB niet beschikbaar.")

    try:
        # --- bot_decisions
        if _table_exists(conn, "bot_decisions"):
            cols = _get_table_columns(conn, "bot_decisions")
            date_col = "date" if "date" in cols else ("report_date" if "report_date" in cols else None)
            if date_col and "status" in cols and "bot_id" in cols:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        UPDATE bot_decisions
                        SET status='executed',
                            updated_at=NOW()
                        WHERE user_id=%s
                          AND bot_id=%s
                          AND {date_col}=%s
                        """,
                        (user_id, bot_id, run_date),
                    )

        # --- bot_orders
        if _table_exists(conn, "bot_orders"):
            cols = _get_table_columns(conn, "bot_orders")
            date_col = "date" if "date" in cols else ("report_date" if "report_date" in cols else None)
            if date_col and "status" in cols and "bot_id" in cols:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        UPDATE bot_orders
                        SET status='executed'
                        WHERE user_id=%s
                          AND bot_id=%s
                          AND {date_col}=%s
                        """,
                        (user_id, bot_id, run_date),
                    )

        # --- bot_executions (optioneel)
        if _table_exists(conn, "bot_executions"):
            cols = _get_table_columns(conn, "bot_executions")

            payload = {}
            if "user_id" in cols:
                payload["user_id"] = user_id
            if "bot_id" in cols:
                payload["bot_id"] = bot_id
            if "date" in cols:
                payload["date"] = run_date

            if "symbol" in cols and symbol:
                payload["symbol"] = symbol
            if "side" in cols and side:
                payload["side"] = side
            if "amount_eur" in cols and amount_eur is not None:
                payload["amount_eur"] = float(amount_eur)
            if "price" in cols and price is not None:
                payload["price"] = float(price)
            if "exchange" in cols and exchange:
                payload["exchange"] = exchange
            if "notes" in cols and notes:
                payload["notes"] = notes

            if "created_at" in cols:
                payload["created_at"] = datetime.utcnow()
            if "timestamp" in cols and "created_at" not in cols:
                payload["timestamp"] = datetime.utcnow()

            if payload:
                keys = list(payload.keys())
                vals = [payload[k] for k in keys]
                placeholders = ", ".join(["%s"] * len(keys))

                conflict = None
                if {"user_id", "bot_id", "date"}.issubset(set(cols)):
                    conflict = "(user_id, bot_id, date)"

                if conflict:
                    updates = ", ".join([f"{k}=EXCLUDED.{k}" for k in keys if k not in ("user_id", "bot_id", "date")])
                    sql = f"""
                        INSERT INTO bot_executions ({", ".join(keys)})
                        VALUES ({placeholders})
                        ON CONFLICT {conflict}
                        DO UPDATE SET {updates}
                    """
                else:
                    sql = f"""
                        INSERT INTO bot_executions ({", ".join(keys)})
                        VALUES ({placeholders})
                    """

                with conn.cursor() as cur:
                    cur.execute(sql, tuple(vals))

        conn.commit()
        return {"ok": True, "bot_id": bot_id, "date": str(run_date), "status": "executed"}

    except Exception as e:
        conn.rollback()
        logger.error(f"❌ bot/mark_executed error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Mark executed mislukt.")
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

    notes = body.get("notes")

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="❌ DB niet beschikbaar.")

    try:
        # --- bot_decisions
        if _table_exists(conn, "bot_decisions"):
            cols = _get_table_columns(conn, "bot_decisions")
            date_col = "date" if "date" in cols else ("report_date" if "report_date" in cols else None)

            if date_col and "status" in cols and "bot_id" in cols:
                with conn.cursor() as cur:
                    if "notes" in cols and notes:
                        cur.execute(
                            f"""
                            UPDATE bot_decisions
                            SET status='skipped',
                                notes=%s,
                                updated_at=NOW()
                            WHERE user_id=%s
                              AND bot_id=%s
                              AND {date_col}=%s
                            """,
                            (notes, user_id, bot_id, run_date),
                        )
                    else:
                        cur.execute(
                            f"""
                            UPDATE bot_decisions
                            SET status='skipped',
                                updated_at=NOW()
                            WHERE user_id=%s
                              AND bot_id=%s
                              AND {date_col}=%s
                            """,
                            (user_id, bot_id, run_date),
                        )

        # --- bot_orders
        if _table_exists(conn, "bot_orders"):
            cols = _get_table_columns(conn, "bot_orders")
            date_col = "date" if "date" in cols else ("report_date" if "report_date" in cols else None)

            if date_col and "status" in cols and "bot_id" in cols:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        UPDATE bot_orders
                        SET status='skipped'
                        WHERE user_id=%s
                          AND bot_id=%s
                          AND {date_col}=%s
                        """,
                        (user_id, bot_id, run_date),
                    )

        conn.commit()
        return {"ok": True, "bot_id": bot_id, "date": str(run_date), "status": "skipped"}

    except Exception as e:
        conn.rollback()
        logger.error(f"❌ bot/skip error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Bot skip mislukt.")
    finally:
        conn.close()
