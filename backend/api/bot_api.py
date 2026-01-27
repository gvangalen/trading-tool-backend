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
              b.risk_profile,        -- ✅ NIEUW

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
                risk_profile,        # ✅ NIEUW

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
                    "risk_profile": risk_profile or "balanced",  # ✅

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
    """
    HARD UI-CONTRACT (DEFINITIEF):

    - Elke actieve bot heeft EXACT 1 decision per dag
    - Als die ontbreekt → agent wordt 1x auto-gedraaid (safety net)
    - NOOIT infinite loops
    - setup_match komt UITSLUITEND uit agent
    - Frontend doet GEEN fallback-logica
    - Logs maken meteen zichtbaar WAAR het fout gaat
    """
    from backend.ai_agents.trading_bot_agent import run_trading_bot_agent

    user_id = current_user["id"]
    today = date.today()

    logger.info(f"🤖 [bot/today] fetch start | user_id={user_id} | date={today}")

    conn, cur = get_db_cursor()
    try:
        # =====================================================
        # SCORES (altijd beschikbaar)
        # =====================================================
        daily_scores = _get_daily_scores_row(conn, user_id, today) or {
            "macro": 10,
            "technical": 10,
            "market": 10,
            "setup": 10,
        }

        # =====================================================
        # ACTIVE BOTS
        # =====================================================
        if not _table_exists(conn, "bot_configs"):
            return {
                "date": str(today),
                "scores": daily_scores,
                "decisions": [],
                "orders": [],
                "proposals": {},
            }

        cur.execute(
            """
            SELECT
              b.id,
              b.name,
              COALESCE(st.symbol, 'BTC')      AS symbol,
              COALESCE(st.timeframe, '—')     AS timeframe,
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
                "proposals": {},
            }

        bots_by_id = {
            int(r[0]): {
                "bot_id": int(r[0]),
                "bot_name": r[1],
                "symbol": (r[2] or "BTC").upper(),
                "timeframe": r[3] or "—",
                "strategy_type": r[4],
            }
            for r in bot_rows
        }

        # =====================================================
        # EXISTING DECISIONS TODAY
        # =====================================================
        decisions_by_bot = {}
        decision_ids = []

        if _table_exists(conn, "bot_decisions"):
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
                ORDER BY bot_id ASC, id DESC
                """,
                (user_id, today),
            )

            for r in cur.fetchall():
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

                bot_id = int(bot_id)
                if bot_id in decisions_by_bot:
                    continue

                scores_payload = _safe_json(scores_json, {})
                reasons_payload = _safe_json(reason_json, [])

                decisions_by_bot[bot_id] = {
                    "id": int(decision_id),
                    "bot_id": bot_id,
                    "symbol": (symbol or bots_by_id[bot_id]["symbol"]).upper(),
                    "decision_ts": decision_ts,
                    "date": decision_date,
                    "action": action,
                    "confidence": confidence,
                    "scores": scores_payload or daily_scores,
                    "reasons": reasons_payload if isinstance(reasons_payload, list) else [str(reasons_payload)],
                    "setup_id": setup_id,
                    "strategy_id": strategy_id,
                    "status": status,
                    "created_at": created_at,
                    "updated_at": updated_at,
                    "setup_match": scores_payload.get("setup_match"),
                }

                decision_ids.append(int(decision_id))

        # =====================================================
        # 🔁 AUTO-RUN SAFETY NET (MAX 1x)
        # =====================================================
        missing_bot_ids = [
            bot_id for bot_id in bots_by_id.keys()
            if bot_id not in decisions_by_bot
        ]

        if missing_bot_ids:
            logger.warning(
                f"⚠️ [bot/today] missing decisions → auto-run agent | "
                f"user_id={user_id} bots={missing_bot_ids}"
            )

            # 🔒 BELANGRIJK: agent maar 1x draaien
            run_trading_bot_agent(
                user_id=user_id,
                report_date=today,
            )

            # 🔁 decisions opnieuw ophalen (GEEN recursion)
            return await get_bot_today(current_user)

        # =====================================================
        # ORDERS
        # =====================================================
        orders = []
        if decision_ids and _table_exists(conn, "bot_orders"):
            cur.execute(
                """
                SELECT id, bot_id, decision_id, symbol, side, status
                FROM bot_orders
                WHERE user_id=%s
                  AND decision_id = ANY(%s)
                """,
                (user_id, decision_ids),
            )
            for r in cur.fetchall():
                orders.append(
                    {
                        "id": r[0],
                        "bot_id": r[1],
                        "decision_id": r[2],
                        "symbol": r[3],
                        "side": r[4],
                        "status": r[5],
                    }
                )

        # =====================================================
        # ✅ FINAL RETURN
        # =====================================================
        return {
            "date": str(today),
            "scores": daily_scores,
            "decisions": list(decisions_by_bot.values()),
            "orders": orders,
            "proposals": {},
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
    """
    FORCE GENERATE BOT DECISION (TODAY)

    CONTRACT:
    - Deze endpoint triggert ALLEEN de bot-agent
    - Stuurt GEEN decisions terug
    - Frontend moet NA afloop altijd /bot/today ophalen
    - NOOIT een 500 naar frontend
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

            # Executed / skipped mag niet opnieuw
            if row and row[1] in ("executed", "skipped"):
                return {
                    "ok": False,
                    "bot_id": bot_id,
                    "date": str(report_date),
                    "error": "Decision is al afgehandeld",
                }

            # ==========================================
            # Bot mode ophalen (manual / auto)
            # ==========================================
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

        # ==========================================
        # 🚀 RUN TRADING BOT AGENT
        # ==========================================
        logger.info(
            f"🤖 run_trading_bot_agent user_id={user_id} bot_id={bot_id} mode={mode}"
        )

        result = run_trading_bot_agent(
            user_id=user_id,
            report_date=report_date,
            bot_id=bot_id,
            auto_execute=(mode == "auto"),
        )

        # ==========================================
        # FAILSAFE RESPONSE (NOOIT 500)
        # ==========================================
        if not result or not result.get("ok"):
            logger.warning(
                f"⚠️ bot agent failed user_id={user_id} bot_id={bot_id}"
            )
            return {
                "ok": False,
                "bot_id": bot_id,
                "date": str(report_date),
                "mode": mode,
            }

        # ==========================================
        # ✅ SUCCESS — frontend haalt /bot/today op
        # ==========================================
        return {
            "ok": True,
            "bot_id": bot_id,
            "date": str(report_date),
            "mode": mode,
        }

    except Exception:
        logger.error("❌ generate_bot_today error", exc_info=True)

        # 🔒 HARD FAILSAFE — NOOIT 500
        return {
            "ok": False,
            "bot_id": bot_id,
            "date": str(report_date),
            "mode": "unknown",
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
    risk_profile = body.get("risk_profile", "balanced")  # ✅ NIEUW

    if risk_profile not in ("conservative", "balanced", "aggressive"):
        raise HTTPException(status_code=400, detail="Ongeldig risk_profile")

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
                    risk_profile,          -- ✅
                    budget_total_eur,
                    budget_daily_limit_eur,
                    budget_min_order_eur,
                    budget_max_order_eur,
                    created_at,
                    updated_at
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW(),NOW())
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
    user_id = current_user["id"]
    body = await request.json()

    # =============================
    # FIELDS
    # =============================
    name = body.get("name")
    mode = body.get("mode")
    risk_profile = body.get("risk_profile")
    is_active = body.get("is_active")   # ✅ CRUCIAAL (pause / resume)

    if risk_profile and risk_profile not in ("conservative", "balanced", "aggressive"):
        raise HTTPException(status_code=400, detail="Ongeldig risk_profile")

    if is_active is not None:
        is_active = bool(is_active)

    # =============================
    # BUDGET FIELDS (flexibel)
    # =============================
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

    # =============================
    # DB UPDATE
    # =============================
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
                    is_active = COALESCE(%s, is_active),      -- ✅ FIX
                    budget_total_eur = COALESCE(%s, budget_total_eur),
                    budget_daily_limit_eur = COALESCE(%s, budget_daily_limit_eur),
                    budget_min_order_eur = COALESCE(%s, budget_min_order_eur),
                    budget_max_order_eur = COALESCE(%s, budget_max_order_eur),
                    updated_at = NOW()
                WHERE id = %s
                  AND user_id = %s
                RETURNING id, name, mode, risk_profile, is_active
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
            "bot_id": row[0],
            "name": row[1],
            "mode": row[2],
            "risk_profile": row[3],
            "is_active": row[4],
        }

    except HTTPException:
        conn.rollback()
        raise
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
