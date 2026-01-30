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
    """
    HUMAN-IN-THE-LOOP EXECUTION

    CONTRACT (IDENTIEK AAN AUTO-MODE):
    - reserve-entry bestaat al (cash_delta < 0)
    - execute-entry:
        * GEEN cash_delta
        * WEL qty_delta
    - portfolio = ledger is single source of truth
    """

    user_id = current_user["id"]
    body = await request.json()

    bot_id = body.get("bot_id")
    if not bot_id:
        raise HTTPException(status_code=400, detail="bot_id is verplicht")

    report_date = date.today()
    if body.get("report_date"):
        report_date = date.fromisoformat(body["report_date"])

    # optioneel (handmatige invoer)
    executed_price = body.get("price")   # echte fill prijs (optioneel)
    executed_qty   = body.get("qty")     # echte fill qty (optioneel)
    notes          = body.get("notes")

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="DB niet beschikbaar")

    try:
        with conn.cursor() as cur:
            # ==========================================
            # 1️⃣ Pak geplande decision (LOCK)
            # ==========================================
            cur.execute(
                """
                SELECT id
                FROM bot_decisions
                WHERE user_id=%s
                  AND bot_id=%s
                  AND decision_date=%s
                  AND status='planned'
                FOR UPDATE
                """,
                (user_id, bot_id, report_date),
            )
            row = cur.fetchone()

            if not row:
                raise HTTPException(
                    status_code=409,
                    detail="Geen geplande decision om uit te voeren"
                )

            decision_id = int(row[0])

            # ==========================================
            # 2️⃣ Haal reserve-ledger entry op
            # ==========================================
            cur.execute(
                """
                SELECT cash_delta_eur, meta
                FROM bot_ledger
                WHERE user_id=%s
                  AND bot_id=%s
                  AND decision_id=%s
                  AND entry_type='reserve'
                ORDER BY ts DESC
                LIMIT 1
                """,
                (user_id, bot_id, decision_id),
            )
            reserve_row = cur.fetchone()

            if not reserve_row:
                raise HTTPException(
                    status_code=500,
                    detail="Reserve ledger entry ontbreekt"
                )

            reserved_cash = abs(float(reserve_row[0] or 0.0))
            reserve_meta  = reserve_row[1] or {}

            estimated_qty = float(reserve_meta.get("estimated_qty") or 0)

            # ==========================================
            # 3️⃣ Bepaal qty (prio: user → estimate)
            # ==========================================
            qty = float(executed_qty) if executed_qty is not None else estimated_qty

            if qty <= 0:
                raise HTTPException(
                    status_code=400,
                    detail="Kan geen execute doen zonder geldige qty"
                )

            # ==========================================
            # 4️⃣ Mark decision executed
            # ==========================================
            cur.execute(
                """
                UPDATE bot_decisions
                SET
                  status='executed',
                  executed_by='manual',
                  executed_at=NOW(),
                  updated_at=NOW()
                WHERE id=%s
                """,
                (decision_id,),
            )

            # ==========================================
            # 5️⃣ Ledger EXECUTE entry (⚠️ GEEN CASH)
            # ==========================================
            record_bot_ledger_entry(
                conn=conn,
                user_id=user_id,
                bot_id=bot_id,
                entry_type="execute",
                cash_delta_eur=0.0,               # 🚨 NOOIT nogmaals afboeken
                qty_delta=qty,                    # ✅ positie opbouwen
                symbol=DEFAULT_SYMBOL,
                decision_id=decision_id,
                note="Manual execution",
                meta={
                    "price": executed_price,
                    "reserved_cash": reserved_cash,
                    "notes": notes,
                },
            )

        conn.commit()

        return {
            "ok": True,
            "bot_id": bot_id,
            "decision_id": decision_id,
            "executed_qty": qty,
            "mode": "manual",
        }

    except HTTPException:
        conn.rollback()
        raise
    except Exception:
        conn.rollback()
        logger.exception("❌ mark_bot_executed crash")
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

# =====================================
# 📦 BOT PORTFOLIOS (UI: Bot cards)
# - Single source of truth: DB
# - Return per bot: budget + ledger stats + (optioneel) price snapshot
# =====================================
# =====================================
# 📦 BOT PORTFOLIOS (UI: Bot cards)
# - Single source of truth: DB
# - Return per bot: budget + ledger stats + (optioneel) price snapshot
# =====================================
@router.get("/bot/portfolios")
async def get_bot_portfolios(current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]
    today = date.today()

    conn, cur = get_db_cursor()
    try:
        # -----------------------------------------
        # 1) Bots ophalen (configs = bron voor UI)
        # -----------------------------------------
        if not _table_exists(conn, "bot_configs"):
            return []

        cur.execute(
            """
            SELECT
              id,
              name,
              is_active,
              mode,
              COALESCE(risk_profile, 'balanced') AS risk_profile,
              COALESCE(budget_total_eur, 0)       AS budget_total_eur,
              COALESCE(budget_daily_limit_eur, 0) AS budget_daily_limit_eur,
              COALESCE(budget_min_order_eur, 0)   AS budget_min_order_eur,
              COALESCE(budget_max_order_eur, 0)   AS budget_max_order_eur
            FROM bot_configs
            WHERE user_id = %s
            ORDER BY id ASC
            """,
            (user_id,),
        )
        bots = cur.fetchall()
        if not bots:
            return []

        # -----------------------------------------
        # 2) Ledger tables? Dan stats meenemen
        # -----------------------------------------
        has_ledger = _table_exists(conn, "bot_ledger")
        has_market = _table_exists(conn, "market_data")

        # Cache laatste prijzen per symbol (optioneel)
        last_price_by_symbol = {}

        out = []

        for (
            bot_id,
            name,
            is_active,
            mode,
            risk_profile,
            budget_total_eur,
            budget_daily_limit_eur,
            budget_min_order_eur,
            budget_max_order_eur,
        ) in bots:

            bot_id = int(bot_id)

            # -----------------------------
            # Default stats (altijd veilig)
            # -----------------------------
            stats = {
                "net_cash_delta_eur": 0.0,          # alle cash deltas (reserve + execute + eventueel refunds)
                "net_executed_cash_delta_eur": 0.0, # alleen execute cash deltas (echte trades)
                "net_qty": 0.0,                     # holdings qty (alle qty_delta)
                "today_spent_eur": 0.0,             # ✅ alleen execute vandaag (ECHT uitgegeven)
                "today_reserved_eur": 0.0,          # reserve entries vandaag (preview/lock)
                "today_executed_eur": 0.0,          # execute entries vandaag (trade)
                "last_price": None,
                "position_value_eur": None,
            }

            symbol = "BTC"  # default; later per bot uitbreiden via join strategy/setup

            # -----------------------------
            # Ledger stats (als table bestaat)
            # -----------------------------
            if has_ledger:
                with conn.cursor() as c2:
                    # (A) Net balances - alles
                    c2.execute(
                        """
                        SELECT
                          COALESCE(SUM(cash_delta_eur), 0) AS net_cash_delta_eur,
                          COALESCE(SUM(qty_delta), 0)      AS net_qty
                        FROM bot_ledger
                        WHERE user_id=%s
                          AND bot_id=%s
                        """,
                        (user_id, bot_id),
                    )
                    row = c2.fetchone() or (0, 0)
                    stats["net_cash_delta_eur"] = float(row[0] or 0.0)
                    stats["net_qty"] = float(row[1] or 0.0)

                    # (B) Net executed cash only (echte trades)
                    c2.execute(
                        """
                        SELECT COALESCE(SUM(cash_delta_eur), 0)
                        FROM bot_ledger
                        WHERE user_id=%s
                          AND bot_id=%s
                          AND entry_type='execute'
                        """,
                        (user_id, bot_id),
                    )
                    stats["net_executed_cash_delta_eur"] = float((c2.fetchone() or [0])[0] or 0.0)

                    # (C) Vandaag: ✅ spent = alleen execute outflow
                    c2.execute(
                        """
                        SELECT COALESCE(SUM(ABS(cash_delta_eur)), 0)
                        FROM bot_ledger
                        WHERE user_id=%s
                          AND bot_id=%s
                          AND entry_type='execute'
                          AND cash_delta_eur < 0
                          AND DATE(ts) = %s
                        """,
                        (user_id, bot_id, today),
                    )
                    stats["today_spent_eur"] = float((c2.fetchone() or [0])[0] or 0.0)

                    # (D) Vandaag: reserve
                    c2.execute(
                        """
                        SELECT COALESCE(SUM(ABS(cash_delta_eur)), 0)
                        FROM bot_ledger
                        WHERE user_id=%s
                          AND bot_id=%s
                          AND entry_type='reserve'
                          AND cash_delta_eur < 0
                          AND DATE(ts) = %s
                        """,
                        (user_id, bot_id, today),
                    )
                    stats["today_reserved_eur"] = float((c2.fetchone() or [0])[0] or 0.0)

                    # (E) Vandaag: execute
                    c2.execute(
                        """
                        SELECT COALESCE(SUM(ABS(cash_delta_eur)), 0)
                        FROM bot_ledger
                        WHERE user_id=%s
                          AND bot_id=%s
                          AND entry_type='execute'
                          AND cash_delta_eur < 0
                          AND DATE(ts) = %s
                        """,
                        (user_id, bot_id, today),
                    )
                    stats["today_executed_eur"] = float((c2.fetchone() or [0])[0] or 0.0)

            # -----------------------------
            # Market price snapshot (optioneel)
            # -----------------------------
            if has_market:
                if symbol not in last_price_by_symbol:
                    with conn.cursor() as c3:
                        c3.execute(
                            """
                            SELECT price
                            FROM market_data
                            WHERE symbol=%s
                              AND price IS NOT NULL
                            ORDER BY timestamp DESC
                            LIMIT 1
                            """,
                            (symbol,),
                        )
                        prow = c3.fetchone()
                        last_price_by_symbol[symbol] = float(prow[0]) if prow and prow[0] is not None else None

                stats["last_price"] = last_price_by_symbol.get(symbol)

                if stats["last_price"] is not None:
                    stats["position_value_eur"] = round(stats["net_qty"] * float(stats["last_price"]), 2)

            # -----------------------------
            # Output object (UI contract)
            # -----------------------------
            out.append(
                {
                    "bot_id": bot_id,
                    "name": name,
                    "is_active": bool(is_active),
                    "mode": mode,
                    "risk_profile": risk_profile or "balanced",
                    "budget": {
                        "total_eur": float(budget_total_eur or 0),
                        "daily_limit_eur": float(budget_daily_limit_eur or 0),
                        "min_order_eur": float(budget_min_order_eur or 0),
                        "max_order_eur": float(budget_max_order_eur or 0),
                    },
                    "symbol": symbol,
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
    Return ECHTE trades van een bot.

    Contract:
    - Alleen entry_type='execute'
    - qty_delta > 0 (BUY)
    - price uit meta (fallback: None)
    - Volledig los van bot_history / decisions
    """
    user_id = current_user["id"]

    if limit < 1:
        limit = 1
    if limit > 100:
        limit = 100

    conn, cur = get_db_cursor()
    try:
        if not _table_exists(conn, "bot_ledger"):
            return []

        cur.execute(
            """
            SELECT
              l.id,
              l.decision_id,
              l.symbol,
              l.qty_delta,
              l.cash_delta_eur,
              l.meta,
              l.ts,
              d.executed_by
            FROM bot_ledger l
            LEFT JOIN bot_decisions d
              ON d.id = l.decision_id
             AND d.user_id = l.user_id
            WHERE l.user_id = %s
              AND l.bot_id = %s
              AND l.entry_type = 'execute'
              AND l.qty_delta > 0
            ORDER BY l.ts DESC
            LIMIT %s
            """,
            (user_id, bot_id, limit),
        )

        rows = cur.fetchall()
        out = []

        for (
            ledger_id,
            decision_id,
            symbol,
            qty_delta,
            cash_delta_eur,
            meta,
            ts,
            executed_by,
        ) in rows:

            meta = _safe_json(meta, {})

            price = meta.get("price")
            reserved_cash = meta.get("reserved_cash")

            # fallback prijs (als qty + cash bekend zijn)
            if price is None and qty_delta and reserved_cash:
                try:
                    price = float(reserved_cash) / float(qty_delta)
                except Exception:
                    price = None

            out.append(
                {
                    "id": ledger_id,
                    "bot_id": bot_id,
                    "decision_id": decision_id,
                    "symbol": symbol,
                    "side": "buy" if qty_delta > 0 else "sell",
                    "qty": round(float(qty_delta), 8),
                    "price": round(float(price), 2) if price else None,
                    "amount_eur": round(float(reserved_cash), 2) if reserved_cash else None,
                    "executed_at": ts,
                    "mode": executed_by or "manual",
                }
            )

        return out

    except Exception:
        logger.error("❌ bot/trades error", exc_info=True)
        raise HTTPException(status_code=500, detail="Bot trades ophalen mislukt")
    finally:
        conn.close()
