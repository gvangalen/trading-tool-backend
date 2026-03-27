print("✅ strategy_api.py geladen!")

from fastapi import APIRouter, HTTPException, Request, Depends
from fastapi.responses import StreamingResponse
from psycopg2.extras import RealDictCursor
import json, csv, io, logging
from datetime import datetime
from backend.utils.data_normalizers import (
    normalize_targets,
    normalize_number,
    normalize_string,
    normalize_array
)

router = APIRouter()
logger = logging.getLogger(__name__)

# ==========================================================
# DB & AUTH
# ==========================================================
from backend.utils.db import get_db_connection
from backend.utils.auth_utils import get_current_user
from backend.api.onboarding_api import mark_step_completed


# ==========================================================
# 🧩 FORMATTER — ÉÉN waarheid voor strategy cards & edit
# ==========================================================
def format_strategy_row(row: dict):
    if not row:
        return None

    data = row.get("data") or {}

    entry = normalize_number(row.get("entry") or data.get("entry"))
    stop_loss = normalize_number(row.get("stop_loss") or data.get("stop_loss"))
    base_amount = normalize_number(row.get("base_amount"))
    targets = normalize_targets(row.get("targets") or data.get("targets"))

    name = normalize_string(row.get("name") or data.get("name"))
    symbol = normalize_string(
        row.get("setup_symbol") or data.get("symbol")
    )
    timeframe = normalize_string(
        row.get("setup_timeframe") or data.get("timeframe")
    )
    explanation = normalize_string(
        row.get("explanation") or data.get("explanation")
    )
    risk_profile = normalize_string(
        row.get("risk_profile") or data.get("risk_profile")
    )

    tags = normalize_array(data.get("tags"))
    favorite = bool(data.get("favorite", False))

    created_at = (
        row.get("created_at").isoformat()
        if row.get("created_at")
        else None
    )

    return {
        "id": row.get("id"),
        "setup_id": row.get("setup_id"),
        "setup_name": row.get("setup_name"),

        "name": name,
        "setup_type": row.get("setup_type"),

        "execution_mode": row.get("execution_mode"),
        "base_amount": base_amount,

        "decision_curve": row.get("decision_curve"),
        "decision_curve_name": data.get("decision_curve_name"),
        "decision_curve_id": row.get("decision_curve_id") or data.get("decision_curve_id"),

        "symbol": symbol,
        "timeframe": timeframe,

        "entry": entry,
        "targets": targets,
        "stop_loss": stop_loss,

        "explanation": explanation,
        "ai_explanation": data.get("ai_explanation"),

        "risk_profile": risk_profile,

        "tags": tags,
        "favorite": favorite,

        "created_at": created_at,
    }
    

# ==========================================================
# 1️⃣ CREATE STRATEGY
# ==========================================================
@router.post("/strategies")
async def save_strategy(
    request: Request,
    current_user: dict = Depends(get_current_user)
):
    user_id = current_user["id"]
    data = await request.json()

    execution_mode = (data.get("execution_mode") or "").lower()
    if execution_mode not in ["fixed", "custom"]:
        raise HTTPException(400, "Ongeldige execution_mode")

    if not data.get("setup_id"):
        raise HTTPException(400, "setup_id is verplicht")

    if not data.get("base_amount"):
        raise HTTPException(400, "base_amount is verplicht")

    if execution_mode == "custom" and not data.get("decision_curve"):
        raise HTTPException(400, "decision_curve is verplicht bij custom execution")

    conn = get_db_connection()

    try:
        with conn.cursor() as cur:
            # --------------------------------------------------
            # VERIFY SETUP
            # --------------------------------------------------
            cur.execute("""
                SELECT id, name, symbol, timeframe, setup_type
                FROM setups
                WHERE id=%s AND user_id=%s
            """, (data["setup_id"], user_id))
            setup_row = cur.fetchone()

            if not setup_row:
                raise HTTPException(403, "Setup niet van gebruiker")

            setup_id, setup_name, setup_symbol, setup_timeframe, setup_type = setup_row
            setup_type = (setup_type or "").lower()

            if setup_type not in ["dca", "trade"]:
                raise HTTPException(400, "Ongeldig setup_type")

            # --------------------------------------------------
            # TRADE VALIDATIE
            # --------------------------------------------------
            if setup_type == "trade":
                if data.get("entry") is None or data.get("stop_loss") is None:
                    raise HTTPException(400, "entry en stop_loss verplicht voor trade")
                if not data.get("targets"):
                    raise HTTPException(400, "targets verplicht voor trade")

            # --------------------------------------------------
            # DUPLICATE CHECK
            # --------------------------------------------------
            cur.execute("""
                SELECT id FROM strategies
                WHERE setup_id=%s AND user_id=%s
            """, (setup_id, user_id))

            if cur.fetchone():
                raise HTTPException(409, "Strategie bestaat al voor deze setup")

            # --------------------------------------------------
            # NAME
            # --------------------------------------------------
            strategy_name = (data.get("name") or "").strip()
            if not strategy_name:
                strategy_name = f"{setup_type.upper()} {setup_symbol} {setup_timeframe}"

            # --------------------------------------------------
            # CURVE SAVE
            # --------------------------------------------------
            curve_id = None
            if execution_mode == "custom":
                curve_name = data.get("decision_curve_name") or f"Curve {datetime.utcnow():%Y%m%d-%H%M}"

                cur.execute("""
                    INSERT INTO indicator_curves (
                        user_id, domain, indicator, curve, name,
                        is_active, is_preset, created_at
                    )
                    VALUES (%s,'execution','position_size',%s,%s,true,false,NOW())
                    RETURNING id
                """, (
                    user_id,
                    json.dumps(data["decision_curve"]),
                    curve_name
                ))

                curve_id = cur.fetchone()[0]

            # --------------------------------------------------
            # FINAL DATA
            # --------------------------------------------------
            data["setup_type"] = setup_type
            data["symbol"] = setup_symbol
            data["timeframe"] = setup_timeframe
            data["setup_name"] = setup_name

            # --------------------------------------------------
            # INSERT
            # --------------------------------------------------
            cur.execute("""
                INSERT INTO strategies (
                    setup_id, name, setup_type,
                    execution_mode, base_amount,
                    decision_curve, decision_curve_id,
                    entry, targets, stop_loss,
                    explanation, risk_profile,
                    data, created_at, user_id
                )
                VALUES (
                    %s,%s,%s,%s,%s,
                    %s,%s,
                    %s,%s,%s,
                    %s,%s,%s,
                    NOW(),%s
                )
                RETURNING id
            """, (
                setup_id,
                strategy_name,
                setup_type,
                execution_mode,
                data.get("base_amount"),
                json.dumps(data.get("decision_curve")) if data.get("decision_curve") else None,
                curve_id,
                str(data.get("entry")) if data.get("entry") is not None else None,
                data.get("targets"),
                str(data.get("stop_loss")) if data.get("stop_loss") is not None else None,
                data.get("explanation"),
                data.get("risk_profile"),
                json.dumps(data),
                user_id
            ))

            strategy_id = cur.fetchone()[0]
            conn.commit()

        mark_step_completed(conn, user_id, "strategy")

        return {"id": strategy_id, "message": "✅ Strategie opgeslagen"}

    finally:
        conn.close()
        
# ==========================================================
# 2️⃣ QUERY STRATEGIES (cards + bot dropdown)
# ==========================================================
@router.post("/strategies/query")
async def query_strategies(
    request: Request,
    current_user: dict = Depends(get_current_user)
):
    filters = await request.json()
    user_id = current_user["id"]

    conn = get_db_connection()

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            q = """
                SELECT
                    s.*,
                    st.symbol AS setup_symbol,
                    st.timeframe AS setup_timeframe,
                    st.name AS setup_name
                FROM strategies s
                LEFT JOIN setups st
                    ON st.id = s.setup_id
                WHERE s.user_id=%s
            """
            p = [user_id]

            if filters.get("symbol"):
                q += " AND st.symbol=%s"
                p.append(filters["symbol"])

            if filters.get("timeframe"):
                q += " AND st.timeframe=%s"
                p.append(filters["timeframe"])

            q += " ORDER BY s.created_at DESC"

            cur.execute(q, tuple(p))
            rows = cur.fetchall()

        return [format_strategy_row(r) for r in rows]

    finally:
        conn.close()
        
# ==========================================================
# 3️⃣ GENERATE STRATEGY (AI)
# ==========================================================
@router.post("/strategies/generate/{setup_id}")
async def generate_strategy_for_setup(
    setup_id: int,
    current_user: dict = Depends(get_current_user)
):
    from backend.celery_task.strategy_task import generate_for_setup
    task = generate_for_setup.delay(
        user_id=current_user["id"],
        setup_id=setup_id
    )
    return {"task_id": task.id}


# ==========================================================
# 4️⃣ UPDATE STRATEGY (incl curve editor)
# ==========================================================
@router.put("/strategies/{strategy_id}")
async def update_strategy(
    strategy_id: int,
    request: Request,
    current_user: dict = Depends(get_current_user)
):
    data = await request.json()

    execution_mode = (data.get("execution_mode") or "").lower()
    if execution_mode not in ["fixed", "custom"]:
        raise HTTPException(400, "Ongeldige execution_mode")

    if not data.get("base_amount"):
        raise HTTPException(400, "base_amount is verplicht")

    if execution_mode == "custom" and not data.get("decision_curve"):
        raise HTTPException(400, "decision_curve verplicht")

    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT s.*, st.symbol, st.timeframe, st.setup_type
                FROM strategies s
                JOIN setups st ON st.id = s.setup_id
                WHERE s.id=%s AND s.user_id=%s
            """, (strategy_id, current_user["id"]))
            existing = cur.fetchone()

            if not existing:
                raise HTTPException(404, "Niet gevonden")

            setup_type = (existing["setup_type"] or "").lower()

            if setup_type == "trade":
                if data.get("entry") is None or data.get("stop_loss") is None:
                    raise HTTPException(400, "entry en stop_loss verplicht")
                if not data.get("targets"):
                    raise HTTPException(400, "targets verplicht")

            cur.execute("""
                UPDATE strategies
                SET
                    name=%s,
                    setup_type=%s,
                    execution_mode=%s,
                    base_amount=%s,
                    decision_curve=%s,
                    decision_curve_id=%s,
                    entry=%s,
                    targets=%s,
                    stop_loss=%s,
                    explanation=%s,
                    risk_profile=%s,
                    data=%s
                WHERE id=%s AND user_id=%s
            """, (
                data.get("name"),
                setup_type,
                execution_mode,
                data.get("base_amount"),
                json.dumps(data.get("decision_curve")) if data.get("decision_curve") else None,
                data.get("decision_curve_id"),
                str(data.get("entry")) if data.get("entry") is not None else None,
                data.get("targets"),
                str(data.get("stop_loss")) if data.get("stop_loss") is not None else None,
                data.get("explanation"),
                data.get("risk_profile"),
                json.dumps(data),
                strategy_id,
                current_user["id"]
            ))

            conn.commit()

        return {"message": "✅ Strategie bijgewerkt"}

    finally:
        conn.close()

# ==========================================================
# 5️⃣ DELETE STRATEGY
# ==========================================================
@router.delete("/strategies/{strategy_id}")
async def delete_strategy(
    strategy_id: int,
    current_user: dict = Depends(get_current_user)
):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM strategies WHERE id=%s AND user_id=%s",
                (strategy_id, current_user["id"])
            )
            if cur.rowcount == 0:
                raise HTTPException(404, "Strategie niet gevonden")
            conn.commit()
        return {"message": "🗑 Verwijderd"}

    finally:
        conn.close()


# ==========================================================
# 6️⃣ AI STRATEGY ANALYSE
# ==========================================================
@router.post("/strategies/analyze/{strategy_id}")
async def analyze_strategy(
    strategy_id: int,
    current_user: dict = Depends(get_current_user)
):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT data FROM strategies WHERE id=%s AND user_id=%s",
                (strategy_id, current_user["id"])
            )
            row = cur.fetchone()

        if not row:
            raise HTTPException(404, "Strategie niet gevonden")

        strategy_data = row[0]

    finally:
        conn.close()

    from backend.ai_agents.strategy_ai_agent import analyze_and_store_strategy
    analyze_and_store_strategy(
        strategy_id=strategy_id,
        strategies=[strategy_data]
    )

    return {"message": "🧠 Strategy AI analyse uitgevoerd"}


# ==========================================================
# 7️⃣ GET STRATEGY BY SETUP
# ==========================================================
@router.get("/strategies/by_setup/{setup_id}")
async def get_strategy_by_setup(
    setup_id: int,
    current_user: dict = Depends(get_current_user)
):
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT *
                FROM strategies
                WHERE setup_id=%s AND user_id=%s
                ORDER BY created_at DESC
                LIMIT 1
            """, (setup_id, current_user["id"]))
            row = cur.fetchone()

        if not row:
            return {"exists": False}

        return {"exists": True, "strategy": format_strategy_row(row)}

    finally:
        conn.close()


# ==========================================================
# 8️⃣ LAST STRATEGY
# ==========================================================
@router.get("/strategies/last")
async def get_last_strategy(current_user: dict = Depends(get_current_user)):
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT *
                FROM strategies
                WHERE user_id=%s
                ORDER BY created_at DESC
                LIMIT 1
            """, (current_user["id"],))
            row = cur.fetchone()

        return format_strategy_row(row) if row else None

    finally:
        conn.close()


# ==========================================================
# 9️⃣ FAVORITE TOGGLE
# ==========================================================
@router.patch("/strategies/{strategy_id}/favorite")
async def toggle_favorite(
    strategy_id: int,
    current_user: dict = Depends(get_current_user)
):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT data FROM strategies WHERE id=%s AND user_id=%s",
                (strategy_id, current_user["id"])
            )
            row = cur.fetchone()
            if not row:
                raise HTTPException(404, "Niet gevonden")

            data = row[0] or {}
            data["favorite"] = not data.get("favorite", False)

            cur.execute(
                "UPDATE strategies SET data=%s WHERE id=%s",
                (json.dumps(data), strategy_id)
            )
            conn.commit()

        return {"favorite": data["favorite"]}

    finally:
        conn.close()


# ==========================================================
# 🔟 EXPORT CSV
# ==========================================================
@router.get("/strategies/export")
async def export_strategies(current_user: dict = Depends(get_current_user)):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, data, created_at
                FROM strategies
                WHERE user_id=%s
                ORDER BY created_at DESC
            """, (current_user["id"],))
            rows = cur.fetchall()

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["ID", "Symbol", "Entry", "Stop Loss", "Created"])

        for id_, s, created in rows:
            writer.writerow([
                id_,
                s.get("symbol"),
                s.get("entry"),
                s.get("stop_loss"),
                created.strftime("%Y-%m-%d %H:%M")
            ])

        output.seek(0)
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=strategies.csv"}
        )

    finally:
        conn.close()

# ==========================================================
# 11 Get Curvers 
# ==========================================================
@router.get("/curves/execution")
async def get_execution_curves(
    current_user: dict = Depends(get_current_user)
):
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT id, name, curve
                FROM indicator_curves
                WHERE user_id=%s
                  AND domain='execution'
                  AND is_active=true
                ORDER BY created_at DESC
            """, (current_user["id"],))

            rows = cur.fetchall()

        return rows

    finally:
        conn.close()


# ==========================================================
# 12 ACTIVE STRATEGY FOR TODAY
# ==========================================================
@router.get("/strategies/active-today")
async def get_active_strategy_today(
    current_user: dict = Depends(get_current_user)
):
    user_id = current_user["id"]
    now = datetime.utcnow()
    weekday = now.strftime("%A").lower()
    month_day = now.day

    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    s.*,
                    st.dca_frequency,
                    st.dca_day,
                    st.dca_month_day
                FROM strategies s
                JOIN setups st ON st.id = s.setup_id
                WHERE s.user_id=%s
                ORDER BY s.created_at DESC
            """, (user_id,))
            rows = cur.fetchall()

        for row in rows:
            setup_type = (row.get("setup_type") or "").lower()

            # 🔥 TRADE = geen timing → skip
            if setup_type == "trade":
                continue

            freq = (row.get("dca_frequency") or "").lower()
            day = (row.get("dca_day") or "").lower()
            md = row.get("dca_month_day")

            if freq == "daily":
                return {"active": True, "strategy": format_strategy_row(row)}

            if freq == "weekly" and day == weekday:
                return {"active": True, "strategy": format_strategy_row(row)}

            if freq == "monthly" and md == month_day:
                return {"active": True, "strategy": format_strategy_row(row)}

        return {"active": False}

    finally:
        conn.close()
