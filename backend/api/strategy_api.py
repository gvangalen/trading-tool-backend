print("✅ strategy_api.py geladen!")

from fastapi import APIRouter, HTTPException, Request, Depends
from fastapi.responses import StreamingResponse
from psycopg2.extras import RealDictCursor
import json, csv, io, logging
from datetime import datetime

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
    data = row.get("data") or {}

    return {
        "id": row.get("id"),
        "setup_id": row.get("setup_id"),
        "strategy_type": row.get("strategy_type"),

        # Core
        "symbol": data.get("symbol"),
        "timeframe": data.get("timeframe"),
        "amount": data.get("amount"),
        "frequency": data.get("frequency"),

        # Trading velden
        "entry": row.get("entry") or data.get("entry"),
        "target": row.get("target") or data.get("target"),
        "stop_loss": row.get("stop_loss") or data.get("stop_loss"),

        # Uitleg
        "explanation": row.get("explanation") or data.get("explanation"),
        "ai_explanation": data.get("ai_explanation"),

        # Meta
        "risk_profile": row.get("risk_profile") or data.get("risk_profile"),
        "tags": data.get("tags", []),
        "favorite": data.get("favorite", False),

        # Timestamps
        "created_at": (
            row.get("created_at").isoformat()
            if row.get("created_at") else None
        ),
    }


# ==========================================================
# 1. CREATE STRATEGY
# ==========================================================
@router.post("/strategies")
async def save_strategy(request: Request, current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]
    data = await request.json()

    strategy_type = (data.get("strategy_type") or "").lower()
    if strategy_type not in ["manual", "trading", "dca"]:
        raise HTTPException(400, "Ongeldig strategy_type")

    required = ["setup_id"]
    if strategy_type == "dca":
        required += ["amount", "frequency"]
    else:
        required += ["entry", "stop_loss"]

    for field in required:
        if data.get(field) in (None, "", []):
            raise HTTPException(400, f"Veld '{field}' is verplicht")

    def safe_str(v):
        return str(v) if v not in (None, "") else None

    def safe_first(lst):
        if isinstance(lst, list) and lst:
            return str(lst[0])
        return None

    entry = safe_str(data.get("entry"))
    stop_loss = safe_str(data.get("stop_loss"))
    target = safe_first(data.get("targets"))

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM setups WHERE id=%s AND user_id=%s",
                (data["setup_id"], user_id)
            )
            if not cur.fetchone():
                raise HTTPException(403, "Setup niet van gebruiker")

            cur.execute("""
                SELECT id FROM strategies
                WHERE setup_id=%s AND strategy_type=%s AND user_id=%s
            """, (data["setup_id"], strategy_type, user_id))
            if cur.fetchone():
                raise HTTPException(409, "Strategie bestaat al")

            cur.execute("""
                INSERT INTO strategies (
                    setup_id, entry, target, stop_loss,
                    explanation, risk_profile,
                    strategy_type, data, created_at, user_id
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s::jsonb,NOW(),%s)
                RETURNING id
            """, (
                data["setup_id"],
                entry,
                target,
                stop_loss,
                safe_str(data.get("explanation")),
                safe_str(data.get("risk_profile")),
                strategy_type,
                json.dumps(data),
                user_id
            ))

            strategy_id = cur.fetchone()[0]
            conn.commit()

        mark_step_completed(conn, user_id, "strategy")
        return {"id": strategy_id, "message": "✅ Strategie succesvol opgeslagen"}

    finally:
        conn.close()


# ==========================================================
# 2. QUERY STRATEGIES (🔥 FIX VOOR CARDS)
# ==========================================================
@router.post("/strategies/query")
async def query_strategies(request: Request, current_user: dict = Depends(get_current_user)):
    filters = await request.json()
    user_id = current_user["id"]

    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            q = """
                SELECT
                    id, setup_id, strategy_type,
                    entry, target, stop_loss,
                    explanation, risk_profile,
                    data, created_at
                FROM strategies
                WHERE user_id=%s
            """
            p = [user_id]

            if filters.get("symbol"):
                q += " AND data->>'symbol'=%s"
                p.append(filters["symbol"])

            if filters.get("timeframe"):
                q += " AND data->>'timeframe'=%s"
                p.append(filters["timeframe"])

            q += " ORDER BY created_at DESC"

            cur.execute(q, tuple(p))
            rows = cur.fetchall()

        return [format_strategy_row(r) for r in rows]

    finally:
        conn.close()


# ==========================================================
# 3. GENERATE STRATEGY
# ==========================================================
@router.post("/strategies/generate/{setup_id}")
async def generate_strategy_for_setup(setup_id: int, current_user: dict = Depends(get_current_user)):
    from backend.celery_task.strategy_task import generate_for_setup
    task = generate_for_setup.delay(user_id=current_user["id"], setup_id=setup_id)
    return {"task_id": task.id}


# ==========================================================
# 4. UPDATE STRATEGY
# ==========================================================
@router.put("/strategies/{strategy_id}")
async def update_strategy(strategy_id: int, request: Request, current_user: dict = Depends(get_current_user)):
    data = await request.json()

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE strategies SET
                    data=%s,
                    explanation=%s,
                    risk_profile=%s
                WHERE id=%s AND user_id=%s
            """, (
                json.dumps(data),
                data.get("explanation"),
                data.get("risk_profile"),
                strategy_id,
                current_user["id"]
            ))
            if cur.rowcount == 0:
                raise HTTPException(404, "Strategie niet gevonden")
            conn.commit()

        return {"message": "✅ Strategie bijgewerkt"}

    finally:
        conn.close()


# ==========================================================
# 5. DELETE STRATEGY
# ==========================================================
@router.delete("/strategies/{strategy_id}")
async def delete_strategy(strategy_id: int, current_user: dict = Depends(get_current_user)):
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
# 6. AI STRATEGY ANALYSE
# ==========================================================
@router.post("/strategies/analyze/{strategy_id}")
async def analyze_strategy(
    strategy_id: int,
    current_user: dict = Depends(get_current_user)
):
    user_id = current_user["id"]

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT data
                FROM strategies
                WHERE id = %s AND user_id = %s
            """, (strategy_id, user_id))
            row = cur.fetchone()

        if not row:
            raise HTTPException(404, "Strategie niet gevonden")

        strategy_data = row[0]

    finally:
        conn.close()

    from backend.ai_agents.strategy_ai_agent import analyze_and_store_strategy

    # 🔑 ORCHESTRATOR AANROEP
    analyze_and_store_strategy(
        strategy_id=strategy_id,
        strategies=[strategy_data],  # verwacht LIST
    )

    return {
        "message": "🧠 Strategy AI analyse uitgevoerd en opgeslagen"
    }

# ==========================================================
# 7. GET STRATEGY BY SETUP
# ==========================================================
@router.get("/strategies/by_setup/{setup_id}")
async def get_strategy_by_setup(setup_id: int, current_user: dict = Depends(get_current_user)):
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    id, setup_id, strategy_type,
                    entry, target, stop_loss,
                    explanation, risk_profile,
                    data, created_at
                FROM strategies
                WHERE setup_id=%s AND user_id=%s
                ORDER BY created_at DESC LIMIT 1
            """, (setup_id, current_user["id"]))
            row = cur.fetchone()

        if not row:
            return {"exists": False}

        return {"exists": True, "strategy": format_strategy_row(row)}

    finally:
        conn.close()


# ==========================================================
# 8. LAST STRATEGY
# ==========================================================
@router.get("/strategies/last")
async def get_last_strategy(current_user: dict = Depends(get_current_user)):
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    id, setup_id, strategy_type,
                    entry, target, stop_loss,
                    explanation, risk_profile,
                    data, created_at
                FROM strategies
                WHERE user_id=%s
                ORDER BY created_at DESC LIMIT 1
            """, (current_user["id"],))
            row = cur.fetchone()

        return format_strategy_row(row) if row else None

    finally:
        conn.close()


# ==========================================================
# 9. FAVORITE TOGGLE
# ==========================================================
@router.patch("/strategies/{strategy_id}/favorite")
async def toggle_favorite(strategy_id: int, current_user: dict = Depends(get_current_user)):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT data FROM strategies WHERE id=%s AND user_id=%s
            """, (strategy_id, current_user["id"]))
            row = cur.fetchone()
            if not row:
                raise HTTPException(404, "Niet gevonden")

            data = row[0]
            data["favorite"] = not data.get("favorite", False)

            cur.execute("""
                UPDATE strategies SET data=%s WHERE id=%s
            """, (json.dumps(data), strategy_id))
            conn.commit()

        return {"favorite": data["favorite"]}

    finally:
        conn.close()


# ==========================================================
# 10. EXPORT CSV
# ==========================================================
@router.get("/strategies/export")
async def export_strategies(current_user: dict = Depends(get_current_user)):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, data, created_at FROM strategies
                WHERE user_id=%s ORDER BY created_at DESC
            """, (current_user["id"],))
            rows = cur.fetchall()

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["ID", "Symbol", "Setup", "Entry", "SL", "Created"])

        for id_, s, created in rows:
            writer.writerow([
                id_,
                s.get("symbol"),
                s.get("setup_name"),
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
# 11. ACTIEVE STRATEGIE VANDAAG
# ==========================================================
@router.get("/strategy/active-today")
async def get_active_strategy_today(current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]
    conn = get_db_connection()

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    s.name, s.symbol, s.timeframe,
                    a.entry, a.targets, a.stop_loss,
                    a.adjustment_reason, a.confidence_score
                FROM active_strategy_snapshot a
                JOIN setups s ON s.id = a.setup_id
                WHERE a.user_id=%s AND a.snapshot_date=CURRENT_DATE
                LIMIT 1
            """, (user_id,))
            row = cur.fetchone()

        if not row:
            return None

        return {
            "setup_name": row[0],
            "symbol": row[1],
            "timeframe": row[2],
            "entry": row[3],
            "targets": row[4],
            "stop_loss": row[5],
            "adjustment_reason": row[6],
            "confidence_score": row[7],
        }

    finally:
        conn.close()
