print("✅ strategy_api.py geladen!")

from fastapi import APIRouter, HTTPException, Request, Query, Depends
from fastapi.responses import StreamingResponse
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
# 1. CREATE STRATEGY
# ==========================================================
@router.post("/strategies")
async def save_strategy(
    request: Request,
    current_user: dict = Depends(get_current_user)
):
    user_id = current_user["id"]
    data = await request.json()
    strategy_type = data.get("strategy_type", "").lower()

    if strategy_type not in ["manual", "trading", "dca"]:
        raise HTTPException(400, "Ongeldig strategy_type")

    required = ["setup_id"]
    if strategy_type == "dca":
        required += ["amount", "frequency"]
    else:
        required += ["entry", "targets", "stop_loss"]

    for f in required:
        if not data.get(f):
            raise HTTPException(400, f"Veld '{f}' is verplicht")

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id FROM setups
                WHERE id = %s AND user_id = %s
            """, (data["setup_id"], user_id))
            if not cur.fetchone():
                raise HTTPException(403, "Setup niet van gebruiker")

            cur.execute("""
                SELECT id FROM strategies
                WHERE setup_id = %s AND strategy_type = %s AND user_id = %s
            """, (data["setup_id"], strategy_type, user_id))
            if cur.fetchone():
                raise HTTPException(409, "Strategie bestaat al")

            cur.execute("""
                INSERT INTO strategies (
                    setup_id, entry, target, stop_loss,
                    explanation, risk_profile, strategy_type,
                    data, created_at, user_id
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s::jsonb,NOW(),%s)
                RETURNING id
            """, (
                data["setup_id"],
                str(data.get("entry","")),
                str(data.get("targets",[""])[0]),
                str(data.get("stop_loss","")),
                data.get("explanation",""),
                data.get("risk_profile"),
                strategy_type,
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
# 2. QUERY STRATEGIES
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
        q = "SELECT id, data FROM strategies WHERE user_id = %s"
        p = [user_id]

        if filters.get("symbol"):
            q += " AND data->>'symbol' = %s"
            p.append(filters["symbol"])

        if filters.get("timeframe"):
            q += " AND data->>'timeframe' = %s"
            p.append(filters["timeframe"])

        q += " ORDER BY created_at DESC"

        with conn.cursor() as cur:
            cur.execute(q, tuple(p))
            rows = cur.fetchall()

        return [{**r[1], "id": r[0]} for r in rows]

    finally:
        conn.close()


# ==========================================================
# 3. GENERATE STRATEGY (1x per setup)
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
# 4. UPDATE STRATEGY
# ==========================================================
@router.put("/strategies/{strategy_id}")
async def update_strategy(
    strategy_id: int,
    request: Request,
    current_user: dict = Depends(get_current_user)
):
    data = await request.json()
    user_id = current_user["id"]

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE strategies SET data = %s
                WHERE id = %s AND user_id = %s
            """, (json.dumps(data), strategy_id, user_id))
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
async def delete_strategy(
    strategy_id: int,
    current_user: dict = Depends(get_current_user)
):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM strategies
                WHERE id = %s AND user_id = %s
            """, (strategy_id, current_user["id"]))
            if cur.rowcount == 0:
                raise HTTPException(404, "Strategie niet gevonden")
            conn.commit()
        return {"message": "🗑 Verwijderd"}

    finally:
        conn.close()


# ==========================================================
# 6. 🧠 AI ANALYSE (WORDT OPGESLAGEN VIA CELERY)
# ==========================================================
@router.post("/strategies/analyze/{strategy_id}")
async def analyze_strategy(
    strategy_id: int,
    current_user: dict = Depends(get_current_user)
):
    user_id = current_user["id"]
    conn = get_db_connection()

    try:
        # 1️⃣ Check of strategie bestaat en van gebruiker is
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id
                FROM strategies
                WHERE id = %s AND user_id = %s
            """, (strategy_id, user_id))
            if not cur.fetchone():
                raise HTTPException(404, "Strategie niet gevonden")

        # 2️⃣ Start AI analyse (GEEN strategie insert)
        from backend.ai_agents.strategy_ai_agent import analyze_strategy_ai

        task = analyze_strategy_ai.delay(user_id=user_id)

        return {
            "message": "🧠 Strategy AI analyse gestart",
            "task_id": task.id
        }

    finally:
        conn.close()

# ==========================================================
# 7. GET STRATEGY BY SETUP
# ==========================================================
@router.get("/strategies/by_setup/{setup_id}")
async def get_strategy_by_setup(
    setup_id: int,
    current_user: dict = Depends(get_current_user)
):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, data FROM strategies
                WHERE setup_id = %s AND user_id = %s
                ORDER BY created_at DESC LIMIT 1
            """, (setup_id, current_user["id"]))
            row = cur.fetchone()

        if not row:
            return {"exists": False}

        return {"exists": True, "strategy": {**row[1], "id": row[0]}}

    finally:
        conn.close()


# ==========================================================
# 8. LAST STRATEGY
# ==========================================================
@router.get("/strategies/last")
async def get_last_strategy(
    current_user: dict = Depends(get_current_user)
):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, data, created_at
                FROM strategies
                WHERE user_id = %s
                ORDER BY created_at DESC LIMIT 1
            """, (current_user["id"],))
            row = cur.fetchone()

        if not row:
            return None

        return {
            **row[1],
            "id": row[0],
            "created_at": row[2].isoformat()
        }

    finally:
        conn.close()

# ==========================================================
# 9. FAVORITE TOGGLE
# ==========================================================
@router.patch("/strategies/{strategy_id}/favorite")
async def toggle_favorite(
    strategy_id: int,
    current_user: dict = Depends(get_current_user)
):
    user_id = current_user["id"]

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT data FROM strategies
                WHERE id = %s AND user_id = %s
            """, (strategy_id, user_id))
            row = cur.fetchone()
            if not row:
                raise HTTPException(404, "Strategie niet gevonden")

            data = row[0]
            data["favorite"] = not data.get("favorite", False)

            cur.execute("""
                UPDATE strategies SET data = %s
                WHERE id = %s
            """, (json.dumps(data), strategy_id))
            conn.commit()

        return {"favorite": data["favorite"]}

    finally:
        conn.close()


# ==========================================================
# 10. EXPORT CSV
# ==========================================================
@router.get("/strategies/export")
async def export_strategies(
    current_user: dict = Depends(get_current_user)
):
    user_id = current_user["id"]

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, data, created_at
                FROM strategies
                WHERE user_id = %s
                ORDER BY created_at DESC
            """, (user_id,))
            rows = cur.fetchall()

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["ID", "Symbol", "Setup", "Entry", "SL", "Origin", "Created"])

        for id_, s, created in rows:
            writer.writerow([
                id_,
                s.get("symbol"),
                s.get("setup_name"),
                s.get("entry"),
                s.get("stop_loss"),
                s.get("origin"),
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
