print("✅ strategy_api.py geladen!")

from fastapi import APIRouter, HTTPException, Request, Query, Depends
from fastapi.responses import StreamingResponse

# Init router (zonder prefix!)
router = APIRouter()

# Database & Auth
from backend.utils.db import get_db_connection
from backend.utils.auth_utils import get_current_user

# Celery
from backend.celery_task.strategy_task import generate_for_setup as generate_strategy_task

import json
import csv
import io
import logging
# =====================================================================
# 🌱 1. CREATE STRATEGY (user-specific)
# =====================================================================
@router.post("/strategies")
async def save_strategy(
    request: Request,
    current_user: dict = Depends(get_current_user)
):
    user_id = current_user["id"]

    try:
        data = await request.json()

        # ---------------------------------------------------
        # Strategy type validation
        # ---------------------------------------------------
        strategy_type = data.get("strategy_type", "").lower()
        if strategy_type not in ["manual", "trading", "dca"]:
            raise HTTPException(
                status_code=400,
                detail=f"Onbekend strategy_type '{strategy_type}'. Gebruik 'manual', 'trading' of 'dca'."
            )

        # ---------------------------------------------------
        # Verplichte velden
        # ---------------------------------------------------
        required_base = ["setup_id"]
        required_map = {
            "manual": ["entry", "targets", "stop_loss"],
            "trading": ["entry", "targets", "stop_loss"],
            "dca": ["amount", "frequency"],
        }
        required_fields = required_base + required_map[strategy_type]

        for f in required_fields:
            if f not in data or data.get(f) in [None, "", []]:
                raise HTTPException(status_code=400, detail=f"Veld '{f}' is verplicht.")

        setup_id = int(data["setup_id"])

        # ---------------------------------------------------
        # DB
        # ---------------------------------------------------
        conn = get_db_connection()
        if not conn:
            raise HTTPException(status_code=500, detail="Geen databaseverbinding")

        with conn.cursor() as cur:

            # Only allow strategy for setups belonging to this user
            cur.execute("""
                SELECT id FROM setups 
                WHERE id = %s AND user_id = %s
            """, (setup_id, user_id))
            if not cur.fetchone():
                raise HTTPException(403, "Setup behoort niet tot deze gebruiker.")

            # Check existing strategy for this setup & type
            cur.execute("""
                SELECT id FROM strategies 
                WHERE setup_id = %s AND strategy_type = %s AND user_id = %s
            """, (setup_id, strategy_type, user_id))
            if cur.fetchone():
                raise HTTPException(409, "Strategie bestaat al voor deze setup.")

            # ------------------------------
            # Autotags
            # ------------------------------
            keywords = ["breakout", "scalp", "swing", "reversal", "dca"]
            combined_text = (
                (data.get("setup_name", "") + " " +
                 data.get("explanation", "") +
                 data.get("ai_explanation", ""))
                .lower()
            )
            found_tags = [k for k in keywords if k in combined_text]
            data["tags"] = list(set(data.get("tags", []) + found_tags))

            # Defaults
            data.setdefault("favorite", False)
            data.setdefault("origin", strategy_type.upper())
            data.setdefault("ai_reason", "")
            data["strategy_type"] = strategy_type
            data["user_id"] = user_id

            # Targets fix
            if "targets" in data and not isinstance(data["targets"], list):
                data["targets"] = [data["targets"]]

            explanation_val = data.get("ai_explanation") or data.get("explanation", "")

            entry_val = "" if strategy_type == "dca" else str(data.get("entry", ""))
            target_val = "" if strategy_type == "dca" else str(data.get("targets", [""])[0])
            stop_loss_val = "" if strategy_type == "dca" else str(data.get("stop_loss", ""))
            risk_profile_val = data.get("risk_profile", None)

            # Insert
            cur.execute("""
                INSERT INTO strategies 
                (setup_id, entry, target, stop_loss, explanation, risk_profile, 
                 strategy_type, data, created_at, user_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, NOW(), %s)
                RETURNING id
            """,
            (
                setup_id,
                entry_val,
                target_val,
                stop_loss_val,
                explanation_val,
                risk_profile_val,
                strategy_type,
                json.dumps(data),
                user_id
            ))

            strategy_id = cur.fetchone()[0]
            conn.commit()

        return {"message": "✅ Strategie opgeslagen", "id": strategy_id}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[save_strategy] ❌ {e}")
        raise HTTPException(500, "Interne fout bij opslaan strategie.")

# =====================================================================
# 🧪 2. QUERY STRATEGIES (user-specific)
# =====================================================================
@router.post("/strategies/query")
async def query_strategies(
    request: Request,
    current_user: dict = Depends(get_current_user)
):
    user_id = current_user["id"]
    conn = get_db_connection()
    if not conn:
        raise HTTPException(500, "Geen databaseverbinding")

    filters = await request.json()
    symbol = filters.get("symbol")
    timeframe = filters.get("timeframe")
    tag = filters.get("tag")

    try:
        with conn.cursor() as cur:
            query = "SELECT id, data FROM strategies WHERE user_id = %s"
            params = [user_id]

            if symbol:
                query += " AND data->>'symbol' = %s"
                params.append(symbol)

            if timeframe:
                query += " AND data->>'timeframe' = %s"
                params.append(timeframe)

            if tag:
                query += " AND %s = ANY(data->'tags')"
                params.append(tag)

            query += " ORDER BY created_at DESC"
            cur.execute(query, tuple(params))

            rows = cur.fetchall()

        result = []
        for id_, s in rows:
            s["id"] = id_
            result.append(s)

        return result

    finally:
        conn.close()

# =====================================================================
# 🤖 3. GENERATE STRATEGY VIA CELERY (user-specific)
# =====================================================================
@router.post("/strategies/generate/{setup_id}")
async def generate_strategy_for_setup(
    setup_id: int,
    request: Request,
    current_user: dict = Depends(get_current_user)
):
    user_id = current_user["id"]
    body = await request.json()
    overwrite = body.get("overwrite", True)

    conn = get_db_connection()
    if not conn:
        raise HTTPException(500, "Geen databaseverbinding")

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, name, symbol, timeframe 
                FROM setups 
                WHERE id = %s AND user_id = %s
            """, (setup_id, user_id))
            row = cur.fetchone()

        if not row:
            raise HTTPException(404, "Setup niet gevonden of niet van gebruiker")

        # Start Celery job per user
        task = generate_strategy_task.delay(
            setup_id=setup_id,
            overwrite=overwrite,
            user_id=user_id
        )

        return {"message": "⏳ Strategie wordt gegenereerd", "task_id": task.id}

    finally:
        conn.close()

# =====================================================================
# ✏ 4. UPDATE STRATEGY (user-specific)
# =====================================================================
@router.put("/strategies/{strategy_id}")
async def update_strategy(
    strategy_id: int,
    request: Request,
    current_user: dict = Depends(get_current_user)
):
    user_id = current_user["id"]
    data = await request.json()

    conn = get_db_connection()
    if not conn:
        raise HTTPException(500, "Geen databaseverbinding")

    try:
        with conn.cursor() as cur:

            # Validate ownership
            cur.execute("""
                SELECT data FROM strategies 
                WHERE id = %s AND user_id = %s
            """, (strategy_id, user_id))
            row = cur.fetchone()

            if not row:
                raise HTTPException(404, "Strategie niet gevonden of niet jouw strategie")

            strategy_data = row[0]

            if "ai_explanation" in data:
                strategy_data["explanation"] = data["ai_explanation"]

            for k, v in data.items():
                strategy_data[k] = v

            cur.execute("""
                UPDATE strategies 
                SET data = %s 
                WHERE id = %s
            """, (json.dumps(strategy_data), strategy_id))

            conn.commit()

        return {"message": "✅ Strategie bijgewerkt"}

    finally:
        conn.close()

# =====================================================================
# 🗑 5. DELETE (user-specific)
# =====================================================================
@router.delete("/strategies/{strategy_id}")
async def delete_strategy(
    strategy_id: int,
    current_user: dict = Depends(get_current_user)
):
    user_id = current_user["id"]
    conn = get_db_connection()
    if not conn:
        raise HTTPException(500, "Geen databaseverbinding")

    try:
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM strategies
                WHERE id = %s AND user_id = %s
            """, (strategy_id, user_id))

            if cur.rowcount == 0:
                raise HTTPException(404, "Strategie niet gevonden")

        conn.commit()
        return {"message": "🗑 Strategie verwijderd"}

    finally:
        conn.close()

# =====================================================================
# ⭐ FAVORITE TOGGLE (user-specific)
# =====================================================================
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
                raise HTTPException(404, "Niet gevonden")

            data = row[0]
            data["favorite"] = not data.get("favorite", False)

            cur.execute("""
                UPDATE strategies 
                SET data = %s 
                WHERE id = %s
            """, (json.dumps(data), strategy_id))
            conn.commit()

        return {"favorite": data["favorite"]}

    finally:
        conn.close()

# =====================================================================
# 🔎 FILTER (user-specific)
# =====================================================================
@router.post("/strategies/filter")
async def filter_strategies(
    request: Request,
    current_user: dict = Depends(get_current_user)
):
    user_id = current_user["id"]
    filters = await request.json()
    symbol = filters.get("symbol")
    timeframe = filters.get("timeframe")
    tag = filters.get("tag")
    min_score = filters.get("min_score")

    conn = get_db_connection()
    try:
        query = "SELECT id, data FROM strategies WHERE user_id = %s"
        params = [user_id]

        if symbol:
            query += " AND data->>'symbol' = %s"
            params.append(symbol)

        if timeframe:
            query += " AND data->>'timeframe' = %s"
            params.append(timeframe)

        if tag:
            query += " AND %s = ANY(data->'tags')"
            params.append(tag)

        if min_score is not None:
            query += " AND (data->>'score')::float >= %s"
            params.append(min_score)

        with conn.cursor() as cur:
            cur.execute(query, tuple(params))
            rows = cur.fetchall()

        out = []
        for (id_, s) in rows:
            s["id"] = id_
            out.append(s)

        return out

    finally:
        conn.close()

# =====================================================================
# 📤 EXPORT (user-specific)
# =====================================================================
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

        for row in rows:
            s = row[1]
            writer.writerow([
                row[0],
                s.get("symbol"),
                s.get("setup_name"),
                s.get("entry"),
                s.get("stop_loss"),
                s.get("origin"),
                row[2].strftime("%Y-%m-%d %H:%M")
            ])

        output.seek(0)

        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=strategies.csv"}
        )

    finally:
        conn.close()

# =====================================================================
# 🔥 GET STRATEGY BY SETUP + TYPE (user-specific)
# =====================================================================
@router.get("/strategies/by_setup/{setup_id}")
async def get_strategy_by_setup(
    setup_id: int,
    strategy_type: str = Query(None),
    current_user: dict = Depends(get_current_user)
):
    user_id = current_user["id"]
    conn = get_db_connection()

    try:
        with conn.cursor() as cur:
            query = """
                SELECT id, data
                FROM strategies
                WHERE (data->>'setup_id')::int = %s
                  AND user_id = %s
            """
            params = [setup_id, user_id]

            if strategy_type:
                query += " AND LOWER(data->>'strategy_type') = LOWER(%s)"
                params.append(strategy_type)

            query += " ORDER BY created_at DESC LIMIT 1"
            cur.execute(query, tuple(params))

            row = cur.fetchone()

        if not row:
            return {"exists": False}

        id_, data = row
        data["id"] = id_

        return {"exists": True, "strategy": data}

    finally:
        conn.close()

# =====================================================================
# 🔚 LAATSTE STRATEGIE (user-specific)
# =====================================================================
@router.get("/strategies/last")
async def get_last_strategy(
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
                LIMIT 1
            """, (user_id,))
            row = cur.fetchone()

        if not row:
            return {"message": "Geen strategieën gevonden"}

        id_, data, created_at = row
        data["id"] = id_
        data["created_at"] = created_at.isoformat()

        return data

    finally:
        conn.close()
