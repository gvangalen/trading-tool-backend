print("✅ strategy_api.py geladen!")

from fastapi import APIRouter, HTTPException, Request, Query, Depends
from fastapi.responses import StreamingResponse

router = APIRouter()

# ==========================================================
# DB & AUTH
# ==========================================================
from backend.utils.db import get_db_connection
from backend.utils.auth_utils import get_current_user

# ⭐ Onboarding – ALLEEN bij POST /strategies
from backend.api.onboarding_api import mark_step_completed

import json
import csv
import io
import logging

logger = logging.getLogger(__name__)

# ==========================================================
# 1. CREATE STRATEGY  (ENIGE onboarding trigger)
# ==========================================================
@router.post("/strategies")
async def save_strategy(
    request: Request,
    current_user: dict = Depends(get_current_user)
):
    user_id = current_user["id"]

    try:
        data = await request.json()
        strategy_type = data.get("strategy_type", "").lower()

        if strategy_type not in ["manual", "trading", "dca"]:
            raise HTTPException(
                400,
                f"Onbekend strategy_type '{strategy_type}'. Gebruik 'manual', 'trading', 'dca'."
            )

        required_map = {
            "manual": ["entry", "targets", "stop_loss"],
            "trading": ["entry", "targets", "stop_loss"],
            "dca": ["amount", "frequency"],
        }

        required = ["setup_id"] + required_map[strategy_type]
        for f in required:
            if f not in data or data.get(f) in [None, "", []]:
                raise HTTPException(400, f"Veld '{f}' is verplicht.")

        setup_id = int(data["setup_id"])

        conn = get_db_connection()
        if not conn:
            raise HTTPException(500, "Geen databaseverbinding")

        with conn.cursor() as cur:

            # Setup owner check
            cur.execute("""
                SELECT id FROM setups
                WHERE id = %s AND user_id = %s
            """, (setup_id, user_id))
            if not cur.fetchone():
                raise HTTPException(403, "Setup behoort niet tot deze gebruiker.")

            # Dubbele strategie check
            cur.execute("""
                SELECT id FROM strategies
                WHERE setup_id = %s AND strategy_type = %s AND user_id = %s
            """, (setup_id, strategy_type, user_id))
            if cur.fetchone():
                raise HTTPException(409, "Strategie bestaat al voor deze setup.")

            # Auto-tags
            keywords = ["breakout", "scalp", "swing", "reversal", "dca"]
            combined = (
                (data.get("setup_name", "") + " " +
                 data.get("explanation", "") +
                 data.get("ai_explanation", ""))
                .lower()
            )
            found_tags = [k for k in keywords if k in combined]
            data["tags"] = list(set(data.get("tags", []) + found_tags))

            # Defaults
            data.setdefault("favorite", False)
            data.setdefault("origin", strategy_type.upper())
            data.setdefault("ai_reason", "")
            data["user_id"] = user_id

            explanation = data.get("ai_explanation") or data.get("explanation", "")

            entry_val  = "" if strategy_type == "dca" else str(data.get("entry", ""))
            target_val = "" if strategy_type == "dca" else str(data.get("targets", [""])[0])
            stop_val   = "" if strategy_type == "dca" else str(data.get("stop_loss", ""))

            cur.execute("""
                INSERT INTO strategies
                (setup_id, entry, target, stop_loss, explanation,
                 risk_profile, strategy_type, data, created_at, user_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, NOW(), %s)
                RETURNING id
            """, (
                setup_id,
                entry_val,
                target_val,
                stop_val,
                explanation,
                data.get("risk_profile"),
                strategy_type,
                json.dumps(data),
                user_id
            ))

            strategy_id = cur.fetchone()[0]
            conn.commit()

        # ⭐ onboarding HIER en nergens anders
        mark_step_completed(conn, user_id, "strategy")

        return {"message": "✅ Strategie opgeslagen", "id": strategy_id}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[save_strategy] ❌ {e}")
        raise HTTPException(500, "Interne fout bij opslaan strategie.")


# ==========================================================
# 2. QUERY STRATEGIES
# ==========================================================
@router.post("/strategies/query")
async def query_strategies(
    request: Request,
    current_user: dict = Depends(get_current_user)
):
    user_id = current_user["id"]
    filters = await request.json()

    conn = get_db_connection()
    try:
        query = "SELECT id, data FROM strategies WHERE user_id = %s"
        params = [user_id]

        if filters.get("symbol"):
            query += " AND data->>'symbol' = %s"
            params.append(filters["symbol"])

        if filters.get("timeframe"):
            query += " AND data->>'timeframe' = %s"
            params.append(filters["timeframe"])

        if filters.get("tag"):
            query += " AND %s = ANY(data->'tags')"
            params.append(filters["tag"])

        query += " ORDER BY created_at DESC"

        with conn.cursor() as cur:
            cur.execute(query, tuple(params))
            rows = cur.fetchall()

        return [{**s, "id": id_} for id_, s in rows]

    finally:
        conn.close()


# ==========================================================
# 3. GENERATE STRATEGY VIA CELERY  ✅ FIX HIER
# ==========================================================
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
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id FROM setups
                WHERE id = %s AND user_id = %s
            """, (setup_id, user_id))
            if not cur.fetchone():
                raise HTTPException(404, "Setup niet gevonden of niet van gebruiker")

        # ✅ LAZY IMPORT (DIT WAS DE BUG)
        from backend.celery_task.strategy_task import generate_for_setup

        task = generate_for_setup.delay(
            user_id=user_id,
            setup_id=setup_id,
            overwrite=overwrite
        )

        return {
            "message": "⏳ Strategie wordt gegenereerd",
            "task_id": task.id
        }

    finally:
        conn.close()


# ==========================================================
# 4. UPDATE STRATEGY
# ==========================================================
@router.put("/strategies/{strategy_id}")
async def update_strategy(
    strategy_id: int,
    request: Request,
    current_user: dict = Depends(get_current_user)
):
    user_id = current_user["id"]
    data = await request.json()

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

            strategy_data = row[0]

            if "ai_explanation" in data:
                strategy_data["explanation"] = data["ai_explanation"]

            for k, v in data.items():
                strategy_data[k] = v

            cur.execute("""
                UPDATE strategies SET data = %s
                WHERE id = %s
            """, (json.dumps(strategy_data), strategy_id))
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
    user_id = current_user["id"]

    conn = get_db_connection()
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


# ==========================================================
# 6. FAVORITE TOGGLE
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
# 7. EXPORT CSV
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


# ==========================================================
# 8. GET STRATEGY BY SETUP
# ==========================================================
@router.get("/strategies/by_setup/{setup_id}")
async def get_strategy_by_setup(
    setup_id: int,
    strategy_type: str = Query(None),
    current_user: dict = Depends(get_current_user)
):
    user_id = current_user["id"]

    conn = get_db_connection()
    try:
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

        with conn.cursor() as cur:
            cur.execute(query, tuple(params))
            row = cur.fetchone()

        if not row:
            return {"exists": False}

        id_, data = row
        data["id"] = id_
        return {"exists": True, "strategy": data}

    finally:
        conn.close()


# ==========================================================
# 9. GET LAST STRATEGY
# ==========================================================
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

        id_, data, created = row
        data["id"] = id_
        data["created_at"] = created.isoformat()

        return data

    finally:
        conn.close()
