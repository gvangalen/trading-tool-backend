print("✅ strategy_api.py geladen!")

from fastapi import APIRouter, HTTPException, Request, Query
from fastapi.responses import StreamingResponse
from backend.utils.db import get_db_connection
from backend.celery_task.strategy_task import generate_strategie_voor_setup as generate_strategy_task
import json
import csv
import io
import logging

router = APIRouter()
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


# =====================================================================
# 🌱 1. CREATE STRATEGY
# =====================================================================
@router.post("/strategies")
async def save_strategy(request: Request):
    try:
        data = await request.json()

        # Extract strategy type
        strategy_type = data.get("strategy_type", "manual").lower()

        # Required fields
        required_fields = ["setup_id", "setup_name", "symbol", "timeframe"]

        if strategy_type == "dca":
            required_fields += ["amount", "frequency"]
        elif strategy_type in ["manual", "trading"]:
            required_fields += ["entry", "targets", "stop_loss"]

        # Validate
        for field in required_fields:
            if field not in data or data.get(field) in [None, "", []]:
                raise HTTPException(
                    status_code=400,
                    detail=f"Veld '{field}' is verplicht."
                )

        conn = get_db_connection()
        if not conn:
            raise HTTPException(status_code=500, detail="Geen databaseverbinding")

        with conn.cursor() as cur:

            # EXITS?
            cur.execute(
                "SELECT id FROM strategies WHERE setup_id = %s AND strategy_type = %s",
                (int(data["setup_id"]), strategy_type),
            )
            if cur.fetchone():
                raise HTTPException(status_code=409, detail="Strategie bestaat al voor deze setup")

            # TAGS
            keywords = ["breakout", "scalp", "swing", "reversal", "dca"]
            combined_text = (
                (data.get("setup_name", "") + " " +
                 data.get("explanation", "") +
                 data.get("ai_explanation", ""))
                .lower()
            )
            found_tags = [k for k in keywords if k in combined_text]
            data["tags"] = list(set(data.get("tags", []) + found_tags))

            data.setdefault("favorite", False)
            data.setdefault("origin", strategy_type.upper())
            data.setdefault("ai_reason", "")
            data["strategy_type"] = strategy_type

            # Targets normalized
            if "targets" in data and not isinstance(data["targets"], list):
                data["targets"] = [data["targets"]]

            # AI EXPLANATION FIX
            if "ai_explanation" in data and data["ai_explanation"]:
                explanation_val = data["ai_explanation"]
            else:
                explanation_val = data.get("explanation", "")

            # SQL INSERT
            entry_val = str(data.get("entry", ""))
            target_val = str(data.get("targets", [""])[0])
            stop_loss_val = str(data.get("stop_loss", ""))
            risk_profile_val = data.get("risk_profile", None)

            cur.execute(
                """
                INSERT INTO strategies 
                (setup_id, entry, target, stop_loss, explanation, risk_profile, strategy_type, data, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, NOW())
                RETURNING id
                """,
                (
                    int(data["setup_id"]),
                    entry_val,
                    target_val,
                    stop_loss_val,
                    explanation_val,
                    risk_profile_val,
                    strategy_type,
                    json.dumps(data),
                ),
            )
            strategy_id = cur.fetchone()[0]
            conn.commit()

        return {"message": "✅ Strategie opgeslagen", "id": strategy_id}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[save_strategy] ❌ {e}")
        raise HTTPException(status_code=500, detail="Interne serverfout bij opslaan strategie.")


# =====================================================================
# 🧪 2. QUERY STRATEGIES
# =====================================================================
@router.post("/strategies/query")
async def query_strategies(request: Request):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Geen databaseverbinding")

    filters = await request.json()
    symbol = filters.get("symbol")
    timeframe = filters.get("timeframe")
    tag = filters.get("tag")

    try:
        with conn.cursor() as cur:
            query = "SELECT id, data FROM strategies WHERE TRUE"
            params = []

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
        for id_, data in rows:
            data["id"] = id_
            result.append(data)

        return result

    finally:
        conn.close()


# =====================================================================
# 🤖 3. GENERATE STRATEGY VIA CELERY
# =====================================================================
@router.post("/strategies/generate/{setup_id}")
async def generate_strategy_for_setup(setup_id: int, request: Request):
    body = await request.json()
    overwrite = body.get("overwrite", True)

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Geen databaseverbinding")

    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, name, symbol, timeframe FROM setups WHERE id = %s",
                (setup_id,)
            )
            row = cur.fetchone()

        if not row:
            raise HTTPException(status_code=404, detail="Setup niet gevonden")

        task = generate_strategy_task.delay(setup_id=setup_id, overwrite=overwrite)

        return {"message": "⏳ Strategie wordt gegenereerd", "task_id": task.id}

    finally:
        conn.close()


# =====================================================================
# ✏ 4. UPDATE STRATEGY
# =====================================================================
@router.put("/strategies/{strategy_id}")
async def update_strategy(strategy_id: int, request: Request):

    data = await request.json()

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Geen databaseverbinding")

    try:
        with conn.cursor() as cur:

            cur.execute("SELECT data FROM strategies WHERE id = %s", (strategy_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Strategie niet gevonden")

            strategy_data = row[0]

            # AI EXPLANATION FIX
            if "ai_explanation" in data:
                strategy_data["explanation"] = data["ai_explanation"]

            for k, v in data.items():
                strategy_data[k] = v

            cur.execute(
                "UPDATE strategies SET data = %s WHERE id = %s",
                (json.dumps(strategy_data), strategy_id)
            )
            conn.commit()

        return {"message": "✅ Strategie bijgewerkt"}

    finally:
        conn.close()


# =====================================================================
# 🗑 5. DELETE
# =====================================================================
@router.delete("/strategies/{strategy_id}")
async def delete_strategy(strategy_id: int):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Geen databaseverbinding")

    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM strategies WHERE id = %s", (strategy_id,))
            conn.commit()

        return {"message": "🗑 Strategie verwijderd"}

    finally:
        conn.close()


# =====================================================================
# ⭐ FAVORITE TOGGLE
# =====================================================================
@router.patch("/strategies/{strategy_id}/favorite")
async def toggle_favorite(strategy_id: int):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Geen databaseverbinding")

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT data FROM strategies WHERE id = %s", (strategy_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Niet gevonden")

            data = row[0]
            data["favorite"] = not data.get("favorite", False)

            cur.execute(
                "UPDATE strategies SET data = %s WHERE id = %s",
                (json.dumps(data), strategy_id)
            )
            conn.commit()

        return {"favorite": data["favorite"]}

    finally:
        conn.close()


# =====================================================================
# 🔎 FILTER
# =====================================================================
@router.post("/strategies/filter")
async def filter_strategies(request: Request):
    filters = await request.json()
    symbol = filters.get("symbol")
    timeframe = filters.get("timeframe")
    tag = filters.get("tag")
    min_score = filters.get("min_score")

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Geen databaseverbinding")

    try:
        query = "SELECT id, data FROM strategies WHERE TRUE"
        params = []

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
        for id_, s in rows:
            s["id"] = id_
            out.append(s)

        return out

    finally:
        conn.close()


# =====================================================================
# 📤 EXPORT
# =====================================================================
@router.get("/strategies/export")
async def export_strategies():
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Geen databaseverbinding")

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id, data, created_at FROM strategies")
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
# 🧮 SCORE MATRIX
# =====================================================================
@router.get("/strategies/score_matrix")
async def score_matrix():

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Geen databaseverbinding")

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT data FROM strategies")
            rows = cur.fetchall()

        matrix = {}

        for (s,) in rows:
            symbol = s.get("symbol")
            tf = s.get("timeframe")
            score = float(s.get("score", 0))
            if not symbol or not tf:
                continue

            matrix.setdefault(symbol, {})
            if tf in matrix[symbol]:
                matrix[symbol][tf] = round((matrix[symbol][tf] + score) / 2, 2)
            else:
                matrix[symbol][tf] = score

        return matrix

    finally:
        conn.close()


# =====================================================================
# 🔥 GET STRATEGY BY SETUP + TYPE
# =====================================================================
@router.get("/strategies/by_setup/{setup_id}")
async def get_strategy_by_setup(
    setup_id: int,
    strategy_type: str = Query(None, description="Optioneel: 'manual', 'trading', 'dca'")
):

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Geen databaseverbinding")

    try:
        with conn.cursor() as cur:

            query = """
                SELECT id, data
                FROM strategies
                WHERE (data->>'setup_id')::int = %s
            """
            params = [setup_id]

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
# 🔚 LAATSTE STRATEGIE
# =====================================================================
@router.get("/strategies/last")
async def get_last_strategy():

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Geen databaseverbinding")

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, data, created_at 
                FROM strategies
                ORDER BY created_at DESC
                LIMIT 1
            """)
            row = cur.fetchone()

        if not row:
            return {"message": "Geen strategieën gevonden"}

        id_, data, created_at = row
        data["id"] = id_
        data["created_at"] = created_at.isoformat()

        return data

    @router.get("/tasks/{task_id}")
async def get_task_status(task_id: str):
    """
    Haalt de status op van een Celery taak.
    Wordt gebruikt door frontend om te wachten tot AI klaar is.
    """
    try:
        result = AsyncResult(task_id, app=celery_app)

        response = {
            "task_id": task_id,
            "state": result.state,
        }

        # Voeg result toe wanneer klaar
        if result.state == "SUCCESS":
            response["result"] = result.result

        if result.state == "FAILURE":
            response["error"] = str(result.result)

        return response

    except Exception as e:
        return {"task_id": task_id, "state": "ERROR", "error": str(e)}

    finally:
        conn.close()
