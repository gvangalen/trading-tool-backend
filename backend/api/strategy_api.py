print("✅ strategy_api.py geladen!")  # komt in logs bij opstarten
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse
from backend.utils.db import get_db_connection
from celery_task.strategy_task import generate_strategy_task
from typing import Optional
from datetime import datetime
import json
import csv
import io
import logging

router = APIRouter()
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# ✅ Strategie opslaan
@router.post("/strategies")
async def save_strategy(request: Request):
    try:
        data = await request.json()
        required_fields = ["setup_id", "setup_name", "asset", "timeframe", "entry", "targets", "stop_loss"]
        for field in required_fields:
            if not data.get(field):
                logger.warning(f"[save_strategy] ❌ '{field}' ontbreekt in data: {data}")
                raise HTTPException(status_code=400, detail=f"'{field}' is verplicht")

        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM strategies WHERE data->>'setup_id' = %s", (str(data["setup_id"]),))
            if cur.fetchone():
                raise HTTPException(status_code=409, detail="Strategie bestaat al")

        keywords = ["breakout", "scalp", "swing", "reversal"]
        found_tags = [k for k in keywords if k in (data["setup_name"] + data.get("explanation", "")).lower()]
        data["tags"] = list(set(data.get("tags", []) + found_tags))
        data.setdefault("favorite", False)
        data.setdefault("origin", "Manual")
        data.setdefault("ai_reason", "")

        with conn.cursor() as cur:
            cur.execute("INSERT INTO strategies (data, created_at) VALUES (%s::jsonb, NOW()) RETURNING id",
                        (json.dumps(data),))
            strategy_id = cur.fetchone()[0]
            conn.commit()

        return {"message": "✅ Strategie opgeslagen", "id": strategy_id}
    except Exception as e:
        logger.error(f"[save_strategy] ❌ {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ✅ AI-strategie genereren (Celery)
@router.post("/strategies/generate/{setup_id}")
async def generate_strategy_for_setup(setup_id: int, request: Request):
    try:
        data = await request.json()
        overwrite = data.get("overwrite", True)

        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT id, data FROM setups WHERE id = %s", (setup_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Setup niet gevonden")
            _, setup = row

        for field in ["name", "asset", "timeframe"]:
            if not setup.get(field):
                raise HTTPException(status_code=400, detail=f"Setup mist verplicht veld: {field}")

        task = generate_strategy_task.delay(setup_id=setup_id, overwrite=overwrite)
        return {"message": "⏳ Strategie wordt gegenereerd", "task_id": task.id}

    except Exception as e:
        logger.error(f"[generate_strategy_for_setup] ❌ {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ✅ Strategie bijwerken
@router.put("/strategies/{strategy_id}")
async def update_strategy(strategy_id: int, request: Request):
    try:
        data = await request.json()
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT data FROM strategies WHERE id = %s", (strategy_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Strategie niet gevonden")
            strategy_data = row[0]
            strategy_data.update(data)

            cur.execute("UPDATE strategies SET data = %s WHERE id = %s", (json.dumps(strategy_data), strategy_id))
            conn.commit()
        return {"message": "✅ Strategie bijgewerkt"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ✅ Strategie verwijderen
@router.delete("/strategies/{strategy_id}")
async def delete_strategy(strategy_id: int):
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("DELETE FROM strategies WHERE id = %s", (strategy_id,))
            conn.commit()
        return {"message": "🗑️ Strategie verwijderd"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ✅ Toggle favorite
@router.patch("/strategies/{strategy_id}/favorite")
async def toggle_favorite(strategy_id: int):
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT data FROM strategies WHERE id = %s", (strategy_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Strategie niet gevonden")
            strategy = row[0]
            strategy["favorite"] = not strategy.get("favorite", False)

            cur.execute("UPDATE strategies SET data = %s WHERE id = %s", (json.dumps(strategy), strategy_id))
            conn.commit()
        return {"message": "✅ Favorite aangepast", "favorite": strategy["favorite"]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ✅ Strategieën filteren
@router.post("/strategies/filter")
async def filter_strategies(request: Request):
    filters = await request.json()
    asset = filters.get("asset")
    timeframe = filters.get("timeframe")
    tag = filters.get("tag")
    min_score = filters.get("min_score")

    conn = get_db_connection()
    with conn.cursor() as cur:
        cur.execute("SELECT id, data FROM strategies")
        rows = cur.fetchall()

    filtered = []
    for row in rows:
        id_, strategy = row
        if asset and strategy.get("asset") != asset:
            continue
        if timeframe and strategy.get("timeframe") != timeframe:
            continue
        if tag and tag not in strategy.get("tags", []):
            continue
        if min_score is not None and float(strategy.get("score", 0)) < float(min_score):
            continue
        strategy["id"] = id_
        filtered.append(strategy)

    return filtered


# ✅ CSV export
@router.get("/strategies/export")
async def export_strategies():
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT id, data, created_at FROM strategies")
            rows = cur.fetchall()

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["ID", "Asset", "Timeframe", "Setup", "Score", "Entry", "Stop Loss", "Origin", "Created"])

        for row in rows:
            s = row[1]
            writer.writerow([
                row[0],
                s.get("asset"),
                s.get("timeframe"),
                s.get("setup_name"),
                s.get("score"),
                s.get("entry"),
                s.get("stop_loss"),
                s.get("origin"),
                row[2].strftime("%Y-%m-%d %H:%M:%S")
            ])

        output.seek(0)
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=strategies.csv"}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ✅ Groeperen per setup
@router.get("/strategies/grouped_by_setup")
async def grouped_by_setup():
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    data->>'setup_id' AS setup_id,
                    COUNT(*) AS strategy_count,
                    MAX(created_at) AS last_created
                FROM strategies
                GROUP BY data->>'setup_id'
                ORDER BY last_created DESC
            """)
            rows = cur.fetchall()

        grouped = [
            {
                "setup_id": int(r[0]),
                "strategy_count": r[1],
                "last_created": r[2].isoformat()
            }
            for r in rows
        ]
        return grouped
    except Exception as e:
        logger.error(f"[grouped_by_setup] ❌ {e}")
        raise HTTPException(status_code=500, detail="Kon strategie-overzicht niet ophalen.")


# ✅ Score-matrix per asset × timeframe
@router.get("/strategies/score_matrix")
async def score_matrix():
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT data FROM strategies")
            rows = cur.fetchall()

        matrix = {}
        for row in rows:
            s = row[0]
            asset = s.get("asset")
            tf = s.get("timeframe")
            score = float(s.get("score", 0))
            if not asset or not tf:
                continue
            matrix.setdefault(asset, {})
            matrix[asset][tf] = round((matrix[asset].get(tf, 0) + score) / 2, 2) if tf in matrix[asset] else score

        return matrix
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ✅ Actieve strategieën (score > 6)
@router.get("/strategies/active")
async def active_strategies(min_score: float = 6.0):
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT id, data FROM strategies")
            rows = cur.fetchall()

        active = []
        for id_, s in rows:
            if float(s.get("score", 0)) >= min_score:
                s["id"] = id_
                active.append(s)

        return active
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ✅ AI-uitleg ophalen
@router.get("/strategies/{strategy_id}/explanation")
async def fetch_strategy_explanation(strategy_id: int):
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT data FROM strategies WHERE id = %s", (strategy_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Strategie niet gevonden")
            explanation = row[0].get("explanation", "")
        return {"id": strategy_id, "explanation": explanation}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ✅ Bulk generatie
@router.post("/strategies/generate_all")
async def generate_all_strategies(request: Request):
    try:
        data = await request.json()
        overwrite = data.get("overwrite", False)

        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT id, data FROM setups")
            setups = cur.fetchall()
            cur.execute("SELECT data->>'setup_id' FROM strategies")
            existing_ids = {int(row[0]) for row in cur.fetchall() if row[0].isdigit()}

            generated, skipped = [], []

            for setup_id, setup in setups:
                if setup_id in existing_ids and not overwrite:
                    skipped.append(setup_id)
                    continue

                strategy = {
                    "setup_id": setup_id,
                    "setup_name": setup.get("name"),
                    "asset": setup.get("asset"),
                    "timeframe": setup.get("timeframe"),
                    "type": "AI-Generated",
                    "explanation": f"Strategie gegenereerd op basis van setup '{setup.get('name')}'",
                    "ai_reason": "Op basis van technische en macrodata is deze strategie voorgesteld",
                    "entry": "100.00",
                    "targets": ["110.00", "120.00"],
                    "stop_loss": "95.00",
                    "risk_reward": "2.0",
                    "score": 7.5,
                    "tags": ["ai", "auto"],
                    "favorite": False,
                    "origin": "AI"
                }

                cur.execute("SELECT id FROM strategies WHERE data->>'setup_id' = %s", (str(setup_id),))
                existing = cur.fetchone()

                if existing and overwrite:
                    cur.execute("UPDATE strategies SET data = %s WHERE id = %s",
                                (json.dumps(strategy), existing[0]))
                    generated.append({"setup_id": setup_id, "updated": True})
                else:
                    cur.execute("INSERT INTO strategies (data, created_at) VALUES (%s::jsonb, NOW()) RETURNING id",
                                (json.dumps(strategy),))
                    new_id = cur.fetchone()[0]
                    generated.append({"setup_id": setup_id, "created": True})

            conn.commit()
            return {
                "message": f"✅ {len(generated)} strategieën gegenereerd",
                "generated": generated,
                "skipped": skipped
            }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
