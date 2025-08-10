print("✅ strategy_api.py geladen!")  # komt in logs bij opstarten

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse
from backend.utils.db import get_db_connection
from backend.utils.ai_strategy_utils import generate_strategy_from_setup
from backend.celery_task.strategy_task import generate_strategie_voor_setup as generate_strategy_task  # ✅ JUISTE CELERY IMPORT
from typing import Optional
from datetime import datetime
import json
import csv
import io
import logging

router = APIRouter()
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


@router.post("/strategies")
async def save_strategy(request: Request):
    try:
        data = await request.json()

        # Strategie-type ophalen (default = manual)
        strategy_type = data.get("strategy_type", "manual").lower()

        # Algemene verplichte velden (voor alle types)
        required_fields = ["setup_id", "setup_name", "symbol", "timeframe"]

        # Extra verplichte velden per type
        if strategy_type == "dca":
            required_fields += ["amount", "frequency"]
            # entry, targets, stop_loss NIET verplicht voor DCA
        elif strategy_type in ["manual", "trading"]:
            required_fields += ["entry", "targets", "stop_loss"]
        else:
            logger.warning(f"[save_strategy] Onbekend strategy_type: {strategy_type}")
            raise HTTPException(status_code=400, detail=f"Onbekend strategy_type: {strategy_type}")

        # Validatie van verplichte velden
        for field in required_fields:
            if field not in data or data.get(field) in [None, "", []]:
                logger.warning(f"[save_strategy] ❌ '{field}' ontbreekt of is leeg in data: {data}")
                raise HTTPException(status_code=400, detail=f"Veld '{field}' is verplicht.")

        # Database connectie
        conn = get_db_connection()
        if not conn:
            logger.error("[save_strategy] Geen databaseverbinding")
            raise HTTPException(status_code=500, detail="Geen databaseverbinding")

        # Controleren of strategie al bestaat voor deze setup
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM strategies WHERE data->>'setup_id' = %s", (str(data["setup_id"]),))
            if cur.fetchone():
                logger.warning(f"[save_strategy] Strategie bestaat al voor setup_id {data['setup_id']}")
                raise HTTPException(status_code=409, detail="Strategie bestaat al")

        # Tags automatisch toevoegen
        keywords = ["breakout", "scalp", "swing", "reversal", "dca"]
        combined_text = (data.get("setup_name", "") + " " + data.get("explanation", "")).lower()
        found_tags = [k for k in keywords if k in combined_text]
        data["tags"] = list(set(data.get("tags", []) + found_tags))

        # Standaardwaarden
        data.setdefault("favorite", False)
        data.setdefault("origin", strategy_type.upper())
        data.setdefault("ai_reason", "")
        data["strategy_type"] = strategy_type  # forceer correcte waarde

        # Strategie opslaan in database
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO strategies (data, created_at) VALUES (%s::jsonb, NOW()) RETURNING id",
                (json.dumps(data),)
            )
            strategy_id = cur.fetchone()[0]
            conn.commit()

        logger.info(f"[save_strategy] ✅ Strategie opgeslagen met ID {strategy_id}")
        return {"message": "✅ Strategie opgeslagen", "id": strategy_id}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[save_strategy] ❌ {e}")
        raise HTTPException(status_code=500, detail="Interne serverfout bij opslaan strategie.")


@router.post("/strategies/query")
async def query_strategies(request: Request):
    try:
        filters = await request.json()
        symbol = filters.get("symbol", "")
        timeframe = filters.get("timeframe", "")
        tag = filters.get("tag", "")

        conn = get_db_connection()
        if not conn:
            logger.error("[query_strategies] Geen databaseverbinding")
            raise HTTPException(status_code=500, detail="Geen databaseverbinding")

        with conn.cursor() as cur:
            query = "SELECT * FROM strategies WHERE TRUE"
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

            logger.info(f"[query_strategies] Uitvoeren query: {query} met params: {params}")
            cur.execute(query, tuple(params))
            rows = cur.fetchall()

        return [row[1] for row in rows]  # return alleen de JSON data

    except Exception as e:
        logger.error(f"[query_strategies] ❌ {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/strategies/generate/{setup_id}")
async def generate_strategy_for_setup(setup_id: int, request: Request):
    try:
        data = await request.json()
        overwrite = data.get("overwrite", True)

        conn = get_db_connection()
        if not conn:
            logger.error("[generate_strategy_for_setup] Geen databaseverbinding")
            raise HTTPException(status_code=500, detail="Geen databaseverbinding")

        with conn.cursor() as cur:
            cur.execute("SELECT id, data FROM setups WHERE id = %s", (setup_id,))
            row = cur.fetchone()
            if not row:
                logger.warning(f"[generate_strategy_for_setup] Setup niet gevonden met ID {setup_id}")
                raise HTTPException(status_code=404, detail="Setup niet gevonden")
            _, setup = row

        for field in ["name", "symbol", "timeframe"]:
            if not setup.get(field):
                logger.warning(f"[generate_strategy_for_setup] Setup mist verplicht veld: {field}")
                raise HTTPException(status_code=400, detail=f"Setup mist verplicht veld: {field}")

        task = generate_strategy_task.delay(setup_id=setup_id, overwrite=overwrite)  # ✅ JUISTE CELERY AANROEP
        logger.info(f"[generate_strategy_for_setup] Celery taak gestart met ID: {task.id}")
        return {"message": "⏳ Strategie wordt gegenereerd", "task_id": task.id}

    except Exception as e:
        logger.error(f"[generate_strategy_for_setup] ❌ {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/strategies/{strategy_id}")
async def update_strategy(strategy_id: int, request: Request):
    try:
        data = await request.json()
        conn = get_db_connection()
        if not conn:
            logger.error("[update_strategy] Geen databaseverbinding")
            raise HTTPException(status_code=500, detail="Geen databaseverbinding")

        with conn.cursor() as cur:
            cur.execute("SELECT data FROM strategies WHERE id = %s", (strategy_id,))
            row = cur.fetchone()
            if not row:
                logger.warning(f"[update_strategy] Strategie niet gevonden met ID {strategy_id}")
                raise HTTPException(status_code=404, detail="Strategie niet gevonden")
            strategy_data = row[0]
            strategy_data.update(data)

            cur.execute("UPDATE strategies SET data = %s WHERE id = %s", (json.dumps(strategy_data), strategy_id))
            conn.commit()

        logger.info(f"[update_strategy] Strategie ID {strategy_id} succesvol bijgewerkt")
        return {"message": "✅ Strategie bijgewerkt"}
    except Exception as e:
        logger.error(f"[update_strategy] ❌ {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/strategies/{strategy_id}")
async def delete_strategy(strategy_id: int):
    try:
        conn = get_db_connection()
        if not conn:
            logger.error("[delete_strategy] Geen databaseverbinding")
            raise HTTPException(status_code=500, detail="Geen databaseverbinding")

        with conn.cursor() as cur:
            cur.execute("DELETE FROM strategies WHERE id = %s", (strategy_id,))
            conn.commit()

        logger.info(f"[delete_strategy] Strategie ID {strategy_id} verwijderd")
        return {"message": "🗑️ Strategie verwijderd"}
    except Exception as e:
        logger.error(f"[delete_strategy] ❌ {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/strategies/{strategy_id}/favorite")
async def toggle_favorite(strategy_id: int):
    try:
        conn = get_db_connection()
        if not conn:
            logger.error("[toggle_favorite] Geen databaseverbinding")
            raise HTTPException(status_code=500, detail="Geen databaseverbinding")

        with conn.cursor() as cur:
            cur.execute("SELECT data FROM strategies WHERE id = %s", (strategy_id,))
            row = cur.fetchone()
            if not row:
                logger.warning(f"[toggle_favorite] Strategie niet gevonden met ID {strategy_id}")
                raise HTTPException(status_code=404, detail="Strategie niet gevonden")
            strategy = row[0]
            strategy["favorite"] = not strategy.get("favorite", False)

            cur.execute("UPDATE strategies SET data = %s WHERE id = %s", (json.dumps(strategy), strategy_id))
            conn.commit()

        logger.info(f"[toggle_favorite] Favorite status aangepast voor strategie ID {strategy_id}")
        return {"message": "✅ Favorite aangepast", "favorite": strategy["favorite"]}
    except Exception as e:
        logger.error(f"[toggle_favorite] ❌ {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/strategies/filter")
async def filter_strategies(request: Request):
    filters = await request.json()
    symbol = filters.get("symbol")
    timeframe = filters.get("timeframe")
    tag = filters.get("tag")
    min_score = filters.get("min_score")

    conn = get_db_connection()
    if not conn:
        logger.error("[filter_strategies] Geen databaseverbinding")
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

        query += " ORDER BY created_at DESC"

        with conn.cursor() as cur:
            logger.info(f"[filter_strategies] Uitvoeren query: {query} met params: {params}")
            cur.execute(query, tuple(params))
            rows = cur.fetchall()

        filtered = []
        for row in rows:
            id_, strategy = row
            strategy["id"] = id_
            filtered.append(strategy)

        logger.info(f"[filter_strategies] Gefilterde strategieën: {len(filtered)}")
        return filtered
    finally:
        conn.close()


@router.get("/strategies/export")
async def export_strategies():
    try:
        conn = get_db_connection()
        if not conn:
            logger.error("[export_strategies] Geen databaseverbinding")
            raise HTTPException(status_code=500, detail="Geen databaseverbinding")

        with conn.cursor() as cur:
            cur.execute("SELECT id, data, created_at FROM strategies")
            rows = cur.fetchall()

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["ID", "Symbol", "Timeframe", "Setup", "Score", "Entry", "Stop Loss", "Origin", "Created"])

        for row in rows:
            s = row[1]
            writer.writerow([
                row[0],
                s.get("symbol"),
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
        logger.error(f"[export_strategies] ❌ {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/strategies/grouped_by_setup")
async def grouped_by_setup():
    try:
        conn = get_db_connection()
        if not conn:
            logger.error("[grouped_by_setup] Geen databaseverbinding")
            raise HTTPException(status_code=500, detail="Geen databaseverbinding")

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

        logger.info(f"[grouped_by_setup] Strategie-overzicht met {len(grouped)} groepen opgehaald")
        return grouped
    except Exception as e:
        logger.error(f"[grouped_by_setup] ❌ {e}")
        raise HTTPException(status_code=500, detail="Kon strategie-overzicht niet ophalen.")


@router.get("/strategies/score_matrix")
async def score_matrix():
    try:
        conn = get_db_connection()
        if not conn:
            logger.error("[score_matrix] Geen databaseverbinding")
            raise HTTPException(status_code=500, detail="Geen databaseverbinding")

        with conn.cursor() as cur:
            cur.execute("SELECT data FROM strategies")
            rows = cur.fetchall()

        matrix = {}
        for row in rows:
            s = row[0]
            symbol = s.get("symbol")
            tf = s.get("timeframe")
            score = float(s.get("score", 0))
            if not symbol or not tf:
                continue
            matrix.setdefault(symbol, {})
            matrix[symbol][tf] = round((matrix[symbol].get(tf, 0) + score) / 2, 2) if tf in matrix[symbol] else score

        logger.info(f"[score_matrix] Score-matrix opgebouwd voor {len(matrix)} symbolen")
        return matrix
    except Exception as e:
        logger.error(f"[score_matrix] ❌ {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/strategies/active")
async def active_strategies(min_score: float = 6.0):
    try:
        conn = get_db_connection()
        if not conn:
            logger.error("[active_strategies] Geen databaseverbinding")
            raise HTTPException(status_code=500, detail="Geen databaseverbinding")

        with conn.cursor() as cur:
            cur.execute("SELECT id, data FROM strategies")
            rows = cur.fetchall()

        active = []
        for id_, s in rows:
            if float(s.get("score", 0)) >= min_score:
                s["id"] = id_
                active.append(s)

        logger.info(f"[active_strategies] Actieve strategieën opgehaald: {len(active)}")
        return active
    except Exception as e:
        logger.error(f"[active_strategies] ❌ {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/strategies/{strategy_id}/explanation")
async def fetch_strategy_explanation(strategy_id: int):
    try:
        conn = get_db_connection()
        if not conn:
            logger.error("[fetch_strategy_explanation] Geen databaseverbinding")
            raise HTTPException(status_code=500, detail="Geen databaseverbinding")

        with conn.cursor() as cur:
            cur.execute("SELECT data FROM strategies WHERE id = %s", (strategy_id,))
            row = cur.fetchone()
            if not row:
                logger.warning(f"[fetch_strategy_explanation] Strategie niet gevonden met ID {strategy_id}")
                raise HTTPException(status_code=404, detail="Strategie niet gevonden")
            explanation = row[0].get("explanation", "")
        logger.info(f"[fetch_strategy_explanation] Uitleg opgehaald voor strategie ID {strategy_id}")
        return {"id": strategy_id, "explanation": explanation}
    except Exception as e:
        logger.error(f"[fetch_strategy_explanation] ❌ {e}")
        raise HTTPException(status_code=500, detail=str(e))