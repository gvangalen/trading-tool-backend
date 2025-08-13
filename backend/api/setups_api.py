from fastapi import APIRouter, HTTPException, Request, Query, Path
from backend.utils.db import get_db_connection
from datetime import datetime
import logging
from backend.celery_task.setup_task import validate_setups_task
from typing import Optional

router = APIRouter()
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def format_setup_rows(rows):
    return [
        {
            "id": row[0],
            "name": row[1],
            "symbol": row[2],
            "timeframe": row[3],
            "account_type": row[4],
            "strategy_type": row[5],
            "min_investment": row[6],
            "dynamic": row[7],
            "score": row[8],
            "explanation": row[9],
            "tags": row[10],
            "indicators": row[11],
            "trend": row[12],
            "score_type": row[13],
            "score_logic": row[14],
            "favorite": row[15],
            "created_at": row[16].isoformat() if row[16] else None
        }
        for row in rows
    ]

# 1. Setup opslaan
@router.post("/setups")
async def save_setup(request: Request):
    try:
        data = await request.json()
        logger.info(f"[save_setup] Ontvangen data: {data}")

        required_fields = ["name", "symbol", "indicators", "trend"]
        for field in required_fields:
            if not data.get(field):
                logger.warning(f"[save_setup] ❌ '{field}' ontbreekt in data: {data}")
                raise HTTPException(status_code=400, detail=f"'{field}' is verplicht")

        indicators = data.get("indicators", [])
        if isinstance(indicators, str):
            indicators = [s.strip() for s in indicators.split(",") if s.strip()]

        tags = data.get("tags", [])
        if isinstance(tags, str):
            tags = [s.strip() for s in tags.split(",") if s.strip()]

        conn = get_db_connection()
        if not conn:
            logger.error("[save_setup] Geen databaseverbinding")
            raise HTTPException(status_code=500, detail="Geen databaseverbinding")

        with conn.cursor() as cur:
            query_check = "SELECT id FROM setups WHERE name = %s AND symbol = %s"
            params_check = (data["name"], data["symbol"])
            logger.debug(f"[save_setup] Uitvoeren query: {query_check} met params: {params_check}")
            cur.execute(query_check, params_check)
            if cur.fetchone():
                logger.warning(f"[save_setup] ⚠️ Setup bestaat al: {data['name']} ({data['symbol']})")
                raise HTTPException(status_code=409, detail="Setup met deze naam en symbool bestaat al")

            query_insert = """
                INSERT INTO setups (
                    name, symbol, indicators, trend, timeframe,
                    account_type, strategy_type, min_investment,
                    tags, score_logic, dynamic_investment, favorite, created_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            params_insert = (
                data["name"],
                data["symbol"],
                indicators,
                data["trend"],
                data.get("timeframe"),
                data.get("account_type"),
                data.get("strategy_type"),
                data.get("min_investment"),
                tags,
                data.get("score_logic"),
                data.get("dynamic_investment", False),
                data.get("favorite", False),
                datetime.utcnow()
            )
            logger.debug(f"[save_setup] Uitvoeren query: {query_insert} met params: {params_insert}")
            cur.execute(query_insert, params_insert)
            conn.commit()
            logger.info(f"[save_setup] ✅ Setup succesvol opgeslagen: {data['name']} ({data['symbol']})")

        return {"status": "success", "message": "Setup opgeslagen"}

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"[save_setup] ❌ Fout bij opslaan: {e}")
        raise HTTPException(status_code=500, detail="Interne fout bij opslaan van setup")
    finally:
        if 'conn' in locals() and conn:
            conn.close()

# 2b. Specifiek alleen DCA setups ophalen
@router.get("/setups/dca")
async def get_dca_setups():
    logger.info("[get_dca_setups] Ophalen alle DCA setups")
    conn = get_db_connection()
    if not conn:
        logger.error("[get_dca_setups] Geen databaseverbinding")
        raise HTTPException(status_code=500, detail="❌ Databaseverbinding mislukt.")
    try:
        with conn.cursor() as cur:
            query = """
                SELECT id, name, symbol, timeframe, account_type, strategy_type,
                       min_investment, dynamic_investment, score, explanation,
                       tags, indicators, trend, score_type, score_logic, favorite,
                       created_at
                FROM setups
                WHERE LOWER(strategy_type) = 'dca'
                ORDER BY created_at DESC LIMIT 50;
            """
            logger.debug(f"[get_dca_setups] Uitvoeren query: {query}")
            cur.execute(query)
            rows = cur.fetchall()
            logger.info(f"[get_dca_setups] Aantal DCA setups opgehaald: {len(rows)}")

            return format_setup_rows(rows)
    except Exception as e:
        logger.error(f"❌ [get_dca_setups] Fout bij ophalen DCA setups: {e}")
        raise HTTPException(status_code=500, detail="❌ Fout bij ophalen DCA setups.")
    finally:
        if conn:
            conn.close()

# 2. Alle setups ophalen zonder filters, met optionele filters strategy_type of exclude_strategy_type
@router.get("/setups")
async def get_setups(strategy_type: Optional[str] = Query(None), exclude_strategy_type: Optional[str] = Query(None)):
    logger.info(f"[get_setups] Ophalen setups met filter strategy_type={strategy_type} exclude={exclude_strategy_type}")
    conn = get_db_connection()
    if not conn:
        logger.error("[get_setups] Geen databaseverbinding")
        raise HTTPException(status_code=500, detail="❌ Databaseverbinding mislukt.")
    try:
        with conn.cursor() as cur:
            query = """
                SELECT id, name, symbol, timeframe, account_type, strategy_type,
                       min_investment, dynamic_investment, score, explanation,
                       tags, indicators, trend, score_type, score_logic, favorite,
                       created_at
                FROM setups
                WHERE TRUE
            """
            params = []
            if strategy_type:
                query += " AND LOWER(strategy_type) = LOWER(%s)"
                params.append(strategy_type)
            if exclude_strategy_type:
                query += " AND LOWER(strategy_type) != LOWER(%s)"
                params.append(exclude_strategy_type)

            query += " ORDER BY created_at DESC LIMIT 100"
            logger.debug(f"[get_setups] Uitvoeren query: {query} met params: {params}")
            cur.execute(query, tuple(params))
            rows = cur.fetchall()
            logger.info(f"[get_setups] Aantal setups opgehaald: {len(rows)}")

            return format_setup_rows(rows)
    except Exception as e:
        logger.error(f"❌ [get_setups] Fout bij ophalen setups: {e}")
        raise HTTPException(status_code=500, detail="❌ Fout bij ophalen setups.")
    finally:
        if conn:
            conn.close()

# 3. Top setups ophalen
@router.get("/setups/top")
async def get_top_setups(limit: int = Query(3, ge=1, le=100)):
    logger.info(f"[get_top_setups] Ophalen top {limit} setups")
    conn = get_db_connection()
    if not conn:
        logger.error("[get_top_setups] Geen databaseverbinding")
        raise HTTPException(status_code=500, detail="❌ Databaseverbinding mislukt.")
    try:
        with conn.cursor() as cur:
            query = """
                SELECT id, name, symbol, timeframe, account_type, strategy_type,
                       min_investment, dynamic_investment, score, explanation,
                       tags, indicators, trend, score_type, score_logic, favorite,
                       created_at
                FROM setups
                ORDER BY score DESC NULLS LAST
                LIMIT %s;
            """
            logger.debug(f"[get_top_setups] Uitvoeren query: {query} met limit: {limit}")
            cur.execute(query, (limit,))
            rows = cur.fetchall()
            logger.info(f"[get_top_setups] Aantal top setups opgehaald: {len(rows)}")

            return format_setup_rows(rows)
    except Exception as e:
        logger.error(f"❌ [get_top_setups] Fout bij ophalen top setups: {e}")
        raise HTTPException(status_code=500, detail="❌ Fout bij ophalen top setups.")
    finally:
        if conn:
            conn.close()


# **NIEUWE** GET setup details per ID (voorkomt botsing met /setups/dca)
@router.get("/setups/{setup_id}")
async def get_setup(setup_id: int = Path(..., title="Setup ID", ge=1)):
    logger.info(f"[get_setup] Ophalen setup ID {setup_id}")
    conn = get_db_connection()
    if not conn:
        logger.error("[get_setup] Geen databaseverbinding")
        raise HTTPException(status_code=500, detail="Databaseverbinding mislukt.")
    try:
        with conn.cursor() as cur:
            query = """
                SELECT id, name, symbol, timeframe, account_type, strategy_type,
                       min_investment, dynamic_investment, score, explanation,
                       tags, indicators, trend, score_type, score_logic, favorite,
                       created_at
                FROM setups
                WHERE id = %s
            """
            cur.execute(query, (setup_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Setup niet gevonden.")
            return format_setup_rows([row])[0]
    finally:
        if conn:
            conn.close()


# 4. Setup bijwerken
@router.patch("/setups/{setup_id}")
async def update_setup(request: Request, setup_id: int = Path(..., title="Setup ID", ge=1)):
    data = await request.json()
    logger.info(f"[update_setup] Update setup ID {setup_id} met data: {data}")
    conn = get_db_connection()
    if not conn:
        logger.error("[update_setup] Geen databaseverbinding")
        raise HTTPException(status_code=500, detail="❌ Databaseverbinding mislukt.")
    try:
        with conn.cursor() as cur:
            query = """
                UPDATE setups SET
                    name = %s,
                    symbol = %s,
                    timeframe = %s,
                    account_type = %s,
                    strategy_type = %s,
                    min_investment = %s,
                    dynamic_investment = %s,
                    score = %s,
                    explanation = %s,
                    tags = %s,
                    indicators = %s,
                    trend = %s,
                    score_type = %s,
                    score_logic = %s,
                    favorite = %s
                WHERE id = %s
            """
            params = (
                data.get("name"), data.get("symbol"), data.get("timeframe"),
                data.get("account_type"), data.get("strategy_type"),
                data.get("min_investment"), data.get("dynamic"),
                data.get("score"), data.get("explanation"), data.get("tags"),
                data.get("indicators"), data.get("trend"),
                data.get("score_type"), data.get("score_logic"),
                data.get("favorite"), setup_id
            )
            logger.debug(f"[update_setup] Uitvoeren query: {query} met params: {params}")
            cur.execute(query, params)
            conn.commit()
            logger.info(f"[update_setup] Setup ID {setup_id} succesvol bijgewerkt")
            return {"message": "✅ Setup succesvol bijgewerkt."}
    except Exception as e:
        logger.error(f"❌ [update_setup] Fout bij bijwerken setup: {e}")
        raise HTTPException(status_code=500, detail="❌ Fout bij bijwerken setup.")
    finally:
        if conn:
            conn.close()

# ✅ Setup verwijderen + gekoppelde strategieën verwijderen
@router.delete("/setups/{setup_id}")
async def delete_setup(setup_id: int):
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            # ⛔️ Verwijder eerst strategieën die aan deze setup gekoppeld zijn
            cur.execute("DELETE FROM strategies WHERE data->>'setup_id' = %s", (str(setup_id),))

            # ✅ Verwijder daarna pas de setup zelf
            cur.execute("DELETE FROM setups WHERE id = %s", (setup_id,))
        conn.commit()
        return {"status": "success", "message": f"Setup en gekoppelde strategieën verwijderd (setup_id={setup_id})"}
    except Exception as e:
        logger.error(f"[delete_setup] ❌ {e}")
        raise HTTPException(status_code=500, detail=str(e))

# 6. Test endpoint
@router.get("/setups/test")
async def test_setup_api():
    logger.info("[test_setup_api] Test endpoint aangeroepen")
    return {"message": "✅ Setup API werkt correct."}

# 7. Celery-trigger
@router.post("/setups/trigger")
def trigger_setup_task():
    logger.info("🚀 Celery-taak 'validate_setups_task' gestart via API.")
    validate_setups_task.delay()
    return {"message": "📡 Setup-validatie gestart via Celery."}

# 8. Naamcontrole
@router.get("/setups/check_name/{name}")
def check_setup_name(name: str):
    logger.info(f"[check_setup_name] Controleren of setup naam bestaat: {name}")
    conn = get_db_connection()
    if not conn:
        logger.error("[check_setup_name] Geen databaseverbinding")
        raise HTTPException(status_code=500, detail="❌ Databaseverbinding mislukt.")
    try:
        with conn.cursor() as cur:
            query = "SELECT COUNT(*) FROM setups WHERE name = %s"
            params = (name,)
            logger.debug(f"[check_setup_name] Uitvoeren query: {query} met params: {params}")
            cur.execute(query, params)
            count = cur.fetchone()[0]
            exists = count > 0
            logger.info(f"[check_setup_name] Naam '{name}' bestaat: {exists}")
            return {"exists": exists}
    except Exception as e:
        logger.error(f"❌ [check_setup_name] Fout bij naamcontrole: {e}")
        raise HTTPException(status_code=500, detail="❌ Fout bij naamcontrole.")
    finally:
        if conn:
            conn.close()

# 9. AI-uitleg genereren
@router.post("/setups/explanation/{setup_id}")
async def generate_explanation(setup_id: int):
    logger.info(f"[generate_explanation] AI-uitleg genereren voor setup ID {setup_id}")
    try:
        from backend.api.ai.setup_explanation import generate_ai_explanation
        explanation = generate_ai_explanation(setup_id)

        conn = get_db_connection()
        if not conn:
            logger.error("[generate_explanation] Geen databaseverbinding")
            raise HTTPException(status_code=500, detail="Geen databaseverbinding")

        with conn.cursor() as cur:
            query = "UPDATE setups SET explanation = %s WHERE id = %s"
            params = (explanation, setup_id)
            logger.debug(f"[generate_explanation] Uitvoeren query: {query} met params: {params}")
            cur.execute(query, params)
            conn.commit()

        logger.info(f"[generate_explanation] Uitleg succesvol opgeslagen voor setup ID {setup_id}")
        return {"explanation": explanation}
    except Exception as e:
        logger.exception(f"❌ [generate_explanation] Fout bij AI-uitleg: {e}")
        raise HTTPException(status_code=500, detail="Fout bij genereren van uitleg.")
    finally:
        if 'conn' in locals() and conn:
            conn.close()

print("🚀 setups_api geladen met routes:", [route.path for route in router.routes])
