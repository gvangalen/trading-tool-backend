from fastapi import APIRouter, HTTPException, Request
from backend.utils.db import get_db_connection
from datetime import datetime
import logging
from backend.celery_task.setup_task import validate_setups_task

router = APIRouter()
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# ✅ 1. Setup opslaan
@router.post("/")
async def save_setup(request: Request):
    data = await request.json()
    required_fields = ["name", "timeframe"]
    for field in required_fields:
        if not data.get(field):
            raise HTTPException(status_code=400, detail=f"'{field}' is verplicht")

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="❌ Databaseverbinding mislukt.")

    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO setups (
                    name, symbol, timeframe, account_type, strategy_type,
                    min_investment, dynamic_investment, score, description,
                    tags, indicators, trend, score_type, score_logic, favorite,
                    created_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                RETURNING id, created_at;
            """, (
                data.get("name"), data.get("symbol", "BTC"), data.get("timeframe"),
                data.get("account_type"), data.get("strategy_type"),
                data.get("min_investment"), data.get("dynamic", False),
                data.get("score"), data.get("description"), data.get("tags", []),
                data.get("indicators"), data.get("trend"),
                data.get("score_type"), data.get("score_logic"),
                data.get("favorite", False)
            ))
            setup_id, created_at = cur.fetchone()
            conn.commit()
            return {
                "message": "✅ Setup succesvol opgeslagen",
                "id": setup_id,
                "created_at": created_at.isoformat()
            }
    except Exception as e:
        logger.error(f"❌ [save_setup] Fout: {e}")
        raise HTTPException(status_code=500, detail=f"❌ Fout bij opslaan setup: {e}")
    finally:
        conn.close()

# ✅ 2. Alle setups ophalen
@router.get("/")
async def get_setups(symbol: str = "BTC"):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="❌ Databaseverbinding mislukt.")
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, name, symbol, timeframe, account_type, strategy_type,
                       min_investment, dynamic_investment, score, description,
                       tags, indicators, trend, score_type, score_logic, favorite,
                       created_at
                FROM setups
                WHERE symbol = %s
                ORDER BY created_at DESC
                LIMIT 50;
            """, (symbol,))
            rows = cur.fetchall()
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
                    "description": row[9],
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
    except Exception as e:
        logger.error(f"❌ [get_setups] Fout: {e}")
        raise HTTPException(status_code=500, detail="❌ Fout bij ophalen setups.")
    finally:
        conn.close()

# ✅ 3. Top setups ophalen
@router.get("/top")
async def get_top_setups(limit: int = 3):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="❌ Databaseverbinding mislukt.")
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, name, symbol, timeframe, account_type, strategy_type,
                       min_investment, dynamic_investment, score, description,
                       tags, indicators, trend, score_type, score_logic, favorite,
                       created_at
                FROM setups
                ORDER BY score DESC NULLS LAST
                LIMIT %s;
            """, (limit,))
            rows = cur.fetchall()
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
                    "description": row[9],
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
    except Exception as e:
        logger.error(f"❌ [get_top_setups] Fout: {e}")
        raise HTTPException(status_code=500, detail="❌ Fout bij ophalen top setups.")
    finally:
        conn.close()

# ✅ 4. Setup bijwerken
@router.patch("/{setup_id}")
async def update_setup(setup_id: int, request: Request):
    data = await request.json()
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="❌ Databaseverbinding mislukt.")
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE setups SET
                    name = %s,
                    symbol = %s,
                    timeframe = %s,
                    account_type = %s,
                    strategy_type = %s,
                    min_investment = %s,
                    dynamic_investment = %s,
                    score = %s,
                    description = %s,
                    tags = %s,
                    indicators = %s,
                    trend = %s,
                    score_type = %s,
                    score_logic = %s,
                    favorite = %s
                WHERE id = %s
            """, (
                data.get("name"), data.get("symbol"), data.get("timeframe"),
                data.get("account_type"), data.get("strategy_type"),
                data.get("min_investment"), data.get("dynamic"),
                data.get("score"), data.get("description"), data.get("tags"),
                data.get("indicators"), data.get("trend"),
                data.get("score_type"), data.get("score_logic"),
                data.get("favorite"), setup_id
            ))
            conn.commit()
            return {"message": "✅ Setup succesvol bijgewerkt."}
    except Exception as e:
        logger.error(f"❌ [update_setup] Fout: {e}")
        raise HTTPException(status_code=500, detail="❌ Fout bij bijwerken setup.")
    finally:
        conn.close()

# ✅ 5. Setup verwijderen
@router.delete("/{setup_id}")
async def delete_setup(setup_id: int):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="❌ Databaseverbinding mislukt.")
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM setups WHERE id = %s", (setup_id,))
            conn.commit()
            return {"message": "🗑️ Setup verwijderd"}
    except Exception as e:
        logger.error(f"❌ [delete_setup] Fout: {e}")
        raise HTTPException(status_code=500, detail="❌ Fout bij verwijderen setup.")
    finally:
        conn.close()

# ✅ 6. Test endpoint
@router.get("/test")
async def test_setup_api():
    return {"message": "✅ Setup API werkt correct."}

# ✅ 7. Celery-trigger
@router.post("/trigger")
def trigger_setup_task():
    validate_setups_task.delay()
    logger.info("🚀 Celery-taak 'validate_setups_task' gestart via API.")
    return {"message": "📡 Setup-validatie gestart via Celery."}
