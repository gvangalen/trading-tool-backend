from fastapi import APIRouter, HTTPException, Request
from backend.utils.db import get_db_connection
from datetime import datetime
import logging
from backend.celery_task.setup_task import validate_setups_task  # Celery trigger

router = APIRouter()
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# ✅ Setup opslaan
@router.post("/setups")
async def save_setup(request: Request):
    data = await request.json()

    name = data.get("name")
    symbol = data.get("symbol", "BTC")
    timeframe = data.get("timeframe")
    account_type = data.get("account_type")
    strategy_type = data.get("strategy_type")
    min_investment = data.get("min_investment")
    dynamic = data.get("dynamic", False)
    score = data.get("score")
    description = data.get("description")
    tags = data.get("tags", [])

    if not name or not timeframe:
        raise HTTPException(status_code=400, detail="Naam en timeframe zijn verplicht.")

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="❌ Databaseverbinding mislukt.")

    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO setups (
                    name, symbol, timeframe, account_type, strategy_type,
                    min_investment, dynamic_investment, score, description, tags, created_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                RETURNING id, created_at;
            """, (
                name, symbol, timeframe, account_type, strategy_type,
                min_investment, dynamic, score, description, tags
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


# ✅ Setup-lijst ophalen
@router.get("/setups")
async def get_setups(symbol: str = "BTC"):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="❌ Databaseverbinding mislukt.")

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, name, symbol, timeframe, account_type, strategy_type,
                       min_investment, dynamic_investment, score, description, tags, created_at
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
                    "created_at": row[11].isoformat() if row[11] else None
                }
                for row in rows
            ]
    except Exception as e:
        logger.error(f"❌ [get_setups] Fout: {e}")
        raise HTTPException(status_code=500, detail="❌ Fout bij ophalen setups.")
    finally:
        conn.close()


# ✅ Top setups op basis van hoogste score
@router.get("/setups/top")
async def get_top_setups(limit: int = 3):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="❌ Databaseverbinding mislukt.")

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, name, symbol, timeframe, account_type, strategy_type,
                       min_investment, dynamic_investment, score, description, tags, created_at
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
                    "created_at": row[11].isoformat() if row[11] else None
                }
                for row in rows
            ]
    except Exception as e:
        logger.error(f"❌ [get_top_setups] Fout: {e}")
        raise HTTPException(status_code=500, detail="❌ Fout bij ophalen top setups.")
    finally:
        conn.close()


# ✅ Test endpoint
@router.get("/setups/test")
async def test_setup_api():
    try:
        conn = get_db_connection()
        conn.close()
        return {"message": "✅ Setup API werkt correct."}
    except Exception as e:
        logger.error(f"❌ [test] Fout: {e}")
        raise HTTPException(status_code=500, detail="❌ Test endpoint faalde.")


# ✅ Celery-taak triggeren
@router.post("/setups/trigger")
def trigger_setup_task():
    validate_setups_task.delay()
    logger.info("🚀 Celery-taak 'validate_setups_task' gestart via API.")
    return {"message": "📡 Setup-validatie gestart via Celery."}
