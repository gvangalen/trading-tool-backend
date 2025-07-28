from fastapi import APIRouter, HTTPException, Request
from backend.utils.db import get_db_connection
from datetime import datetime
import json
import logging
from backend.celery_task.setup_task import validate_setups_task

router = APIRouter()
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

@router.post("/setups")
async def save_setup(request: Request):
    try:
        data = await request.json()

        # ✅ Vereiste velden controleren
        required_fields = ["name", "symbol", "indicators", "trend"]
        for field in required_fields:
            if not data.get(field):
                logger.warning(f"[save_setup] ❌ '{field}' ontbreekt in data: {data}")
                raise HTTPException(status_code=400, detail=f"'{field}' is verplicht")

        # ✅ Fallback: convert strings naar lists
        indicators = data.get("indicators", [])
        if isinstance(indicators, str):
            indicators = [s.strip() for s in indicators.split(",")]

        tags = data.get("tags", [])
        if isinstance(tags, str):
            tags = [s.strip() for s in tags.split(",")]

        conn = get_db_connection()
        with conn.cursor() as cur:
            # ✅ Dubbele naam + symbool voorkomen
            cur.execute("SELECT id FROM setups WHERE name = %s AND symbol = %s", (data["name"], data["symbol"]))
            if cur.fetchone():
                logger.warning(f"[save_setup] ⚠️ Setup bestaat al: {data['name']} ({data['symbol']})")
                raise HTTPException(status_code=409, detail="Setup met deze naam en symbool bestaat al")

            # ✅ Correcte insert met native Python lists (voor PostgreSQL TEXT[])
            cur.execute("""
                INSERT INTO setups (
                    name, symbol, indicators, trend, account_type, strategy_type,
                    min_investment, tags, score_logic, dynamic_investment, favorite, created_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                data["name"],
                data["symbol"],
                indicators,
                data["trend"],
                data.get("account_type"),
                data.get("strategy_type"),
                data.get("min_investment"),
                tags,
                data.get("score_logic"),
                data.get("dynamic_investment", False),
                data.get("favorite", False),
                datetime.utcnow()
            ))
            conn.commit()

        logger.info(f"[save_setup] ✅ Setup opgeslagen: {data['name']} ({data['symbol']})")
        return {"status": "success", "message": "Setup opgeslagen"}

    except Exception as e:
        logger.exception(f"[save_setup] ❌ Fout bij opslaan: {e}")
        raise HTTPException(status_code=500, detail="Interne fout bij opslaan van setup")


# ✅ 2. Alle setups ophalen
@router.get("/setups")
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
@router.get("/setups/top")
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
@router.patch("/setups/{setup_id}")
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
@router.delete("/setups/{setup_id}")
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
@router.get("/setups/test")
async def test_setup_api():
    return {"message": "✅ Setup API werkt correct."}

# ✅ 7. Celery-trigger
@router.post("/setups/trigger")
def trigger_setup_task():
    validate_setups_task.delay()
    logger.info("🚀 Celery-taak 'validate_setups_task' gestart via API.")
    return {"message": "📡 Setup-validatie gestart via Celery."}

# ✅ 8. Naamcontrole endpoint
@router.get("/setups/check_name/{name}")
def check_setup_name(name: str):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="❌ Databaseverbinding mislukt.")
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM setups WHERE name = %s", (name,))
            count = cur.fetchone()[0]
            return {"exists": count > 0}
    except Exception as e:
        logger.error(f"❌ [check_setup_name] Fout: {e}")
        raise HTTPException(status_code=500, detail="❌ Fout bij naamcontrole.")
    finally:
        conn.close()
