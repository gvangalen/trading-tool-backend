# setup_api.py
import logging
import json
from fastapi import APIRouter, HTTPException
from fastapi import Request
from db import get_db_connection

router = APIRouter()
logger = logging.getLogger(__name__)

# ✅ Setup aanmaken
@router.post("/api/setups")
async def save_setup(request: Request):
    data = await request.json()

    setup_name = data.get("name")
    trend = data.get("trend")
    indicators = data.get("indicators")
    timeframe = data.get("timeframe")
    account_type = data.get("account_type")
    strategy_type = data.get("strategy_type")
    symbol = data.get("symbol", "BTC")
    min_investment = data.get("min_investment")
    dynamic = data.get("dynamic", False)

    if not setup_name or not trend or not indicators or not timeframe:
        raise HTTPException(status_code=400, detail="Naam, trend, indicatoren en timeframe zijn verplicht.")

    conditions = {
        "indicators": indicators,
        "trend": trend,
        "timeframe": timeframe,
        "account_type": account_type,
        "strategy_type": strategy_type,
        "min_investment": min_investment,
        "dynamic": dynamic
    }

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Databaseverbinding mislukt")

    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO setups (symbol, setup_name, conditions, created_at)
                VALUES (%s, %s, %s::jsonb, NOW())
                RETURNING id;
            """, (symbol, setup_name, json.dumps(conditions)))
            setup_id = cur.fetchone()[0]
            conn.commit()

        logger.info(f"✅ Setup '{setup_name}' opgeslagen (ID: {setup_id})")
        return {"message": "Setup succesvol opgeslagen", "id": setup_id}

    except Exception as e:
        logger.error(f"❌ Fout in save_setup: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

# ✅ Setups ophalen
@router.get("/api/setups")
async def get_setups(symbol: str = "BTC"):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Databaseverbinding mislukt")

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, symbol, setup_name, conditions, created_at
                FROM setups
                WHERE symbol = %s
                ORDER BY created_at DESC
                LIMIT 50;
            """, (symbol,))
            rows = cur.fetchall()

        return [
            {
                "id": row[0],
                "symbol": row[1],
                "name": row[2],
                "indicators": row[3].get("indicators"),
                "trend": row[3].get("trend"),
                "timeframe": row[3].get("timeframe"),
                "account_type": row[3].get("account_type"),
                "strategy_type": row[3].get("strategy_type"),
                "min_investment": row[3].get("min_investment"),
                "dynamic": row[3].get("dynamic"),
                "created_at": str(row[4])
            } for row in rows
        ]

    except Exception as e:
        logger.error(f"❌ Fout in get_setups: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

# ✅ Setup verwijderen
@router.delete("/api/setups/{setup_id}")
async def delete_setup(setup_id: int):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Databaseverbinding mislukt")

    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM setups WHERE id = %s RETURNING id;", (setup_id,))
            deleted = cur.fetchone()

            if not deleted:
                raise HTTPException(status_code=404, detail="Setup niet gevonden")

            conn.commit()
            return {"message": f"Setup {setup_id} verwijderd"}

    except Exception as e:
        logger.error(f"❌ Fout in delete_setup: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

# ✅ Setup bijwerken
@router.put("/api/setups/{setup_id}")
async def update_setup(setup_id: int, request: Request):
    data = await request.json()

    setup_name = data.get("name")
    trend = data.get("trend")
    indicators = data.get("indicators")
    timeframe = data.get("timeframe")
    account_type = data.get("account_type")
    strategy_type = data.get("strategy_type")
    symbol = data.get("symbol", "BTC")
    min_investment = data.get("min_investment")
    dynamic = data.get("dynamic", False)

    if not setup_name or not trend or not indicators or not timeframe:
        raise HTTPException(status_code=400, detail="Naam, trend, indicatoren en timeframe zijn verplicht.")

    conditions = {
        "indicators": indicators,
        "trend": trend,
        "timeframe": timeframe,
        "account_type": account_type,
        "strategy_type": strategy_type,
        "min_investment": min_investment,
        "dynamic": dynamic
    }

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Databaseverbinding mislukt")

    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE setups
                SET setup_name = %s,
                    conditions = %s::jsonb
                WHERE id = %s
                RETURNING id;
            """, (setup_name, json.dumps(conditions), setup_id))
            updated = cur.fetchone()

            if not updated:
                raise HTTPException(status_code=404, detail="Setup niet gevonden")

            conn.commit()
            logger.info(f"🔄 Setup {setup_id} succesvol bijgewerkt")
            return {"message": f"Setup {setup_id} geüpdatet"}

    except Exception as e:
        logger.error(f"❌ Fout in update_setup: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()
