# ✅ setup_api.py — FastAPI version

from fastapi import APIRouter, HTTPException, Request
import json
from db import get_db_connection

router = APIRouter()

# ✅ Create setup
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
        raise HTTPException(status_code=400, detail="Name, trend, indicators and timeframe are required.")

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
        raise HTTPException(status_code=500, detail="Database connection failed.")

    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO setups (symbol, setup_name, conditions, created_at)
                VALUES (%s, %s, %s::jsonb, NOW())
                RETURNING id;
            """, (symbol, setup_name, json.dumps(conditions)))
            setup_id = cur.fetchone()[0]
            conn.commit()

        return {"message": "Setup successfully saved", "id": setup_id}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

# ✅ Fetch setups
@router.get("/api/setups")
async def get_setups(symbol: str = "BTC"):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed.")

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
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

# ✅ Delete setup
@router.delete("/api/setups/{setup_id}")
async def delete_setup(setup_id: int):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed.")

    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM setups WHERE id = %s RETURNING id;", (setup_id,))
            deleted = cur.fetchone()

            if not deleted:
                raise HTTPException(status_code=404, detail="Setup not found.")

            conn.commit()
            return {"message": f"Setup {setup_id} deleted"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

# ✅ Update setup
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
        raise HTTPException(status_code=400, detail="Name, trend, indicators and timeframe are required.")

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
        raise HTTPException(status_code=500, detail="Database connection failed.")

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
                raise HTTPException(status_code=404, detail="Setup not found.")

            conn.commit()
            return {"message": f"Setup {setup_id} updated"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()
