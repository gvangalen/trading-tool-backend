from fastapi import APIRouter, HTTPException, Request
import json
from utils.db import get_db_connection

router = APIRouter()

# ✅ Setup toevoegen
@router.post("/api/setups", status_code=201)
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
        raise HTTPException(status_code=500, detail=f"Database error: {e}")
    finally:
        conn.close()

# ✅ Setups ophalen
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

        setups = []
        for row in rows:
            conditions = row[3]
            if isinstance(conditions, str):
                try:
                    conditions = json.loads(conditions)
                except json.JSONDecodeError:
                    conditions = {}

            setups.append({
                "id": row[0],
                "symbol": row[1],
                "name": row[2],
                "indicators": conditions.get("indicators"),
                "trend": conditions.get("trend"),
                "timeframe": conditions.get("timeframe"),
                "account_type": conditions.get("account_type"),
                "strategy_type": conditions.get("strategy_type"),
                "min_investment": conditions.get("min_investment"),
                "dynamic": conditions.get("dynamic"),
                "created_at": row[4].isoformat()
            })

        return setups

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")
    finally:
        conn.close()

# ✅ Setup verwijderen
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
        raise HTTPException(status_code=500, detail=f"Database error: {e}")
    finally:
        conn.close()

# ✅ Setup bewerken
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
        raise HTTPException(status_code=500, detail=f"Database error: {e}")
    finally:
        conn.close()
