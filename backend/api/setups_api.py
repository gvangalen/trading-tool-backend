from fastapi import APIRouter, HTTPException, Request
from utils.db import get_db_connection

router = APIRouter()

# ✅ Setup toevoegen
@router.post("/setups", status_code=201)
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
        raise HTTPException(status_code=400, detail="Name and timeframe are required.")

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed.")

    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO setups (
                    name, symbol, timeframe, account_type, strategy_type,
                    min_investment, dynamic_investment, score, description, tags, created_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                RETURNING id;
            """, (
                name, symbol, timeframe, account_type, strategy_type,
                min_investment, dynamic, score, description, tags
            ))
            setup_id = cur.fetchone()[0]
            conn.commit()
            return {"message": "Setup succesvol opgeslagen", "id": setup_id}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")
    finally:
        conn.close()

# ✅ Setups ophalen
@router.get("/setups")
async def get_setups(symbol: str = "BTC"):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed.")

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

        setups = []
        for row in rows:
            setups.append({
                "id": row[0],
                "name": row[1],
                "symbol": row[2],
                "timeframe": row[3],
                "account_type": row[4],
                "strategy_type": row[5],
                "min_investment": float(row[6]) if row[6] is not None else None,
                "dynamic": row[7],
                "score": float(row[8]) if row[8] is not None else None,
                "description": row[9],
                "tags": row[10],
                "created_at": row[11].isoformat()
            })

        return setups

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")
    finally:
        conn.close()

# ✅ Setup verwijderen
@router.delete("/setups/{setup_id}")
async def delete_setup(setup_id: int):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed.")

    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM setups WHERE id = %s RETURNING id;", (setup_id,))
            deleted = cur.fetchone()

            if not deleted:
                raise HTTPException(status_code=404, detail="Setup niet gevonden.")

            conn.commit()
            return {"message": f"Setup {setup_id} verwijderd"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")
    finally:
        conn.close()

# ✅ Setup bijwerken
@router.put("/setups/{setup_id}")
async def update_setup(setup_id: int, request: Request):
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
        raise HTTPException(status_code=400, detail="Name en timeframe zijn vereist.")

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Databaseverbinding mislukt.")

    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE setups
                SET name = %s,
                    symbol = %s,
                    timeframe = %s,
                    account_type = %s,
                    strategy_type = %s,
                    min_investment = %s,
                    dynamic_investment = %s,
                    score = %s,
                    description = %s,
                    tags = %s
                WHERE id = %s
                RETURNING id;
            """, (
                name, symbol, timeframe, account_type, strategy_type,
                min_investment, dynamic, score, description, tags, setup_id
            ))
            updated = cur.fetchone()

            if not updated:
                raise HTTPException(status_code=404, detail="Setup niet gevonden.")

            conn.commit()
            return {"message": f"Setup {setup_id} bijgewerkt"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")
    finally:
        conn.close()

# ✅ Top setups ophalen op basis van hoogste score
@router.get("/setups/top")
async def get_top_setups(limit: int = 3):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed.")

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, name, symbol, timeframe, score, created_at
                FROM setups
                WHERE score IS NOT NULL
                ORDER BY score DESC
                LIMIT %s;
            """, (limit,))
            rows = cur.fetchall()

        setups = []
        for row in rows:
            setups.append({
                "id": row[0],
                "name": row[1],
                "symbol": row[2],
                "timeframe": row[3],
                "score": float(row[4]) if row[4] is not None else None,
                "created_at": row[5].isoformat()
            })

        return setups

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Top setups ophalen mislukt: {e}")
    finally:
        conn.close()
