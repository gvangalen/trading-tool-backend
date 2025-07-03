from fastapi import APIRouter, HTTPException, Request
from utils.db import get_db_connection
from datetime import datetime

router = APIRouter(prefix="/setups")

# ✅ Nieuwe setup opslaan
@router.post("/", status_code=201)
async def save_setup(request: Request):
    """
    Voeg een nieuwe setup toe aan de database.
    """
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
        raise HTTPException(status_code=500, detail="Databaseverbinding mislukt.")

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
        raise HTTPException(status_code=500, detail=f"❌ Fout bij opslaan setup: {e}")
    finally:
        conn.close()

# ✅ Setup-lijst ophalen (default = BTC)
@router.get("/")
async def get_setups(symbol: str = "BTC"):
    """
    Haal de laatste 50 setups op voor een specifieke asset (default: BTC).
    """
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Databaseverbinding mislukt.")

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, name, symbol, timeframe, account_type, strategy_type,
                       min_investment, dynamic_investment, score, description, tags, created_at
