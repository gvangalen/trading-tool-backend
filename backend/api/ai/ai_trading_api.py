# ✅ ai_trading_api.py
import logging
from fastapi import APIRouter, HTTPException
from utils.db import get_db_connection
import psycopg2.extras

router = APIRouter()
logger = logging.getLogger(__name__)

# ✅ Tradingadvies ophalen per asset
@router.get("/trading_advice")
async def get_trading_advice(symbol: str = "BTC"):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="❌ DB01: Databaseverbinding mislukt.")

    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT asset, advice, explanation, risk_profile, created_at
                FROM trading_advice
                WHERE asset = %s
                ORDER BY created_at DESC
                LIMIT 1
            """, (symbol.upper(),))
            row = cur.fetchone()

            if not row:
                raise HTTPException(status_code=404, detail=f"⚠️ Geen tradingadvies gevonden voor {symbol}.")

            return dict(row)

    except Exception as e:
        logger.error(f"❌ Fout bij ophalen tradingadvies voor {symbol}: {e}")
        raise HTTPException(status_code=500, detail="❌ Ophalen tradingadvies mislukt.")
    finally:
        conn.close()
