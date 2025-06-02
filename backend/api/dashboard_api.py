# ✅ dashboard_api.py — FastAPI versie, geoptimaliseerd

from fastapi import APIRouter, HTTPException
from utils.db import get_db_connection
import psycopg2.extras
import logging

router = APIRouter()
logger = logging.getLogger(__name__)

@router.get("/api/dashboard_data")
async def get_dashboard_data():
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed.")

    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # ✅ Market data (laatste per asset)
            cur.execute("""
                SELECT DISTINCT ON (symbol) symbol, price, volume, change_24h, timestamp
                FROM market_data
                WHERE symbol IN ('BTC', 'SOL')
                ORDER BY symbol, timestamp DESC
            """)
            market_data = [dict(row) for row in cur.fetchall()]

            # ✅ Technical data (laatste per asset)
            cur.execute("""
                SELECT DISTINCT ON (symbol) symbol, rsi, volume, ma_200, timestamp
                FROM technical_data
                WHERE symbol IN ('BTC', 'SOL')
                ORDER BY symbol, timestamp DESC
            """)
            technical_data = [dict(row) for row in cur.fetchall()]

            # ✅ Macro data (laatste per indicator)
            cur.execute("""
                SELECT DISTINCT ON (name) name, value, trend, interpretation, action, timestamp
                FROM macro_data
                ORDER BY name, timestamp DESC
            """)
            macro_data = [dict(row) for row in cur.fetchall()]

            # ✅ Setups (laatste status per setup)
            cur.execute("""
                SELECT DISTINCT ON (name) name, status, timestamp
                FROM setups
                ORDER BY name, timestamp DESC
            """)
            setups = [dict(row) for row in cur.fetchall()]

        logger.info("✅ Dashboard data succesvol opgehaald.")
        return {
            "market_data": market_data,
            "technical_data": technical_data,
            "macro_data": macro_data,
            "setups": setups
        }

    except Exception as e:
        logger.error(f"❌ Fout bij ophalen dashboard data: {e}")
        raise HTTPException(status_code=500, detail="Error fetching dashboard data.")
    finally:
        conn.close()
        
