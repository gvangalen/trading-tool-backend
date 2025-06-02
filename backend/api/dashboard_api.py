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
        raise HTTPException(status_code=500, detail="DASH00: Databaseverbinding mislukt.")

    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:

            # ✅ Market data
            try:
                cur.execute("""
                    SELECT DISTINCT ON (symbol) symbol, price, volume, change_24h, timestamp
                    FROM market_data
                    WHERE symbol IN ('BTC', 'SOL')
                    ORDER BY symbol, timestamp DESC
                """)
                market_data = [dict(row) for row in cur.fetchall()]
                logger.info(f"📈 DASH01: Market data geladen ({len(market_data)} rijen)")
            except Exception as e:
                logger.warning(f"⚠️ DASH01: Market data fout: {e}")
                market_data = []

            # ✅ Technical data
            try:
                cur.execute("""
                    SELECT DISTINCT ON (symbol) symbol, rsi, volume, ma_200, timestamp
                    FROM technical_data
                    WHERE symbol IN ('BTC', 'SOL')
                    ORDER BY symbol, timestamp DESC
                """)
                technical_data = [dict(row) for row in cur.fetchall()]
                logger.info(f"🧪 DASH02: Technical data geladen ({len(technical_data)} rijen)")
            except Exception as e:
                logger.warning(f"⚠️ DASH02: Technical data fout: {e}")
                technical_data = []

            # ✅ Macro data
            try:
                cur.execute("""
                    SELECT DISTINCT ON (name) name, value, trend, interpretation, action, timestamp
                    FROM macro_data
                    ORDER BY name, timestamp DESC
                """)
                macro_data = [dict(row) for row in cur.fetchall()]
                logger.info(f"🌍 DASH03: Macro data geladen ({len(macro_data)} rijen)")
            except Exception as e:
                logger.warning(f"⚠️ DASH03: Macro data fout: {e}")
                macro_data = []

            # ✅ Setups
            try:
                cur.execute("""
                    SELECT DISTINCT ON (name) name, status, timestamp
                    FROM setups
                    ORDER BY name, timestamp DESC
                """)
                setups = [dict(row) for row in cur.fetchall()]
                logger.info(f"📋 DASH04: Setups geladen ({len(setups)} rijen)")
            except Exception as e:
                logger.warning(f"⚠️ DASH04: Setups data fout: {e}")
                setups = []

        return {
            "market_data": market_data,
            "technical_data": technical_data,
            "macro_data": macro_data,
            "setups": setups
        }

    except Exception as e:
        logger.error(f"❌ DASH05: Algemeen dashboard-fout: {e}")
        raise HTTPException(status_code=500, detail="DASH05: Fout bij ophalen dashboard data.")
    finally:
        conn.close()
