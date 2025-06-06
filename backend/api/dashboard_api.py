import logging
from fastapi import APIRouter, HTTPException
from utils.db import get_db_connection
import psycopg2.extras

router = APIRouter(prefix="/api")
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

@router.get("/dashboard")
async def get_dashboard_data():
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="DASH00: Databaseverbinding mislukt.")

    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:

            # ✅ Market
            try:
                cur.execute("""
                    SELECT DISTINCT ON (symbol) symbol, price, volume, change_24h, timestamp
                    FROM market_data
                    WHERE symbol IN ('BTC', 'SOL')
                    ORDER BY symbol, timestamp DESC
                """)
                market_data = cur.fetchall()
                logger.info(f"📈 DASH01: Market data geladen ({len(market_data)} rijen)")
            except Exception as e:
                logger.warning(f"⚠️ DASH01: Market data fout: {e}")
                market_data = []

            # ✅ Technical
            try:
                cur.execute("""
                    SELECT DISTINCT ON (symbol) symbol, rsi, volume, ma_200, timestamp
                    FROM technical_data
                    WHERE symbol IN ('BTC', 'SOL')
                    ORDER BY symbol, timestamp DESC
                """)
                technical_data = cur.fetchall()
                logger.info(f"🧪 DASH02: Technical data geladen ({len(technical_data)} rijen)")
            except Exception as e:
                logger.warning(f"⚠️ DASH02: Technical data fout: {e}")
                technical_data = []

            # ✅ Macro
            try:
                cur.execute("""
                    SELECT DISTINCT ON (name) name, value, trend, interpretation, action, timestamp
                    FROM macro_data
                    ORDER BY name, timestamp DESC
                """)
                macro_data = cur.fetchall()
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
                setups = cur.fetchall()
                logger.info(f"📋 DASH04: Setups geladen ({len(setups)} rijen)")
            except Exception as e:
                logger.warning(f"⚠️ DASH04: Setups fout: {e}")
                setups = []

        return {
            "market_data": [dict(row) for row in market_data],
            "technical_data": [dict(row) for row in technical_data],
            "macro_data": [dict(row) for row in macro_data],
            "setups": [dict(row) for row in setups],
        }

    except Exception as e:
        logger.error(f"❌ DASH05: Dashboard error: {e}")
        raise HTTPException(status_code=500, detail="DASH05: Dashboard data ophalen mislukt.")
    finally:
        conn.close()

# ✅ Healthcheck
@router.get("/health")
async def health_check():
    try:
        conn = get_db_connection()
        if not conn:
            raise HTTPException(status_code=500, detail="HEALTH01: DB-connectie faalt.")
        conn.close()
        return {"status": "ok"}
    except Exception as e:
        logger.error(f"❌ HEALTH02: {e}")
        raise HTTPException(status_code=500, detail="HEALTH02: Interne fout")

# ✅ Laatste AI-advies
@router.get("/trading_advice")
async def get_trading_advice(symbol: str = "BTC"):
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT asset, advice, explanation, timestamp
                FROM trading_advice
                WHERE asset = %s
                ORDER BY timestamp DESC
                LIMIT 1
            """, (symbol,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail=f"Geen advies voor {symbol}.")
            return dict(row)
    except Exception as e:
        logger.error(f"❌ ADVICE01: {e}")
        raise HTTPException(status_code=500, detail="ADVICE01: Ophalen advies mislukt.")
    finally:
        conn.close()

# ✅ Top 5 setups uit strategieën
@router.get("/top_setups")
async def get_top_setups():
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT name, score, timeframe, asset, explanation, timestamp
                FROM strategies
                WHERE score IS NOT NULL
                ORDER BY score DESC
                LIMIT 5
            """)
            rows = cur.fetchall()
            return [dict(row) for row in rows]
    except Exception as e:
        logger.error(f"❌ SETUPS01: {e}")
        raise HTTPException(status_code=500, detail="SETUPS01: Ophalen top setups mislukt.")
    finally:
        conn.close()
