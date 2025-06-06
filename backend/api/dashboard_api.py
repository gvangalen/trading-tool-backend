import logging
from fastapi import APIRouter, HTTPException
from utils.db import get_db_connection
import psycopg2.extras

router = APIRouter(prefix="/api")
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ✅ 1. Gecombineerde dashboarddata
@router.get("/dashboard")
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

            # ✅ Setup status
            try:
                cur.execute("""
                    SELECT DISTINCT ON (name) name, status, timestamp
                    FROM setups
                    ORDER BY name, timestamp DESC
                """)
                setups = [dict(row) for row in cur.fetchall()]
                logger.info(f"📋 DASH04: Setups geladen ({len(setups)} rijen)")
            except Exception as e:
                logger.warning(f"⚠️ DASH04: Setups fout: {e}")
                setups = []

        # ✅ Dummy scoreberekening (vervang later door AI/logica)
        macro_score = len(macro_data) * 10 if macro_data else 0
        technical_score = len(technical_data) * 10 if technical_data else 0
        setup_score = len(setups) * 10 if setups else 0

        # ✅ Dummy uitleg per score
        macro_explanation = "📊 Gebaseerd op " + ", ".join(d['name'] for d in macro_data) if macro_data else "❌ Geen macrodata"
        technical_explanation = "📈 Laatste RSI: " + str(technical_data[0]['rsi']) if technical_data else "❌ Geen technische data"
        setup_explanation = f"🧠 {len(setups)} setups geladen" if setups else "❌ Geen setups actief"

        return {
            "market_data": market_data,
            "technical_data": technical_data,
            "macro_data": macro_data,
            "setups": setups,
            "scores": {
                "macro": macro_score,
                "technical": technical_score,
                "setup": setup_score
            },
            "explanation": {
                "macro": macro_explanation,
                "technical": technical_explanation,
                "setup": setup_explanation
            }
        }

    except Exception as e:
        logger.error(f"❌ DASH05: Dashboard error: {e}")
        raise HTTPException(status_code=500, detail="DASH05: Dashboard data ophalen mislukt.")
    finally:
        conn.close()


# ✅ 2. Healthcheck endpoint
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


# ✅ 3. Tradingadvies per asset
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


# ✅ 4. Top setups ophalen (voor TopSetupsMini component)
@router.get("/top_setups")
async def get_top_setups():
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT name, score, timeframe, asset, explanation, timestamp
                FROM strategies
                WHERE data->>'score' IS NOT NULL
                ORDER BY CAST(data->>'score' AS FLOAT) DESC
                LIMIT 5
            """)
            rows = cur.fetchall()
            return [dict(row) for row in rows]
    except Exception as e:
        logger.error(f"❌ SETUPS01: {e}")
        raise HTTPException(status_code=500, detail="SETUPS01: Ophalen top setups mislukt.")
    finally:
        conn.close()
