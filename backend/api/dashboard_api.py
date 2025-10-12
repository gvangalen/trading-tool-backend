import logging
from fastapi import APIRouter, HTTPException
from backend.utils.db import get_db_connection
import psycopg2.extras

router = APIRouter()
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
                    WHERE symbol = 'BTC'
                    ORDER BY symbol, timestamp DESC
                """)
                market_data = [dict(row) for row in cur.fetchall()]
                logger.info(f"📈 DASH01: Market data geladen ({len(market_data)} rijen)")
            except Exception as e:
                logger.warning(f"⚠️ DASH01: Market data fout: {e}")
                market_data = []

            # ✅ Technical data (alle indicatornamen lowercase)
            try:
                cur.execute("""
                    SELECT symbol, LOWER(indicator) AS indicator, value, score, timestamp
                    FROM technical_indicators
                    WHERE symbol = 'BTC'
                    AND LOWER(indicator) IN ('rsi', 'volume', 'ma_200')
                    ORDER BY indicator, timestamp DESC
                """)
                rows = cur.fetchall()
                technical_data = {
                    row["indicator"]: {
                        "value": row["value"],
                        "score": row["score"],
                        "timestamp": row["timestamp"]
                    }
                    for row in rows
                }
                logger.info(f"🧪 DASH02: Technical data geladen ({len(technical_data)} indicatoren)")
            except Exception as e:
                logger.warning(f"⚠️ DASH02: Technical data fout: {e}")
                technical_data = {}

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
                    SELECT DISTINCT ON (name) name, created_at AS timestamp
                    FROM setups
                    ORDER BY name, created_at DESC
                """)
                setups = [dict(row) for row in cur.fetchall()]
                logger.info(f"📋 DASH04: Setups geladen ({len(setups)} rijen)")
            except Exception as e:
                logger.warning(f"⚠️ DASH04: Setups fout: {e}")
                setups = []

        # ✅ Scores berekenen
        macro_score = len(macro_data) * 10 if macro_data else 0

        # 🔧 Nieuwe technische scoreberekening (op basis van score-waarden)
        valid_indicators = ["rsi", "volume", "ma_200"]
        used_scores = [v["score"] for k, v in technical_data.items() if k in valid_indicators]
        technical_score = round(sum(used_scores) / len(used_scores), 2) if used_scores else 0

        setup_score = len(setups) * 10 if setups else 0

        # ✅ Uitleg per score
        macro_explanation = (
            "📊 Gebaseerd op " + ", ".join(d['name'] for d in macro_data)
            if macro_data else "❌ Geen macrodata"
        )

        technical_explanation = (
            "📈 Laatste RSI: " + str(technical_data.get("rsi", {}).get("value", "n.v.t."))
            if technical_data else "❌ Geen technische data"
        )

        setup_explanation = (
            f"🧠 {len(setups)} setups geladen"
            if setups else "❌ Geen setups actief"
        )

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

# ✅ 2. Healthcheck
@router.get("/dashboard/health")
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
@router.get("/dashboard/trading_advice")
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

# ✅ 4. Top setups (voor component)
@router.get("/dashboard/top_setups")
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

# ✅ 5. Setup summary (per unieke naam)
@router.get("/dashboard/setup_summary")
async def get_setup_summary():
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT DISTINCT ON (name) name, created_at AS timestamp
                FROM setups
                ORDER BY name, created_at DESC
            """)
            rows = cur.fetchall()
            return [{"name": row["name"], "timestamp": row["timestamp"].isoformat()} for row in rows]
    except Exception as e:
        logger.warning(f"⚠️ DASH06: Fout bij ophalen setup summary: {e}")
        return []
    finally:
        conn.close()
