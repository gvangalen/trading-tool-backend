import logging
from datetime import datetime, timedelta
from fastapi import APIRouter, HTTPException, Request, Query
from backend.utils.db import get_db_connection
from backend.utils.technical_interpreter import process_technical_indicator
from backend.config.config_loader import load_technical_config

router = APIRouter()
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ✅ Config laden bij opstart
try:
    TECHNICAL_CONFIG = load_technical_config()
    logger.info("🚀 technical_data_api.py geladen – alle technische routes zijn actief.")
except Exception as e:
    TECHNICAL_CONFIG = {}
    logger.error(f"❌ [INIT] Laden TECHNICAL_CONFIG mislukt: {e}")


# ✅ Opslaan van technische data
def save_technical_data(symbol, rsi, volume, ma_200, score, advies):
    conn = get_db_connection()
    if not conn:
        logger.error("❌ TECH02: Geen databaseverbinding.")
        return False
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO technical_data 
                (symbol, rsi, volume, ma_200, score, advies, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, NOW())
            """, (symbol, float(rsi), float(volume), float(ma_200), score, advies))
            conn.commit()
        logger.info(f"✅ TECH03: Data opgeslagen voor {symbol}")
        return True
    except Exception as e:
        logger.error(f"❌ TECH04: Opslagfout: {e}")
        return False
    finally:
        conn.close()


# ✅ POST vanaf TradingView webhook
@router.post("/technical_data/webhook")
async def tradingview_webhook(request: Request):
    try:
        data = await request.json()
        symbol = data.get("symbol", "BTC")
        rsi = data.get("rsi")
        volume = data.get("volume")
        ma_200 = data.get("ma_200")

        if None in (rsi, volume, ma_200):
            raise HTTPException(status_code=400, detail="Incomplete webhook data.")

        save_technical_data_task.delay(symbol, rsi, volume, ma_200)
        return {"message": "Webhook ontvangen en taak gestart."}
    except Exception as e:
        logger.error(f"❌ Webhook error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ✅ Handmatige trigger
@router.post("/technical_data/trigger")
async def trigger_technical_task(request: Request):
    try:
        data = await request.json()
        symbol = data.get("symbol", "BTC")
        rsi = data.get("rsi")
        volume = data.get("volume")
        ma_200 = data.get("ma_200")

        if None in (rsi, volume, ma_200):
            raise HTTPException(status_code=400, detail="Incomplete trigger data.")

        save_technical_data_task.delay(symbol, rsi, volume, ma_200)
        return {"message": f"📡 Celery-task gestart voor {symbol}"}
    except Exception as e:
        logger.error(f"❌ Trigger error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ✅ POST: directe opslag met interpretatie
@router.post("/technical_data")
async def save_technical_data_post(request: Request):
    try:
        data = await request.json()
        symbol = data.get("symbol")
        rsi = data.get("rsi")
        volume = data.get("volume")
        ma_200 = data.get("ma_200")

        if None in (symbol, rsi, volume, ma_200):
            raise HTTPException(status_code=400, detail="Verplichte velden ontbreken.")

        try:
            rsi = float(rsi)
            volume = float(volume)
            ma_200 = float(ma_200)
        except ValueError:
            raise HTTPException(status_code=400, detail="RSI, volume en MA-200 moeten numeriek zijn.")

        score, advies = process_technical_indicator(symbol, rsi, volume, ma_200)
        save_technical_data(symbol, rsi, volume, ma_200, score, advies)

        return {"message": "Technische data opgeslagen", "symbol": symbol}
    except Exception as e:
        logger.error(f"❌ TECH08: Save via POST failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ✅ GET: laatste technische data
@router.get("/technical_data")
async def get_technical_data():
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Databaseverbinding mislukt.")
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT symbol, rsi, volume, ma_200, score, advies, timestamp
                FROM technical_data
                ORDER BY timestamp DESC
                LIMIT 50;
            """)
            rows = cur.fetchall()

        return [
            {
                "symbol": row[0],
                "rsi": float(row[1]),
                "volume": float(row[2]),
                "ma_200": float(row[3]),
                "score": float(row[4]) if row[4] is not None else None,
                "advies": row[5],
                "timestamp": row[6].isoformat()
            } for row in rows
        ]
    except Exception as e:
        logger.error(f"❌ TECH05: Ophalen mislukt: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@router.get("/technical_data/{symbol}")
async def get_technical_for_symbol(symbol: str):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Databaseverbinding mislukt.")
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT symbol, rsi, volume, ma_200, score, advies, timestamp
                FROM technical_data
                WHERE symbol = %s
                ORDER BY timestamp DESC
                LIMIT 25;
            """, (symbol,))
            rows = cur.fetchall()

        return [
            {
                "symbol": row[0],
                "rsi": float(row[1]),
                "volume": float(row[2]),
                "ma_200": float(row[3]),
                "score": float(row[4]) if row[4] is not None else None,
                "advies": row[5],
                "timestamp": row[6].isoformat()
            } for row in rows
        ]
    except Exception as e:
        logger.error(f"❌ TECH07: Ophalen symbol error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@router.delete("/technical_data/{symbol}")
async def delete_technical_data(symbol: str):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Databaseverbinding mislukt.")
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM technical_data WHERE symbol = %s", (symbol,))
            conn.commit()
            return {"message": f"Technische data voor {symbol} verwijderd."}
    except Exception as e:
        logger.error(f"❌ TECH06: Verwijderen mislukt: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@router.get("/technical_data/day")
def get_technical_data_day():
    return [
        {
            "symbol": "BTC",
            "indicator": "RSI",
            "waarde": 45.0,
            "score": 1,
            "advies": "🟢 Bullish",
            "uitleg": "RSI is onder 50 en stijgend.",
            "timestamp": datetime.utcnow().isoformat()
        },
        {
            "symbol": "BTC",
            "indicator": "Volume",
            "waarde": 900000000,
            "score": 0,
            "advies": "⚖️ Neutraal",
            "uitleg": "Volume ligt rond het gemiddelde.",
            "timestamp": datetime.utcnow().isoformat()
        },
        {
            "symbol": "BTC",
            "indicator": "200MA",
            "waarde": "Boven MA",
            "score": 1,
            "advies": "🟢 Bullish",
            "uitleg": "Prijs ligt boven 200MA.",
            "timestamp": datetime.utcnow().isoformat()
        }
    ]
    
# ✅ WEEK
@router.get("/technical_data/week")
async def get_technical_week_data():
    logger.info("📤 [get/week] Ophalen technical-data (laatste 7 dagen)...")
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Databaseverbinding mislukt.")
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT symbol, rsi, volume, ma_200, score, advies, timestamp
            FROM technical_data
            WHERE timestamp >= NOW() - INTERVAL '7 days'
            ORDER BY timestamp DESC
            LIMIT 50;
        """)
        rows = cur.fetchall()
        return [
            {
                "symbol": row[0],
                "rsi": float(row[1]) if row[1] is not None else None,
                "volume": float(row[2]) if row[2] is not None else None,
                "ma_200": float(row[3]) if row[3] is not None else None,
                "score": float(row[4]) if row[4] is not None else None,
                "advies": row[5],
                "timestamp": row[6].isoformat() if row[6] else None
            }
            for row in rows
        ]
    except Exception as e:
        logger.error(f"❌ [get/week] Databasefout: {e}")
        raise HTTPException(status_code=500, detail="❌ [DB02] Ophalen weekdata mislukt.")
    finally:
        conn.close()


# ✅ MONTH
@router.get("/technical_data/month")
async def get_technical_month_data():
    logger.info("📤 [get/month] Ophalen technical-data (laatste 30 dagen)...")
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Databaseverbinding mislukt.")
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT symbol, rsi, volume, ma_200, score, advies, timestamp
            FROM technical_data
            WHERE timestamp >= NOW() - INTERVAL '30 days'
            ORDER BY timestamp DESC
            LIMIT 50;
        """)
        rows = cur.fetchall()
        return [
            {
                "symbol": row[0],
                "rsi": float(row[1]) if row[1] is not None else None,
                "volume": float(row[2]) if row[2] is not None else None,
                "ma_200": float(row[3]) if row[3] is not None else None,
                "score": float(row[4]) if row[4] is not None else None,
                "advies": row[5],
                "timestamp": row[6].isoformat() if row[6] else None
            }
            for row in rows
        ]
    except Exception as e:
        logger.error(f"❌ [get/month] Databasefout: {e}")
        raise HTTPException(status_code=500, detail="❌ [DB03] Ophalen maanddata mislukt.")
    finally:
        conn.close()


# ✅ QUARTER
@router.get("/technical_data/quarter")
async def get_technical_quarter_data():
    logger.info("📤 [get/quarter] Ophalen technical-data (laatste 90 dagen)...")
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Databaseverbinding mislukt.")
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT symbol, rsi, volume, ma_200, score, advies, timestamp
            FROM technical_data
            WHERE timestamp >= NOW() - INTERVAL '90 days'
            ORDER BY timestamp DESC
            LIMIT 50;
        """)
        rows = cur.fetchall()
        return [
            {
                "symbol": row[0],
                "rsi": float(row[1]) if row[1] is not None else None,
                "volume": float(row[2]) if row[2] is not None else None,
                "ma_200": float(row[3]) if row[3] is not None else None,
                "score": float(row[4]) if row[4] is not None else None,
                "advies": row[5],
                "timestamp": row[6].isoformat() if row[6] else None
            }
            for row in rows
        ]
    except Exception as e:
        logger.error(f"❌ [get/quarter] Databasefout: {e}")
        raise HTTPException(status_code=500, detail="❌ [DB04] Ophalen kwartaaldata mislukt.")
    finally:
        conn.close()
