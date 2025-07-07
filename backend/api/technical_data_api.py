import logging
import json
from datetime import datetime
from fastapi import APIRouter, HTTPException, Request
from backend.utils.db import get_db_connection
from backend.utils.technical_interpreter import process_technical_indicator
from backend.celery_task.technical_task import save_technical_data_task

router = APIRouter()
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

CONFIG_PATH = "technical_indicators_config.json"


def load_technical_config():
    try:
        with open(CONFIG_PATH, "r") as f:
            return json.load(f)
    except Exception as e:
        logger.warning(f"⚠️ TECH01: Config load failed: {e}")
        return {}


def save_technical_data(symbol, rsi, volume, ma_200, score, advies, timeframe="1D"):
    conn = get_db_connection()
    if not conn:
        logger.error("❌ TECH02: No DB connection.")
        return False
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO technical_data 
                (symbol, rsi, volume, ma_200, score, advies, timeframe, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
            """, (symbol, float(rsi), float(volume), float(ma_200), score, advies, timeframe))
            conn.commit()
        logger.info(f"✅ TECH03: Saved data for {symbol}")
        return True
    except Exception as e:
        logger.error(f"❌ TECH04: Save error: {e}")
        return False
    finally:
        conn.close()


# ✅ Webhook endpoint vanuit TradingView
@router.post("/technical_data/webhook")
async def tradingview_webhook(request: Request):
    try:
        data = await request.json()
        symbol = data.get("symbol", "BTC")
        rsi = data.get("rsi")
        volume = data.get("volume")
        ma_200 = data.get("ma_200")
        timeframe = data.get("timeframe", "1D")

        if None in (rsi, volume, ma_200):
            raise HTTPException(status_code=400, detail="Incomplete webhook data.")

        save_technical_data_task.delay(symbol, rsi, volume, ma_200, timeframe)
        return {"message": "Webhook ontvangen en taak gestart."}
    except Exception as e:
        logger.error(f"❌ Webhook error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ✅ Handmatige trigger via dashboard
@router.post("/technical_data/trigger")
async def trigger_technical_task(request: Request):
    try:
        data = await request.json()
        symbol = data.get("symbol", "BTC")
        rsi = data.get("rsi")
        volume = data.get("volume")
        ma_200 = data.get("ma_200")
        timeframe = data.get("timeframe", "1D")

        if None in (rsi, volume, ma_200):
            raise HTTPException(status_code=400, detail="Incomplete trigger data.")

        save_technical_data_task.delay(symbol, rsi, volume, ma_200, timeframe)
        return {"message": f"📡 Celery-task gestart voor {symbol}"}
    except Exception as e:
        logger.error(f"❌ Trigger error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ✅ Meest recente technische data ophalen
@router.get("/technical_data")
async def get_technical_data():
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Databaseverbinding mislukt.")

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT symbol, rsi, volume, ma_200, score, advies, timeframe, timestamp
                FROM technical_data
                ORDER BY timestamp DESC
                LIMIT 50;
            """)
            rows = cur.fetchall()

        result = []
        for row in rows:
            result.append({
                "symbol": row[0],
                "rsi": float(row[1]),
                "volume": float(row[2]),
                "ma_200": float(row[3]),
                "score": float(row[4]) if row[4] is not None else None,
                "advies": row[5],
                "timeframe": row[6],
                "timestamp": row[7].isoformat()
            })

        return result

    except Exception as e:
        logger.error(f"❌ TECH05: Fetch error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


# ✅ Alias route voor frontend
@router.get("/technical_data/list")
async def get_technical_data_list():
    return await get_technical_data()


# ✅ Technische data per asset ophalen
@router.get("/technical_data/{symbol}")
async def get_technical_for_symbol(symbol: str):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Databaseverbinding mislukt.")

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT symbol, rsi, volume, ma_200, score, advies, timeframe, timestamp
                FROM technical_data
                WHERE symbol = %s
                ORDER BY timestamp DESC
                LIMIT 25;
            """, (symbol,))
            rows = cur.fetchall()

        result = []
        for row in rows:
            result.append({
                "symbol": row[0],
                "rsi": float(row[1]),
                "volume": float(row[2]),
                "ma_200": float(row[3]),
                "score": float(row[4]) if row[4] is not None else None,
                "advies": row[5],
                "timeframe": row[6],
                "timestamp": row[7].isoformat()
            })

        return result

    except Exception as e:
        logger.error(f"❌ TECH07: Fetch symbol error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


# ✅ Verwijder technische data (optioneel per symbool)
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
        logger.error(f"❌ TECH06: Delete error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()
