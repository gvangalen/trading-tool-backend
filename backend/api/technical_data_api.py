import logging
import os
import json
from fastapi import APIRouter, HTTPException, Request
from celery import Celery
from utils.db import get_db_connection
from utils.technical_interpreter import process_technical_indicator

router = APIRouter(prefix="/api")
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

celery = Celery(__name__)
celery.conf.update(
    broker=os.getenv("CELERY_BROKER_URL", "redis://market_dashboard-redis:6379/0"),
    backend=os.getenv("CELERY_RESULT_BACKEND", "redis://market_dashboard-redis:6379/0"),
)

CONFIG_PATH = "technical_indicators_config.json"

def load_technical_config():
    try:
        with open(CONFIG_PATH, "r") as f:
            return json.load(f)
    except Exception as e:
        logger.warning(f"⚠️ TECH01: Config load failed: {e}")
        return {}

def save_technical_data(symbol, rsi, volume, ma_200, price, rsi_i, vol_i, ma_i, timeframe=None):
    conn = get_db_connection()
    if not conn:
        logger.error("❌ TECH02: No DB connection.")
        return False
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO technical_data 
                (symbol, rsi, rsi_interpretation, volume, volume_interpretation, ma_200, ma200_interpretation, price, timeframe, is_updated, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, TRUE, NOW())
            """, (symbol, float(rsi), rsi_i, float(volume), vol_i, float(ma_200), ma_i, float(price), timeframe))
            conn.commit()
        logger.info(f"✅ TECH03: Saved data for {symbol}")
        return True
    except Exception as e:
        logger.error(f"❌ TECH04: Save error: {e}")
        return False
    finally:
        conn.close()

@celery.task(name="save_technical_data_task")
def save_technical_data_task(symbol, rsi, volume, ma_200, price):
    config = load_technical_config()
    rsi_i = process_technical_indicator("rsi", rsi, config.get("rsi", {})).get("interpretation")
    vol_i = process_technical_indicator("volume", volume, config.get("volume", {})).get("interpretation")
    ma_i = process_technical_indicator("ma_200", ma_200, config.get("ma_200", {})).get("interpretation")
    save_technical_data(symbol, rsi, volume, ma_200, price, rsi_i, vol_i, ma_i)

# ✅ Webhook endpoint
@router.post("/tradingview_webhook")
async def tradingview_webhook(request: Request):
    try:
        data = await request.json()
        symbol = data.get("symbol", "BTC")
        rsi = data.get("rsi")
        volume = data.get("volume")
        ma_200 = data.get("ma_200")
        price = data.get("price")
        if None in (rsi, volume, ma_200, price):
            raise HTTPException(status_code=400, detail="Incomplete webhook data.")
        save_technical_data_task.delay(symbol, rsi, volume, ma_200, price)
        return {"message": "Webhook received"}
    except Exception as e:
        logger.error(f"❌ Webhook error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ✅ GET: laatste technische data
@router.get("/technical_data")
async def get_technical_data():
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, symbol, rsi, rsi_interpretation, volume, volume_interpretation,
                       ma_200, ma200_interpretation, price, timeframe, timestamp
                FROM technical_data
                ORDER BY timestamp DESC
                LIMIT 20
            """)
            rows = cur.fetchall()
        return [
            {
                "id": row[0], "symbol": row[1], "rsi": float(row[2]), "rsi_interpretation": row[3],
                "volume": float(row[4]), "volume_interpretation": row[5],
                "ma_200": float(row[6]), "ma200_interpretation": row[7],
                "price": float(row[8]), "timeframe": row[9],
                "timestamp": row[10].isoformat() if row[10] else None
            } for row in rows
        ]
    except Exception as e:
        logger.error(f"❌ TECH06: Fetch failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

# ✅ POST: handmatig toevoegen
@router.post("/technical_data")
async def add_technical_data(request: Request):
    try:
        data = await request.json()
        symbol = data.get("symbol")
        rsi = data.get("rsi")
        volume = data.get("volume")
        ma_200 = data.get("ma_200")
        price = data.get("price")
        timeframe = data.get("timeframe", "1D")

        if None in (symbol, rsi, volume, ma_200, price):
            raise HTTPException(status_code=400, detail="TECH07: Incomplete data.")

        config = load_technical_config()
        rsi_i = process_technical_indicator("rsi", rsi, config.get("rsi", {})).get("interpretation")
        vol_i = process_technical_indicator("volume", volume, config.get("volume", {})).get("interpretation")
        ma_i = process_technical_indicator("ma_200", ma_200, config.get("ma_200", {})).get("interpretation")

        success = save_technical_data(symbol, rsi, volume, ma_200, price, rsi_i, vol_i, ma_i, timeframe)
        if success:
            return {"message": f"Manually saved data for {symbol}"}
        else:
            raise HTTPException(status_code=500, detail="TECH08: Save failed.")

    except Exception as e:
        logger.error(f"❌ TECH09: Add failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ✅ DELETE
@router.delete("/technical_data/{id}")
async def delete_technical_data(id: int):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM technical_data WHERE id = %s", (id,))
            if cur.rowcount == 0:
                raise HTTPException(status_code=404, detail="TECH11: ID not found.")
            conn.commit()
            return {"message": f"Deleted record {id}"}
    except Exception as e:
        logger.error(f"❌ TECH13: Delete failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

# ✅ GET: filteren op asset + timeframe
@router.get("/technical_data/{symbol}/{timeframe}")
async def get_data_for_asset_timeframe(symbol: str, timeframe: str):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, symbol, rsi, rsi_interpretation, volume, volume_interpretation,
                       ma_200, ma200_interpretation, price, timeframe, timestamp
                FROM technical_data
                WHERE symbol = %s AND timeframe = %s
                ORDER BY timestamp DESC
                LIMIT 10
            """, (symbol.upper(), timeframe))
            rows = cur.fetchall()
        return [
            {
                "id": row[0], "symbol": row[1], "rsi": float(row[2]), "rsi_interpretation": row[3],
                "volume": float(row[4]), "volume_interpretation": row[5],
                "ma_200": float(row[6]), "ma200_interpretation": row[7],
                "price": float(row[8]), "timeframe": row[9],
                "timestamp": row[10].isoformat() if row[10] else None
            } for row in rows
        ]
    except Exception as e:
        logger.error(f"❌ TECH15: Filter failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()
