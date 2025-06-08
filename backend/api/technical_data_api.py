import logging
import os
import json
from fastapi import APIRouter, HTTPException, Request
from celery import Celery
from utils.db import get_db_connection
from utils.technical_interpreter import process_technical_indicator

router = APIRouter()
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

@celery.task(name="save_technical_data_task")
def save_technical_data_task(symbol, rsi, volume, ma_200, timeframe="1D"):
    config = load_technical_config()
    rsi_data = process_technical_indicator("rsi", rsi, config.get("rsi", {}))
    vol_data = process_technical_indicator("volume", volume, config.get("volume", {}))
    ma_data = process_technical_indicator("ma_200", ma_200, config.get("ma_200", {}))

    parts = [rsi_data, vol_data, ma_data]
    scores = [p.get("score", 0) if p else 0 for p in parts]
    total_score = round(sum(scores) / len(scores), 2)

    advies = (
        "Bullish" if total_score >= 70 else
        "Bearish" if total_score <= 30 else
        "Neutraal"
    )

    save_technical_data(symbol, rsi, volume, ma_200, total_score, advies, timeframe)

@router.post("/tradingview_webhook")
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
        return {"message": "Webhook received"}
    except Exception as e:
        logger.error(f"❌ Webhook error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/technical_data")
async def get_technical_data():
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, symbol, rsi, volume, ma_200, score, advies, timeframe, timestamp
                FROM technical_data
                ORDER BY timestamp DESC
                LIMIT 20
            """)
            rows = cur.fetchall()
        return [
            {
                "id": row[0], "symbol": row[1], "rsi": float(row[2]),
                "volume": float(row[3]), "ma_200": float(row[4]),
                "score": float(row[5]), "advies": row[6],
                "timeframe": row[7],
                "timestamp": row[8].isoformat() if row[8] else None
            } for row in rows
        ]
    except Exception as e:
        logger.error(f"❌ TECH06: Fetch failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

@router.post("/technical_data")
async def add_technical_data(request: Request):
    try:
        data = await request.json()
        symbol = data.get("symbol")
        rsi = data.get("rsi")
        volume = data.get("volume")
        ma_200 = data.get("ma_200")
        timeframe = data.get("timeframe", "1D")

        if None in (symbol, rsi, volume, ma_200):
            raise HTTPException(status_code=400, detail="TECH07: Incomplete data.")

        config = load_technical_config()
        rsi_data = process_technical_indicator("rsi", rsi, config.get("rsi", {}))
        vol_data = process_technical_indicator("volume", volume, config.get("volume", {}))
        ma_data = process_technical_indicator("ma_200", ma_200, config.get("ma_200", {}))

        parts = [rsi_data, vol_data, ma_data]
        scores = [p.get("score", 0) if p else 0 for p in parts]
        total_score = round(sum(scores) / len(scores), 2)

        advies = (
            "Bullish" if total_score >= 70 else
            "Bearish" if total_score <= 30 else
            "Neutraal"
        )

        success = save_technical_data(symbol, rsi, volume, ma_200, total_score, advies, timeframe)
        if success:
            return {"message": f"Manually saved data for {symbol}"}
        else:
            raise HTTPException(status_code=500, detail="TECH08: Save failed.")
    except Exception as e:
        logger.error(f"❌ TECH09: Add failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

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

@router.get("/technical_data/{symbol}/{timeframe}")
async def get_data_for_asset_timeframe(symbol: str, timeframe: str):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, symbol, rsi, volume, ma_200, score, advies, timeframe, timestamp
                FROM technical_data
                WHERE symbol = %s AND timeframe = %s
                ORDER BY timestamp DESC
                LIMIT 10
            """, (symbol.upper(), timeframe))
            rows = cur.fetchall()
        return [
            {
                "id": row[0], "symbol": row[1], "rsi": float(row[2]),
                "volume": float(row[3]), "ma_200": float(row[4]),
                "score": float(row[5]), "advies": row[6],
                "timeframe": row[7],
                "timestamp": row[8].isoformat() if row[8] else None
            } for row in rows
        ]
    except Exception as e:
        logger.error(f"❌ TECH15: Filter failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()
