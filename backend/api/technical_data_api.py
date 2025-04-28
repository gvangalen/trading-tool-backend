# technical_data_api.py
import logging
import os
from fastapi import APIRouter, HTTPException, Request
from db import get_db_connection
from celery import Celery
from datetime import datetime

router = APIRouter()

# ✅ Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ✅ Celery setup
celery = Celery(__name__)
celery.conf.update(
    broker=os.getenv("CELERY_BROKER_URL", "redis://market_dashboard-redis:6379/0"),
    backend=os.getenv("CELERY_RESULT_BACKEND", "redis://market_dashboard-redis:6379/0"),
)

# ✅ Data opslaan in database
def save_technical_data(symbol, rsi, volume, ma_200, price):
    conn = get_db_connection()
    if not conn:
        logger.error("❌ Databaseverbinding faalde")
        return False

    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO technical_data (symbol, rsi, volume, ma_200, price, is_updated, timestamp)
                VALUES (%s, %s, %s, %s, %s, TRUE, NOW())
            """, (symbol, float(rsi), float(volume), float(ma_200), float(price)))
            conn.commit()
        logger.info(f"✅ Technische data opgeslagen voor {symbol}")
        return True
    except Exception as e:
        logger.error(f"❌ Databasefout: {e}")
        return False
    finally:
        conn.close()

# ✅ Celery task
@celery.task(name="save_technical_data_task")
def save_technical_data_task(symbol, rsi, volume, ma_200, price):
    logger.info(f"📡 Celery taak gestart voor {symbol}")
    success = save_technical_data(symbol, rsi, volume, ma_200, price)
    if success:
        logger.info(f"✅ Celery taak voltooid voor {symbol}")
    else:
        logger.error(f"❌ Celery taak mislukt voor {symbol}")

# ✅ Webhook endpoint van TradingView
@router.post("/api/tradingview_webhook")
async def tradingview_webhook(request: Request):
    try:
        data = await request.json()
        logger.info(f"📩 Webhook ontvangen: {data}")

        symbol = data.get("symbol", "BTC")
        rsi = data.get("rsi")
        volume = data.get("volume")
        ma_200 = data.get("ma_200")
        price = data.get("price")

        if None in (rsi, volume, ma_200, price):
            raise HTTPException(status_code=400, detail="Incomplete webhook data")

        save_technical_data_task.delay(symbol, rsi, volume, ma_200, price)

        return {"message": "Webhook succesvol ontvangen en verwerkt"}
    except Exception as e:
        logger.error(f"❌ Webhook verwerkingsfout: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ✅ Endpoint: Technische data ophalen
@router.get("/api/technical_data")
async def get_technical_data():
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Geen databaseverbinding")

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, symbol, rsi, volume, ma_200, price, timestamp
                FROM technical_data
                ORDER BY timestamp DESC
                LIMIT 10
            """)
            rows = cur.fetchall()

        data = [
            {
                "id": row[0],
                "symbol": row[1],
                "rsi": float(row[2]),
                "volume": float(row[3]),
                "ma_200": float(row[4]),
                "price": float(row[5]) if row[5] is not None else None,
                "timestamp": row[6].isoformat() if row[6] else None
            }
            for row in rows
        ]

        logger.info(f"📊 {len(data)} technische records opgehaald")
        return data
    except Exception as e:
        logger.error(f"❌ Fout bij ophalen technische data: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()
