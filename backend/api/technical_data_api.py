import logging
import os
import json
from fastapi import APIRouter, HTTPException, Request
from celery import Celery
from utils.db import get_db_connection
from utils.technical_interpreter import process_technical_indicator

router = APIRouter()

# ✅ Logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ✅ Celery setup
celery = Celery(__name__)
celery.conf.update(
    broker=os.getenv("CELERY_BROKER_URL", "redis://market_dashboard-redis:6379/0"),
    backend=os.getenv("CELERY_RESULT_BACKEND", "redis://market_dashboard-redis:6379/0"),
)

CONFIG_PATH = "technical_indicators_config.json"

# ✅ Config loader
def load_technical_config():
    try:
        with open(CONFIG_PATH, "r") as f:
            return json.load(f)
    except Exception as e:
        logger.warning(f"⚠️ TECH01: Kan config niet laden: {e}")
        return {}  # fallback naar lege config

# ✅ DB save helper
def save_technical_data(symbol, rsi, volume, ma_200, price, rsi_i, vol_i, ma_i):
    conn = get_db_connection()
    if not conn:
        logger.error("❌ TECH02: Geen DB-verbinding.")
        return False
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO technical_data 
                (symbol, rsi, rsi_interpretation, volume, volume_interpretation, ma_200, ma200_interpretation, price, is_updated, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, TRUE, NOW())
            """, (symbol, float(rsi), rsi_i, float(volume), vol_i, float(ma_200), ma_i, float(price)))
            conn.commit()
        logger.info(f"✅ TECH03: Data opgeslagen voor {symbol}")
        return True
    except Exception as e:
        logger.error(f"❌ TECH04: Opslaan DB-fout: {e}")
        return False
    finally:
        conn.close()

# ✅ Celery task
@celery.task(name="save_technical_data_task")
def save_technical_data_task(symbol, rsi, volume, ma_200, price):
    logger.info(f"⚙️ [celery] Start taak voor {symbol}")
    config = load_technical_config()

    rsi_result = process_technical_indicator("rsi", rsi, config.get("rsi", {}))
    volume_result = process_technical_indicator("volume", volume, config.get("volume", {}))
    ma200_result = process_technical_indicator("ma_200", ma_200, config.get("ma_200", {}))

    rsi_i = rsi_result.get("interpretation") if rsi_result else None
    vol_i = volume_result.get("interpretation") if volume_result else None
    ma_i = ma200_result.get("interpretation") if ma200_result else None

    save_technical_data(symbol, rsi, volume, ma_200, price, rsi_i, vol_i, ma_i)

# ✅ Webhook endpoint van TradingView
@router.post("/api/tradingview_webhook")
async def tradingview_webhook(request: Request):
    try:
        data = await request.json()
        logger.info(f"📩 [webhook] Ontvangen: {data}")

        symbol = data.get("symbol", "BTC")
        rsi = data.get("rsi")
        volume = data.get("volume")
        ma_200 = data.get("ma_200")
        price = data.get("price")

        if None in (rsi, volume, ma_200, price):
            raise HTTPException(status_code=400, detail="WEBHOOK01: Incomplete webhook data.")

        save_technical_data_task.delay(symbol, rsi, volume, ma_200, price)

        return {"message": "Webhook ontvangen en verwerkt."}

    except Exception as e:
        logger.error(f"❌ WEBHOOK02: Fout: {e}")
        raise HTTPException(status_code=500, detail=f"WEBHOOK02: {e}")

# ✅ Ophalen laatste technische data
@router.get("/api/technical_data")
async def get_technical_data():
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="TECH05: Databaseverbinding mislukt.")
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, symbol, rsi, rsi_interpretation, volume, volume_interpretation, ma_200, ma200_interpretation, price, timestamp
                FROM technical_data
                ORDER BY timestamp DESC
                LIMIT 20
            """)
            rows = cur.fetchall()

        result = [
            {
                "id": row[0],
                "symbol": row[1],
                "rsi": float(row[2]),
                "rsi_interpretation": row[3],
                "volume": float(row[4]),
                "volume_interpretation": row[5],
                "ma_200": float(row[6]),
                "ma200_interpretation": row[7],
                "price": float(row[8]) if row[8] is not None else None,
                "timestamp": row[9].isoformat() if row[9] else None
            }
            for row in rows
        ]

        logger.info(f"📊 [get] {len(result)} technische records opgehaald")
        return result

    except Exception as e:
        logger.error(f"❌ TECH06: Ophalen mislukt: {e}")
        raise HTTPException(status_code=500, detail=f"TECH06: {e}")
    finally:
        conn.close()
