import logging
import json
from fastapi import APIRouter, HTTPException, Request
from utils.db import get_db_connection
from utils.technical_interpreter import process_technical_indicator
from celery_task.technical_task import save_technical_data_task  # ✅ Celery taak importeren

router = APIRouter(prefix="/technical_data")
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
@router.post("/webhook")
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
@router.post("/trigger")
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
@router.get("/")
async def get_technical_data():
    # ... [zelfde als in jouw code]
    pass

# ✅ Toevoegen, verwijderen, per asset ophalen
# ... [alles onderaan blijft ongewijzigd]
