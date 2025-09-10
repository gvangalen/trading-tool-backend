import logging
from datetime import datetime, timedelta
from fastapi import APIRouter, HTTPException, Request
from backend.utils.db import get_db_connection
from backend.utils.technical_interpreter import process_technical_indicator
from backend.celery_task.technical_task import save_technical_data_task
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


# ✅ AGGREGATIE – week, maand, kwartaal (via timestamp)
def aggregate_data(days: int):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Databaseverbinding mislukt.")

    cutoff = datetime.utcnow() - timedelta(days=days)
    indicators = ['rsi', 'volume', 'ma_200']
    data = {}

    try:
        with conn.cursor() as cur:
            for indicator in indicators:
                cur.execute(f"""
                    SELECT AVG({indicator}) 
                    FROM technical_data
                    WHERE symbol = 'BTC'
                    AND timestamp >= %s
                """, (cutoff,))
                result = cur.fetchone()
                data[indicator] = float(result[0]) if result[0] else None
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Aggregatie mislukt: {e}")
    finally:
        conn.close()

    return {"symbol": "BTC", **data}


@router.get("/api/technical/day")
def get_day_data():
    return aggregate_data(1)


@router.get("/api/technical/week")
def get_week_data():
    return aggregate_data(7)


@router.get("/api/technical/month")
def get_month_data():
    return aggregate_data(30)


@router.get("/api/technical/quarter")
def get_quarter_data():
    return aggregate_data(90)
