import logging
from datetime import datetime
from fastapi import APIRouter, HTTPException, Request, Query
from backend.utils.db import get_db_connection
from backend.models.technical_model import TechnicalIndicator

router = APIRouter()
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ✅ GET all
@router.get("/technical_data")
async def get_technical_data():
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Databaseverbinding mislukt.")
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT symbol, indicator, value, score, advies, uitleg, timestamp
                FROM technical_indicators
                ORDER BY timestamp DESC
                LIMIT 50;
            """)
            rows = cur.fetchall()

        return [
            {
                "symbol": row[0],
                "indicator": row[1],
                "waarde": float(row[2]),
                "score": int(row[3]),
                "advies": row[4],
                "uitleg": row[5],
                "timestamp": row[6].isoformat(),
            } for row in rows
        ]
    except Exception as e:
        logger.error(f"❌ TECH05: Ophalen mislukt: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

@router.post("/technical_data")
async def save_technical_data(item: TechnicalIndicator):
    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            logger.error("❌ TECH_POST: Geen databaseverbinding.")
            raise HTTPException(status_code=500, detail="❌ Geen databaseverbinding.")

        logger.info(f"📥 TECH_POST: Ontvangen data: {item.dict()}")

        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO technical_indicators (
                    symbol, indicator, value, score, advies, uitleg, timestamp
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s);
            """, (
                item.symbol,
                item.indicator,
                item.value,
                item.score,
                item.advies,
                item.uitleg,
                item.timestamp,
            ))
            conn.commit()

        logger.info(f"✅ TECH_POST: Succesvol opgeslagen: {item.symbol} - {item.indicator}")
        return {"status": "success"}

    except Exception as e:
        import traceback
        logger.error(f"❌ TECH_POST: Fout bij opslaan technische data: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"❌ Fout bij opslaan: {e}")

    finally:
        if conn:
            conn.close()
            
# ✅ Dagdata per indicator
@router.get("/technical_data/day")
async def get_latest_day_data():
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Geen databaseverbinding.")
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT symbol, indicator, value, score, advies, uitleg, timestamp
                FROM technical_indicators
                WHERE symbol = 'BTC' AND DATE(timestamp) = CURRENT_DATE
                ORDER BY timestamp DESC;
            """)
            rows = cur.fetchall()

        return [
            {
                "symbol": row[0],
                "indicator": row[1],
                "waarde": float(row[2]),
                "score": int(row[3]),
                "advies": row[4],
                "uitleg": row[5],
                "timestamp": row[6].isoformat(),
            } for row in rows
        ]
    except Exception as e:
        logger.error(f"❌ [day] Fout bij ophalen: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

# ✅ WEEK
@router.get("/technical_data/week")
async def get_technical_week_data():
    logger.info("📤 [get/week] Ophalen technical-indicators (7 dagen)...")
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Databaseverbinding mislukt.")
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT symbol, indicator, value, score, advies, uitleg, timestamp
                FROM technical_indicators
                WHERE timestamp >= NOW() - INTERVAL '7 days'
                ORDER BY timestamp DESC
                LIMIT 100;
            """)
            rows = cur.fetchall()
            return [
                {
                    "symbol": row[0],
                    "indicator": row[1],
                    "waarde": float(row[2]),
                    "score": int(row[3]),
                    "advies": row[4],
                    "uitleg": row[5],
                    "timestamp": row[6].isoformat(),
                } for row in rows
            ]
    except Exception as e:
        logger.error(f"❌ [get/week] Databasefout: {e}")
        raise HTTPException(status_code=500, detail="❌ [DB02] Ophalen weekdata mislukt.")
    finally:
        conn.close()

# ✅ MONTH
@router.get("/technical_data/month")
async def get_technical_month_data():
    logger.info("📤 [get/month] Ophalen technical-indicators (30 dagen)...")
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Databaseverbinding mislukt.")
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT symbol, indicator, value, score, advies, uitleg, timestamp
                FROM technical_indicators
                WHERE timestamp >= NOW() - INTERVAL '30 days'
                ORDER BY timestamp DESC
                LIMIT 100;
            """)
            rows = cur.fetchall()
            return [
                {
                    "symbol": row[0],
                    "indicator": row[1],
                    "waarde": float(row[2]),
                    "score": int(row[3]),
                    "advies": row[4],
                    "uitleg": row[5],
                    "timestamp": row[6].isoformat(),
                } for row in rows
            ]
    except Exception as e:
        logger.error(f"❌ [get/month] Databasefout: {e}")
        raise HTTPException(status_code=500, detail="❌ [DB03] Ophalen maanddata mislukt.")
    finally:
        conn.close()

# ✅ QUARTER
@router.get("/technical_data/quarter")
async def get_technical_quarter_data():
    logger.info("📤 [get/quarter] Ophalen technical-indicators (90 dagen)...")
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Databaseverbinding mislukt.")
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT symbol, indicator, value, score, advies, uitleg, timestamp
                FROM technical_indicators
                WHERE timestamp >= NOW() - INTERVAL '90 days'
                ORDER BY timestamp DESC
                LIMIT 100;
            """)
            rows = cur.fetchall()
            return [
                {
                    "symbol": row[0],
                    "indicator": row[1],
                    "waarde": float(row[2]),
                    "score": int(row[3]),
                    "advies": row[4],
                    "uitleg": row[5],
                    "timestamp": row[6].isoformat(),
                } for row in rows
            ]
    except Exception as e:
        logger.error(f"❌ [get/quarter] Databasefout: {e}")
        raise HTTPException(status_code=500, detail="❌ [DB04] Ophalen kwartaaldata mislukt.")
    finally:
        conn.close()

# ✅ SYMBOL specifiek
@router.get("/technical_data/{symbol}")
async def get_technical_for_symbol(symbol: str):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Databaseverbinding mislukt.")
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT symbol, indicator, value, score, advies, uitleg, timestamp
                FROM technical_indicators
                WHERE symbol = %s
                ORDER BY timestamp DESC
                LIMIT 50;
            """, (symbol,))
            rows = cur.fetchall()
            return [
                {
                    "symbol": row[0],
                    "indicator": row[1],
                    "waarde": float(row[2]),
                    "score": int(row[3]),
                    "advies": row[4],
                    "uitleg": row[5],
                    "timestamp": row[6].isoformat(),
                } for row in rows
            ]
    except Exception as e:
        logger.error(f"❌ TECH07: Ophalen symbol error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

# ✅ Verwijderen
@router.delete("/technical_data/{symbol}")
async def delete_technical_data(symbol: str):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Databaseverbinding mislukt.")
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM technical_indicators WHERE symbol = %s", (symbol,))
            conn.commit()
        return {"message": f"Technische data voor {symbol} verwijderd."}
    except Exception as e:
        logger.error(f"❌ TECH06: Verwijderen mislukt: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

# ✅ Handmatig triggeren van Celery-task (test)
@router.post("/technical_data/trigger")
async def trigger_technical_task():
    try:
        fetch_technical_data.delay()
        logger.info("🚀 Celery-task 'fetch_technical_data' handmatig gestart.")
        return {"message": "⏳ Celery-task gestart: technische data wordt opgehaald."}
    except Exception as e:
        logger.error(f"❌ Trigger-fout: {e}")
        raise HTTPException(status_code=500, detail="Triggeren van Celery mislukt.")
