import logging
from datetime import datetime
from fastapi import APIRouter, HTTPException
from backend.utils.db import get_db_connection
from backend.models.technical_model import TechnicalIndicator

router = APIRouter()
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ✅ Veilige conversies
def safe_float(value):
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None

def safe_int(value):
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None

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
                "waarde": safe_float(row[2]),
                "score": safe_int(row[3]),
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
            # ✅ Eerst: probeer data van vandaag
            cur.execute("""
                SELECT symbol, indicator, value, score, advies, uitleg, timestamp
                FROM technical_indicators
                WHERE symbol = 'BTC' AND DATE(timestamp) = CURRENT_DATE
                ORDER BY timestamp DESC;
            """)
            rows = cur.fetchall()

            # ⚠️ Fallback: als geen data van vandaag, pak laatste beschikbare dag
            if not rows:
                cur.execute("""
                    SELECT symbol, indicator, value, score, advies, uitleg, timestamp
                    FROM technical_indicators
                    WHERE symbol = 'BTC'
                    ORDER BY timestamp DESC
                    LIMIT 3;
                """)
                fallback_ts = cur.fetchone()[6]  # Pak timestamp van nieuwste
                # Zoek dan alles van diezelfde dag
                cur.execute("""
                    SELECT symbol, indicator, value, score, advies, uitleg, timestamp
                    FROM technical_indicators
                    WHERE symbol = 'BTC' AND DATE(timestamp) = %s
                    ORDER BY timestamp DESC;
                """, (fallback_ts.date(),))
                rows = cur.fetchall()

        return [
            {
                "symbol": row[0],
                "indicator": row[1],
                "waarde": safe_float(row[2]),
                "score": safe_int(row[3]),
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

# ✅ WEEK (laatste 7 unieke dagen met data)
@router.get("/technical_data/week")
async def get_technical_week_data():
    logger.info("📤 [get/week] Ophalen technical-indicators (laatste 7 unieke dagen)...")
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Databaseverbinding mislukt.")
    try:
        with conn.cursor() as cur:

            # 🗓️ Stap 1: haal de laatste 7 unieke datums op waarop data is opgeslagen
            cur.execute("""
                SELECT DISTINCT DATE(timestamp) AS dag
                FROM technical_indicators
                WHERE symbol = 'BTC'
                ORDER BY dag DESC
                LIMIT 7;
            """)
            date_rows = cur.fetchall()
            dagen = [r[0] for r in date_rows]

            if not dagen:
                logger.warning("⚠️ Geen dagen gevonden in de laatste week.")
                return []

            logger.info(f"📅 Geselecteerde dagen voor weekdata: {dagen}")

            # 🧮 Stap 2: haal alle technische data op voor die dagen
            cur.execute("""
                SELECT symbol, indicator, value, score, advies, uitleg, timestamp
                FROM technical_indicators
                WHERE symbol = 'BTC'
                AND DATE(timestamp) = ANY(%s)
                ORDER BY timestamp DESC;
            """, (dagen,))
            rows = cur.fetchall()

        logger.info(f"✅ Weekdata opgehaald: {len(rows)} rijen gevonden.")

        return [
            {
                "symbol": row[0],
                "indicator": row[1],
                "waarde": safe_float(row[2]),
                "score": safe_int(row[3]),
                "advies": row[4],
                "uitleg": row[5],
                "timestamp": row[6].isoformat(),
            } for row in rows
        ]

    except Exception as e:
        logger.error(f"❌ [get/week] Databasefout: {e}")
        raise HTTPException(status_code=500, detail=f"❌ [DB02] Ophalen weekdata mislukt: {e}")
    finally:
        conn.close()

# ✅ MONTH (4 recente weken)
@router.get("/technical_data/month")
async def get_technical_month_data():
    logger.info("📤 [get/month] Ophalen technical-indicators (4 recente weken)...")
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Databaseverbinding mislukt.")
    try:
        with conn.cursor() as cur:
            # 🗓️ Stap 1: haal de 4 meest recente unieke weken op (op basis van eerste dag van de week)
            cur.execute("""
                SELECT DISTINCT DATE_TRUNC('week', timestamp)::date AS week_start
                FROM technical_indicators
                WHERE symbol = 'BTC'
                ORDER BY week_start DESC
                LIMIT 4;
            """)
            week_rows = cur.fetchall()
            weken = [r[0] for r in week_rows]

            if not weken:
                logger.warning("⚠️ Geen weken gevonden in de maanddata.")
                return []

            logger.info(f"📅 Geselecteerde weken: {weken}")

            # 🧮 Stap 2: haal alle technische data op die in die weken valt
            cur.execute("""
                SELECT symbol, indicator, value, score, advies, uitleg, timestamp
                FROM technical_indicators
                WHERE symbol = 'BTC'
                AND DATE_TRUNC('week', timestamp)::date = ANY(%s)
                ORDER BY timestamp DESC;
            """, (weken,))
            rows = cur.fetchall()

        logger.info(f"✅ Maanddata opgehaald: {len(rows)} rijen gevonden.")

        return [
            {
                "symbol": row[0],
                "indicator": row[1],
                "waarde": safe_float(row[2]),
                "score": safe_int(row[3]),
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

# ✅ QUARTER (12 recente weken)
@router.get("/technical_data/quarter")
async def get_technical_quarter_data():
    logger.info("📤 [get/quarter] Ophalen technical-indicators (12 recente weken)...")
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Databaseverbinding mislukt.")
    try:
        with conn.cursor() as cur:
            # 🗓️ Stap 1: haal de 12 meest recente unieke weken op
            cur.execute("""
                SELECT DISTINCT DATE_TRUNC('week', timestamp)::date AS week_start
                FROM technical_indicators
                WHERE symbol = 'BTC'
                ORDER BY week_start DESC
                LIMIT 12;
            """)
            week_rows = cur.fetchall()
            weken = [r[0] for r in week_rows]

            if not weken:
                logger.warning("⚠️ Geen weken gevonden voor kwartaaldata.")
                return []

            logger.info(f"📅 Geselecteerde weken (quarter): {weken}")

            # 🧮 Stap 2: haal alle technische data op voor die weken
            cur.execute("""
                SELECT symbol, indicator, value, score, advies, uitleg, timestamp
                FROM technical_indicators
                WHERE symbol = 'BTC'
                AND DATE_TRUNC('week', timestamp)::date = ANY(%s)
                ORDER BY timestamp DESC;
            """, (weken,))
            rows = cur.fetchall()

        logger.info(f"✅ Kwartaaldata opgehaald: {len(rows)} rijen gevonden.")

        return [
            {
                "symbol": row[0],
                "indicator": row[1],
                "waarde": safe_float(row[2]),
                "score": safe_int(row[3]),
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
                    "waarde": safe_float(row[2]),
                    "score": safe_int(row[3]),
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

# ✅ 1. Alle beschikbare indicatornamen ophalen (voor dropdown)
@router.get("/indicators")
async def get_all_indicators():
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Geen databaseverbinding.")
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT name, display_name
                FROM indicators
                WHERE active = TRUE
                ORDER BY name;
            """)
            rows = cur.fetchall()
        return [{"name": r[0], "display_name": r[1]} for r in rows]
    except Exception as e:
        logger.error(f"❌ Fout bij ophalen indicatornamen: {e}")
        raise HTTPException(status_code=500, detail="Fout bij ophalen indicatornamen.")
    finally:
        conn.close()
# ✅ 2. Alle scoreregels ophalen
@router.get("/technical_indicator_rules")
async def get_all_indicator_rules():
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Geen databaseverbinding.")
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, indicator, range_min, range_max, score, trend, interpretation, action
                FROM technical_indicator_rules
                ORDER BY indicator, score DESC;
            """)
            rows = cur.fetchall()
        return [
            {
                "id": r[0],
                "indicator": r[1],
                "range_min": r[2],
                "range_max": r[3],
                "score": r[4],
                "trend": r[5],
                "interpretation": r[6],
                "action": r[7]
            } for r in rows
        ]
    except Exception as e:
        logger.error(f"❌ Fout bij ophalen regels: {e}")
        raise HTTPException(status_code=500, detail="Fout bij ophalen regels.")
    finally:
        conn.close()


