import logging
from datetime import datetime
from fastapi import APIRouter, HTTPException
from backend.utils.db import get_db_connection
from backend.models.technical_model import TechnicalIndicator

router = APIRouter()
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


# ============================================
# 🧩  Hulp-functies
# ============================================
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


def safe_fetchall(cur):
    """Zorgt dat fetchall() nooit None teruggeeft."""
    try:
        rows = cur.fetchall()
        return rows or []
    except Exception as e:
        logger.warning(f"⚠️ safe_fetchall mislukt: {e}")
        return []


# ============================================
# 📊  Routes
# ============================================

@router.get("/technical_data")
async def get_technical_data():
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Databaseverbinding mislukt.")
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT indicator, value, score, advies, uitleg, timestamp
                FROM technical_indicators
                ORDER BY timestamp DESC
                LIMIT 50;
            """)
            rows = safe_fetchall(cur)

        if not rows:
            logger.info("⚠️ Geen technische data gevonden.")
            return []

        return [
            {
                "indicator": r[0],
                "waarde": safe_float(r[1]),
                "score": safe_int(r[2]),
                "advies": r[3],
                "uitleg": r[4],
                "timestamp": r[5].isoformat() if r[5] else None,
            } for r in rows
        ]
    except Exception as e:
        logger.error(f"❌ TECH05: Ophalen mislukt: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@router.post("/technical_data")
async def save_or_activate_technical_data(payload: dict):
    """
    ➕ Voeg een bestaande technische indicator toe of sla data op.
    - Controleert eerst of de indicator bestaat in de `indicators`-tabel.
    """
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="❌ Geen databaseverbinding.")

    indicator = payload.get("indicator")
    value = payload.get("value")
    score = payload.get("score")
    advies = payload.get("advies")
    uitleg = payload.get("uitleg")
    timestamp = payload.get("timestamp")

    if not indicator:
        raise HTTPException(status_code=400, detail="❌ 'indicator' is verplicht in de payload.")

    try:
        with conn.cursor() as cur:
            # ✅ Controleer of indicator bestaat
            cur.execute("""
                SELECT 1 FROM indicators
                WHERE LOWER(name) = LOWER(%s)
                AND category = 'technical'
                AND active = TRUE;
            """, (indicator,))
            if not cur.fetchone():
                raise HTTPException(
                    status_code=404,
                    detail=f"Indicator '{indicator}' bestaat niet in de configuratie."
                )

            # 🟩 Volledige data-insert
            logger.info(f"📥 Technische data opslaan: {indicator} ({value})")
            cur.execute("""
                INSERT INTO technical_indicators (indicator, value, score, advies, uitleg, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s);
            """, (
                indicator,
                value,
                score,
                advies,
                uitleg,
                timestamp or datetime.utcnow(),
            ))
            conn.commit()
            return {"message": f"✅ Technische data voor '{indicator}' opgeslagen."}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ TECH_POST: Fout bij verwerken indicator '{indicator}': {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
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
                SELECT indicator, value, score, advies, uitleg, timestamp
                FROM technical_indicators
                WHERE DATE(timestamp) = CURRENT_DATE
                ORDER BY timestamp DESC;
            """)
            rows = safe_fetchall(cur)

            if not rows:
                cur.execute("""
                    SELECT timestamp FROM technical_indicators
                    ORDER BY timestamp DESC
                    LIMIT 1;
                """)
                fallback = cur.fetchone()
                if not fallback:
                    return []
                fallback_date = fallback[0].date()
                cur.execute("""
                    SELECT indicator, value, score, advies, uitleg, timestamp
                    FROM technical_indicators
                    WHERE DATE(timestamp) = %s
                    ORDER BY timestamp DESC;
                """, (fallback_date,))
                rows = safe_fetchall(cur)

        return [
            {
                "indicator": r[0],
                "waarde": safe_float(r[1]),
                "score": safe_int(r[2]),
                "advies": r[3],
                "uitleg": r[4],
                "timestamp": r[5].isoformat() if r[5] else None,
            } for r in rows
        ]

    except Exception as e:
        logger.error(f"❌ [day] Fout bij ophalen: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


# ✅ WEEK
@router.get("/technical_data/week")
async def get_technical_week_data():
    logger.info("📤 [get/week] Ophalen technical-indicators (laatste 7 dagen)...")
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Databaseverbinding mislukt.")
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT DATE(timestamp) AS dag
                FROM technical_indicators
                ORDER BY dag DESC
                LIMIT 7;
            """)
            dagen = [r[0] for r in cur.fetchall()]

            if not dagen:
                return []

            cur.execute("""
                SELECT indicator, value, score, advies, uitleg, timestamp
                FROM technical_indicators
                WHERE DATE(timestamp) = ANY(%s)
                ORDER BY timestamp DESC;
            """, (dagen,))
            rows = cur.fetchall()

        return [
            {
                "indicator": r[0],
                "waarde": safe_float(r[1]),
                "score": safe_int(r[2]),
                "advies": r[3],
                "uitleg": r[4],
                "timestamp": r[5].isoformat(),
            } for r in rows
        ]
    except Exception as e:
        logger.error(f"❌ [get/week] Databasefout: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


# ✅ MONTH
@router.get("/technical_data/month")
async def get_technical_month_data():
    logger.info("📤 [get/month] Ophalen technical-indicators (4 recente weken)...")
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Databaseverbinding mislukt.")
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT DATE_TRUNC('week', timestamp)::date AS week_start
                FROM technical_indicators
                ORDER BY week_start DESC
                LIMIT 4;
            """)
            weken = [r[0] for r in cur.fetchall()]

            if not weken:
                return []

            cur.execute("""
                SELECT indicator, value, score, advies, uitleg, timestamp
                FROM technical_indicators
                WHERE DATE_TRUNC('week', timestamp)::date = ANY(%s)
                ORDER BY timestamp DESC;
            """, (weken,))
            rows = cur.fetchall()

        return [
            {
                "indicator": r[0],
                "waarde": safe_float(r[1]),
                "score": safe_int(r[2]),
                "advies": r[3],
                "uitleg": r[4],
                "timestamp": r[5].isoformat(),
            } for r in rows
        ]
    except Exception as e:
        logger.error(f"❌ [get/month] Databasefout: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


# ✅ QUARTER
@router.get("/technical_data/quarter")
async def get_technical_quarter_data():
    logger.info("📤 [get/quarter] Ophalen technical-indicators (12 recente weken)...")
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Databaseverbinding mislukt.")
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT DATE_TRUNC('week', timestamp)::date AS week_start
                FROM technical_indicators
                ORDER BY week_start DESC
                LIMIT 12;
            """)
            weken = [r[0] for r in cur.fetchall()]

            if not weken:
                return []

            cur.execute("""
                SELECT indicator, value, score, advies, uitleg, timestamp
                FROM technical_indicators
                WHERE DATE_TRUNC('week', timestamp)::date = ANY(%s)
                ORDER BY timestamp DESC;
            """, (weken,))
            rows = cur.fetchall()

        return [
            {
                "indicator": r[0],
                "waarde": safe_float(r[1]),
                "score": safe_int(r[2]),
                "advies": r[3],
                "uitleg": r[4],
                "timestamp": r[5].isoformat(),
            } for r in rows
        ]
    except Exception as e:
        logger.error(f"❌ [get/quarter] Databasefout: {e}")
        raise HTTPException(status_code=500, detail=str(e))
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

# ✅ Verwijderen van één specifieke technische indicator (case-insensitive)
@router.delete("/technical_data/{indicator}")
async def delete_technical_indicator(indicator: str):
    """
    Verwijdert alle rijen voor één specifieke technische indicator
    (ongeacht hoofd-/kleine letters).
    Gebruikt door het rode kruisje in de frontend.
    """
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Databaseverbinding mislukt.")

    try:
        with conn.cursor() as cur:
            # ▶ Eerst controleren of er iets te verwijderen is
            cur.execute("""
                SELECT COUNT(*) 
                FROM technical_indicators
                WHERE LOWER(indicator) = LOWER(%s);
            """, (indicator,))
            count = cur.fetchone()[0] if cur.fetchone is not None else 0

            if count == 0:
                logger.warning(f"⚠️ Indicator '{indicator}' niet gevonden voor verwijdering.")
                raise HTTPException(
                    status_code=404,
                    detail=f"Indicator '{indicator}' niet gevonden."
                )

            # 🗑 Verwijder alle entries voor deze indicator (volledige opschoning)
            cur.execute("""
                DELETE FROM technical_indicators
                WHERE LOWER(indicator) = LOWER(%s);
            """, (indicator,))
            deleted = cur.rowcount or 0
            conn.commit()

            logger.info(f"🗑️ Indicator '{indicator}' verwijderd ({deleted} rijen).")
            return {
                "message": f"✅ Indicator '{indicator}' succesvol verwijderd.",
                "deleted_rows": deleted
            }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ TECH_DELETE: Fout bij verwijderen van indicator '{indicator}': {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Fout bij verwijderen van indicator '{indicator}'."
        )
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
@router.get("/technical/indicators")
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
                AND category = 'technical'
                ORDER BY name;
            """)
            rows = cur.fetchall()

        return [{"name": r[0], "display_name": r[1]} for r in rows]

    except Exception as e:
        logger.error(f"❌ Fout bij ophalen indicatornamen: {e}")
        raise HTTPException(status_code=500, detail="Fout bij ophalen indicatornamen.")
    finally:
        conn.close()
        
# ✅ 2. Alle scoreregels ophalen per indicator
@router.get("/technical_indicator_rules/{indicator_name}")
async def get_rules_for_indicator(indicator_name: str):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Geen databaseverbinding.")

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, indicator, range_min, range_max, score, trend, interpretation, action
                FROM technical_indicator_rules
                WHERE LOWER(indicator) = LOWER(%s)
                ORDER BY range_min ASC;
            """, (indicator_name,))
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
                "action": r[7],
            } for r in rows
        ]

    except Exception as e:
        logger.error(f"❌ Fout bij ophalen scoreregels voor {indicator_name}: {e}")
        raise HTTPException(status_code=500, detail="Fout bij ophalen scoreregels.")
    finally:
        conn.close()
