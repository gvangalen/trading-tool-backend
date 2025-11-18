import logging
from datetime import datetime
from fastapi import APIRouter, HTTPException
from backend.utils.db import get_db_connection

router = APIRouter()
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


# =========================================================
# 🧩 SAFE HELPERS — voorkomen ALLE future crashes
# =========================================================
def safe_float(value):
    """Convert any value to float safely. NULL → 0.0"""
    try:
        if value is None:
            return 0.0
        return float(value)
    except Exception:
        return 0.0


def safe_int(value):
    """Convert any value to int safely. NULL → 0"""
    try:
        if value is None:
            return 0
        return int(value)
    except Exception:
        return 0


def safe_fetchall(cur):
    """Fetchall dat nooit crasht en nooit None teruggeeft."""
    try:
        rows = cur.fetchall()
        return rows or []
    except Exception as e:
        logger.warning(f"⚠️ safe_fetchall issue: {e}")
        return []


# =========================================================
# 📊 GET ALL TECHNICAL DATA (limit 50)
# =========================================================
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

        return [
            {
                "indicator": r[0],
                "waarde": safe_float(r[1]),
                "score": safe_int(r[2]),
                "advies": r[3],
                "uitleg": r[4],
                "timestamp": r[5].isoformat() if r[5] else None,
            }
            for r in rows
        ]

    except Exception as e:
        logger.error(f"❌ TECH05: Ophalen mislukt: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


# =========================================================
# ➕ POST — ADD NEW TECHNICAL INDICATOR ENTRY
# =========================================================
@router.post("/technical_data")
async def save_or_activate_technical_data(payload: dict):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Geen databaseverbinding.")

    indicator = payload.get("indicator")
    value = payload.get("value")
    score = payload.get("score")
    advies = payload.get("advies")
    uitleg = payload.get("uitleg")
    timestamp = payload.get("timestamp")

    if not indicator:
        raise HTTPException(status_code=400, detail="❌ 'indicator' is verplicht.")

    try:
        with conn.cursor() as cur:
            # controlleer of indicator bekend is
            cur.execute("""
                SELECT 1 FROM indicators
                WHERE LOWER(name) = LOWER(%s)
                AND category='technical'
                AND active = TRUE;
            """, (indicator,))

            if not cur.fetchone():
                raise HTTPException(
                    status_code=404,
                    detail=f"Indicator '{indicator}' bestaat niet of staat niet actief."
                )

            # insert — nooit meer NULL → altijd safe
            cur.execute("""
                INSERT INTO technical_indicators (indicator, value, score, advies, uitleg, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s);
            """, (
                indicator,
                safe_float(value),
                safe_int(score),
                advies,
                uitleg,
                timestamp or datetime.utcnow(),
            ))

            conn.commit()
            return {"message": f"✅ Technische data voor '{indicator}' opgeslagen."}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ TECH_POST: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


# =========================================================
# 📅 DAY
# =========================================================
@router.get("/technical_data/day")
async def get_latest_day_data():
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Geen databaseverbinding.")
    try:
        with conn.cursor() as cur:
            # probeer huidige dag
            cur.execute("""
                SELECT indicator, value, score, advies, uitleg, timestamp
                FROM technical_indicators
                WHERE DATE(timestamp) = CURRENT_DATE
                ORDER BY timestamp DESC;
            """)
            rows = safe_fetchall(cur)

            # fallback
            if not rows:
                cur.execute("""
                    SELECT timestamp FROM technical_indicators
                    ORDER BY timestamp DESC LIMIT 1;
                """)
                last = cur.fetchone()
                if last:
                    fallback_date = last[0].date()
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
            }
            for r in rows
        ]

    finally:
        conn.close()


# =========================================================
# ⏳ WEEK (7 dagen)
# =========================================================
@router.get("/technical_data/week")
async def get_technical_week_data():
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
            dagen = [r[0] for r in safe_fetchall(cur)]

            cur.execute("""
                SELECT indicator, value, score, advies, uitleg, timestamp
                FROM technical_indicators
                WHERE DATE(timestamp) = ANY(%s)
                ORDER BY timestamp DESC;
            """, (dagen,))
            rows = safe_fetchall(cur)

        return [
            {
                "indicator": r[0],
                "waarde": safe_float(r[1]),
                "score": safe_int(r[2]),
                "advies": r[3],
                "uitleg": r[4],
                "timestamp": r[5].isoformat(),
            }
            for r in rows
        ]

    finally:
        conn.close()


# =========================================================
# 📅 MONTH (4 weken)
# =========================================================
@router.get("/technical_data/month")
async def get_technical_month_data():
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Databaseverbinding mislukt.")
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT DATE_TRUNC('week', timestamp)::date
                FROM technical_indicators
                ORDER BY 1 DESC
                LIMIT 4;
            """)
            weken = [r[0] for r in safe_fetchall(cur)]

            cur.execute("""
                SELECT indicator, value, score, advies, uitleg, timestamp
                FROM technical_indicators
                WHERE DATE_TRUNC('week', timestamp)::date = ANY(%s)
                ORDER BY timestamp DESC;
            """, (weken,))
            rows = safe_fetchall(cur)

        return [
            {
                "indicator": r[0],
                "waarde": safe_float(r[1]),
                "score": safe_int(r[2]),
                "advies": r[3],
                "uitleg": r[4],
                "timestamp": r[5].isoformat(),
            }
            for r in rows
        ]

    finally:
        conn.close()


# =========================================================
# 🗓 QUARTER (12 weken)
# =========================================================
@router.get("/technical_data/quarter")
async def get_technical_quarter_data():
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Databaseverbinding mislukt.")
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT DATE_TRUNC('week', timestamp)::date
                FROM technical_indicators
                ORDER BY 1 DESC
                LIMIT 12;
            """)
            weken = [r[0] for r in safe_fetchall(cur)]

            cur.execute("""
                SELECT indicator, value, score, advies, uitleg, timestamp
                FROM technical_indicators
                WHERE DATE_TRUNC('week', timestamp)::date = ANY(%s)
                ORDER BY timestamp DESC;
            """, (weken,))
            rows = safe_fetchall(cur)

        return [
            {
                "indicator": r[0],
                "waarde": safe_float(r[1]),
                "score": safe_int(r[2]),
                "advies": r[3],
                "uitleg": r[4],
                "timestamp": r[5].isoformat(),
            }
            for r in rows
        ]

    finally:
        conn.close()

# =========================================================
# ❌ DELETE INDICATOR
# =========================================================
@router.delete("/technical_data/{indicator}")
async def delete_technical_indicator(indicator: str):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Databaseverbinding mislukt.")

    try:
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM technical_indicators
                WHERE LOWER(indicator) = LOWER(%s);
            """, (indicator,))
            deleted = cur.rowcount or 0
            conn.commit()

        return {
            "message": f"🗑 Indicator '{indicator}' verwijderd.",
            "deleted_rows": deleted,
        }

    finally:
        conn.close()


# =========================================================
# 🎯 GET ALL TECHNICAL INDICATOR NAMES
# =========================================================
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
            rows = safe_fetchall(cur)

        return [{"name": r[0], "display_name": r[1]} for r in rows]

    finally:
        conn.close()


# =========================================================
# 🧠 GET SCORING RULES
# =========================================================
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
            rows = safe_fetchall(cur)

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
            }
            for r in rows
        ]

    finally:
        conn.close()
