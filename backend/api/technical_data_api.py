import os
import logging
from datetime import datetime
from fastapi import APIRouter, HTTPException, Request
from dotenv import load_dotenv
from backend.utils.db import get_db_connection

# =====================================
# 🔧 ENV + Logging
# =====================================
dotenv_path = os.path.join(os.path.dirname(__file__), "..", ".env")
load_dotenv(dotenv_path=dotenv_path)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s")

router = APIRouter()
logger.info("🚀 technical_data_api.py geladen – nieuwe stabiele versie actief.")

# =====================================
# 🧩 Helper
# =====================================
def get_db_cursor():
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500,
            detail="❌ Geen databaseverbinding.")
    return conn, conn.cursor()

# =====================================
# 🔧 Veilig fetchen
# =====================================
def safe_fetchall(cur):
    try:
        rows = cur.fetchall()
        return rows or []
    except:
        return []

# =====================================
# GET — ALLE TECHNISCHE DATA
# =====================================
@router.get("/technical_data")
async def get_technical_data():
    conn, cur = get_db_cursor()
    try:
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
                "waarde": r[1],
                "score": r[2],
                "advies": r[3],
                "uitleg": r[4],
                "timestamp": r[5].isoformat() if r[5] else None
            }
            for r in rows
        ]

    finally:
        conn.close()

# =====================================
# ➕ Technische indicator toevoegen
# =====================================
@router.post("/technical_data")
async def add_technical_indicator(request: Request):
    logger.info("📐 [add] Technische indicator toevoegen...")
    data = await request.json()
    name = data.get("indicator")

    if not name:
        raise HTTPException(status_code=400, detail="❌ 'indicator' is verplicht.")

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="❌ Geen databaseverbinding.")

    try:
        # Config ophalen
        with conn.cursor() as cur:
            cur.execute("""
                SELECT source, link
                FROM indicators
                WHERE LOWER(name)=LOWER(%s)
                AND category='technical'
                AND active=TRUE;
            """, (name,))
            cfg = cur.fetchone()

        if not cfg:
            raise HTTPException(
                status_code=404,
                detail=f"Indicator '{name}' niet gevonden in configuratie."
            )

        source, link = cfg

        # Waarde ophalen (zonder await!)
        from backend.utils.technical_interpreter import fetch_technical_value
        result = fetch_technical_value(
            name=name,
            source=source,
            link=link
        )

        if not result:
            raise HTTPException(status_code=500, detail=f"❌ Geen waarde voor '{name}'")

        # Extract
        if isinstance(result, dict) and "value" in result:
            value = float(result["value"])
        else:
            value = float(result)

        # Score berekenen — FIXED
        from backend.utils.scoring_utils import generate_scores_db
        score_obj = generate_scores_db(name, value, "technical")

        score = score_obj.get("score", 10)
        advies = score_obj.get("trend", "–")
        uitleg = score_obj.get("interpretation", "–")
        action = score_obj.get("action", "–")

        # Opslaan
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO technical_indicators
                (indicator, value, score, advies, uitleg, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING id;
            """,
            (name, value, score, advies, uitleg, datetime.utcnow()))

            new_id = cur.fetchone()[0]
            conn.commit()

        return {
            "message": f"Indicator '{name}' toegevoegd.",
            "id": new_id,
            "value": value,
            "score": score,
            "advies": advies,
            "uitleg": uitleg,
            "action": action
        }

    except Exception as e:
        logger.error(f"❌ [add_technical_indicator] Fout: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

# =====================================
# 📅 DAY
# =====================================
@router.get("/technical_data/day")
async def get_latest_day_data():
    conn, cur = get_db_cursor()
    try:
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
                "waarde": r[1],
                "score": r[2],
                "advies": r[3],
                "uitleg": r[4],
                "timestamp": r[5].isoformat()
            }
            for r in rows
        ]

    finally:
        conn.close()

# =====================================
# ⏳ WEEK
# =====================================
@router.get("/technical_data/week")
async def get_technical_week_data():
    conn, cur = get_db_cursor()
    try:
        cur.execute("""
            SELECT DISTINCT DATE(timestamp)
            FROM technical_indicators
            ORDER BY 1 DESC
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
                "waarde": r[1],
                "score": r[2],
                "advies": r[3],
                "uitleg": r[4],
                "timestamp": r[5].isoformat()
            }
            for r in rows
        ]

    finally:
        conn.close()

# =====================================
# 📅 MONTH
# =====================================
@router.get("/technical_data/month")
async def get_technical_month_data():
    conn, cur = get_db_cursor()
    try:
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
                "waarde": r[1],
                "score": r[2],
                "advies": r[3],
                "uitleg": r[4],
                "timestamp": r[5].isoformat()
            }
            for r in rows
        ]

    finally:
        conn.close()

# =====================================
# 🗓 QUARTER
# =====================================
@router.get("/technical_data/quarter")
async def get_technical_quarter_data():
    conn, cur = get_db_cursor()
    try:
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
                "waarde": r[1],
                "score": r[2],
                "advies": r[3],
                "uitleg": r[4],
                "timestamp": r[5].isoformat()
            }
            for r in rows
        ]

    finally:
        conn.close()

# =====================================
# ❌ DELETE
# =====================================
@router.delete("/technical_data/{indicator}")
async def delete_technical_indicator(indicator: str):
    conn, cur = get_db_cursor()
    try:
        cur.execute("""
            DELETE FROM technical_indicators
            WHERE LOWER(indicator) = LOWER(%s);
        """, (indicator,))
        deleted = cur.rowcount
        conn.commit()

        return {
            "message": f"Indicator '{indicator}' verwijderd.",
            "deleted_rows": deleted
        }

    finally:
        conn.close()

# =====================================
# 🎯 INDICATOR DROPDOWN
# =====================================
@router.get("/technical/indicators")
async def get_all_indicators():
    conn, cur = get_db_cursor()
    try:
        cur.execute("""
            SELECT name, display_name
            FROM indicators
            WHERE active = TRUE
            AND category='technical'
            ORDER BY name;
        """)
        rows = safe_fetchall(cur)

        return [
            {"name": r[0], "display_name": r[1]}
            for r in rows
        ]

    finally:
        conn.close()

# =====================================
# 🧠 SCORING RULES
# =====================================
@router.get("/technical_indicator_rules/{indicator_name}")
async def get_rules_for_indicator(indicator_name: str):
    conn, cur = get_db_cursor()
    try:
        cur.execute("""
            SELECT id, indicator, range_min, range_max, score, trend, interpretation, action
            FROM technical_indicator_rules
            WHERE LOWER(indicator)=LOWER(%s)
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
                "action": r[7]
            }
            for r in rows
        ]

    finally:
        conn.close()
