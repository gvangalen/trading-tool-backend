import logging
from datetime import datetime
from fastapi import APIRouter, HTTPException, Request
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

# =====================================
# ➕ Technische indicator toevoegen
# =====================================
@router.post("/technical_data")
async def add_technical_indicator(request: Request):
    """
    ➕ Voeg een technische indicator toe:
    - Controleert of de indicator bestaat in `indicators`
    - Haalt actuele waarde op via technical_interpreter (binance)
    - Berekent score via technical_indicator_rules
    - Slaat op in technical_indicators
    """
    logger.info("📐 [add] Technische indicator toevoegen...")
    data = await request.json()
    name = data.get("indicator")

    if not name:
        raise HTTPException(status_code=400, detail="❌ 'indicator' is verplicht.")

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="❌ Geen databaseverbinding.")

    try:
        # =============================================
        # 1️⃣ Indicatorconfig ophalen uit `indicators`
        # =============================================
        with conn.cursor() as cur:
            cur.execute("""
                SELECT source, data_url, symbol, interval
                FROM indicators
                WHERE LOWER(name)=LOWER(%s)
                AND category='technical'
                AND active=TRUE;
            """, (name,))
            row = cur.fetchone()

        if not row:
            raise HTTPException(
                status_code=404,
                detail=f"Indicator '{name}' staat niet in de DB-config."
            )

        source, data_url, symbol, interval = row

        # =============================================
        # 2️⃣ Waarde ophalen via technical_interpreter
        # =============================================
        logger.info(f"⚙️ Ophalen waarde voor {name} via source={source}")

        from backend.utils.technical_interpreter import fetch_technical_value

        result = await fetch_technical_value(
            name=name,
            source=source,
            symbol=symbol,
            interval=interval,
            link=data_url
        )

        if not result:
            raise HTTPException(
                status_code=500,
                detail=f"❌ Geen waarde ontvangen van datasource voor '{name}'."
            )

        # Result kan dict of float zijn
        value = float(result["value"] if isinstance(result, dict) else result)

        # =============================================
        # 3️⃣ Score berekenen via DB rules
        # =============================================
        from backend.utils.scoring_utils import generate_scores_db

        score_obj = generate_scores_db(name, value, category="technical")

        score = score_obj.get("score", 10)
        trend = score_obj.get("trend", "–")
        interpretation = score_obj.get("interpretation", "–")
        action = score_obj.get("action", "–")

        logger.info(
            f"📊 Score berekend: {score} trend={trend}"
        )

        # =============================================
        # 4️⃣ Opslaan in technical_indicators
        # =============================================
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO technical_indicators
                (indicator, value, score, advies, uitleg, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING id;
            """, (name, value, score, trend, interpretation, datetime.utcnow()))

            new_id = cur.fetchone()[0]
            conn.commit()

        logger.info(f"✅ [add] Technische indicator '{name}' opgeslagen.")

        return {
            "message": f"Indicator '{name}' succesvol toegevoegd.",
            "id": new_id,
            "value": value,
            "score": score,
            "advies": trend,
            "uitleg": interpretation,
            "action": action,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ [tech-add] Fout: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"❌ Fout bij opslaan technical indicator: {str(e)}"
        )
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
