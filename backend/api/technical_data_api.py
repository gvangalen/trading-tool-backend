import os
import logging
from datetime import datetime
from fastapi import APIRouter, HTTPException, Request, Depends
from dotenv import load_dotenv

from backend.utils.db import get_db_connection
from backend.utils.auth_utils import get_current_user
from backend.utils.scoring_utils import (
    get_score_rule_from_db,
    normalize_indicator_name
)

# ⭐ Onboarding
from backend.api.onboarding_api import mark_step_completed

# =====================================
# 🔧 ENV + Logging
# =====================================
dotenv_path = os.path.join(os.path.dirname(__file__), "..", ".env")
load_dotenv(dotenv_path=dotenv_path)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s")

router = APIRouter()
logger.info("🚀 technical_data_api.py geladen – nieuwe user-aware versie actief.")


# =====================================
# 🧩 Helper
# =====================================
def get_db_cursor():
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500,
            detail="❌ Geen databaseverbinding.")
    return conn, conn.cursor()

def safe_fetchall(cur):
    try:
        rows = cur.fetchall()
        return rows or []
    except:
        return []


# =====================================
# GET — ALLE TECHNISCHE DATA (per user)
# =====================================
@router.get("/technical_data")
async def get_technical_data(current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]
    conn, cur = get_db_cursor()
    try:
        cur.execute("""
            SELECT indicator, value, score, advies, uitleg, timestamp
            FROM technical_indicators
            WHERE user_id = %s
            ORDER BY timestamp DESC
            LIMIT 50;
        """, (user_id,))
        rows = safe_fetchall(cur)

        # ⭐ ONBOARDING
        mark_step_completed(conn, user_id, "technical")

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
# ➕ Technische indicator toevoegen
# =====================================
@router.post("/technical_data")
async def add_technical_indicator(
    request: Request,
    current_user: dict = Depends(get_current_user)
):
    logger.info("📐 [add] Technische indicator toevoegen...")
    data = await request.json()

    user_id = current_user["id"]
    name_raw = data.get("indicator")

    if not name_raw:
        raise HTTPException(400, "❌ 'indicator' is verplicht.")

    name = normalize_indicator_name(name_raw)

    conn = get_db_connection()
    if not conn:
        raise HTTPException(500, "❌ Geen databaseverbinding.")

    try:
        # config ophalen
        with conn.cursor() as cur:
            cur.execute("""
                SELECT source, link
                FROM indicators
                WHERE LOWER(name)=LOWER(%s)
                  AND category='technical'
                  AND active=TRUE
            """, (name,))
            cfg = cur.fetchone()

        if not cfg:
            raise HTTPException(404, f"Indicator '{name}' niet gevonden of niet actief.")

        source, link = cfg

        # waarde ophalen via interpreter
        from backend.utils.technical_interpreter import fetch_technical_value
        result = fetch_technical_value(name=name, source=source, link=link)
        if not result:
            raise HTTPException(500, f"❌ Geen waarde voor '{name}'.")

        value = float(result["value"] if isinstance(result, dict) else result)

        # score bepalen
        score_obj = get_score_rule_from_db("technical", name, value)
        if not score_obj:
            raise HTTPException(500, f"❌ Geen scoreregels voor '{name}'.")

        score = score_obj["score"]
        advies = score_obj["trend"]
        uitleg = score_obj["interpretation"]

        # opslaan
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO technical_indicators
                (indicator, value, score, advies, uitleg, user_id, timestamp)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                RETURNING id;
            """, (
                name, value, score, advies, uitleg, user_id, datetime.utcnow()
            ))
            new_id = cur.fetchone()[0]

        conn.commit()

        # ⭐ ONBOARDING
        mark_step_completed(conn, user_id, "technical")

        return {
            "message": f"Indicator '{name}' toegevoegd.",
            "id": new_id,
            "value": value,
            "score": score,
            "advies": advies,
            "uitleg": uitleg
        }

    except Exception as e:
        logger.error(f"❌ [add_technical_indicator] {e}")
        raise HTTPException(500, str(e))

    finally:
        conn.close()



# =====================================
# 📅 DAY (per user)
# =====================================
@router.get("/technical_data/day")
async def get_latest_day_data(current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]
    conn, cur = get_db_cursor()

    try:
        cur.execute("""
            SELECT indicator, value, score, advies, uitleg, timestamp
            FROM technical_indicators
            WHERE DATE(timestamp)=CURRENT_DATE
              AND user_id=%s
            ORDER BY timestamp DESC;
        """, (user_id,))
        rows = safe_fetchall(cur)

        # fallback
        if not rows:
            cur.execute("""
                SELECT timestamp FROM technical_indicators
                WHERE user_id=%s
                ORDER BY timestamp DESC LIMIT 1;
            """, (user_id,))
            last = cur.fetchone()

            if last:
                fallback_date = last[0].date()
                cur.execute("""
                    SELECT indicator, value, score, advies, uitleg, timestamp
                    FROM technical_indicators
                    WHERE DATE(timestamp)=%s
                      AND user_id=%s
                    ORDER BY timestamp DESC;
                """, (fallback_date, user_id))
                rows = safe_fetchall(cur)

        # ⭐ ONBOARDING
        mark_step_completed(conn, user_id, "technical")

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
async def get_technical_week_data(current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]
    conn, cur = get_db_cursor()

    try:
        cur.execute("""
            SELECT DISTINCT DATE(timestamp)
            FROM technical_indicators
            WHERE user_id=%s
            ORDER BY 1 DESC
            LIMIT 7;
        """, (user_id,))
        dagen = [r[0] for r in safe_fetchall(cur)]

        cur.execute("""
            SELECT indicator, value, score, advies, uitleg, timestamp
            FROM technical_indicators
            WHERE DATE(timestamp)=ANY(%s)
              AND user_id=%s
            ORDER BY timestamp DESC;
        """, (dagen, user_id))
        rows = safe_fetchall(cur)

        # ⭐ ONBOARDING
        mark_step_completed(conn, user_id, "technical")

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
async def get_technical_month_data(current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]
    conn, cur = get_db_cursor()

    try:
        cur.execute("""
            SELECT DISTINCT DATE_TRUNC('week', timestamp)::date
            FROM technical_indicators
            WHERE user_id=%s
            ORDER BY 1 DESC
            LIMIT 4;
        """, (user_id,))
        weken = [r[0] for r in safe_fetchall(cur)]

        cur.execute("""
            SELECT indicator, value, score, advies, uitleg, timestamp
            FROM technical_indicators
            WHERE DATE_TRUNC('week', timestamp)::date=ANY(%s)
              AND user_id=%s
            ORDER BY timestamp DESC;
        """, (weken, user_id))
        rows = safe_fetchall(cur)

        # ⭐ ONBOARDING
        mark_step_completed(conn, user_id, "technical")

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
async def get_technical_quarter_data(current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]
    conn, cur = get_db_cursor()

    try:
        cur.execute("""
            SELECT DISTINCT DATE_TRUNC('week', timestamp)::date
            FROM technical_indicators
            WHERE user_id=%s
            ORDER BY 1 DESC
            LIMIT 12;
        """, (user_id,))
        weken = [r[0] for r in safe_fetchall(cur)]

        cur.execute("""
            SELECT indicator, value, score, advies, uitleg, timestamp
            FROM technical_indicators
            WHERE DATE_TRUNC('week', timestamp)::date=ANY(%s)
              AND user_id=%s
            ORDER BY timestamp DESC;
        """, (weken, user_id))
        rows = safe_fetchall(cur)

        # ⭐ ONBOARDING
        mark_step_completed(conn, user_id, "technical")

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
# ❌ DELETE INDICATOR
# =====================================
@router.delete("/technical_data/{indicator}")
async def delete_technical_indicator(
    indicator: str,
    current_user: dict = Depends(get_current_user)
):
    user_id = current_user["id"]
    conn, cur = get_db_cursor()

    try:
        cur.execute("""
            DELETE FROM technical_indicators
            WHERE LOWER(indicator)=LOWER(%s)
              AND user_id=%s;
        """, (indicator, user_id))
        deleted = cur.rowcount
        conn.commit()

        # ⭐ ONBOARDING
        mark_step_completed(conn, user_id, "technical")

        return {
            "message": f"Indicator '{indicator}' verwijderd.",
            "deleted_rows": deleted
        }

    finally:
        conn.close()



# =====================================
# 🎯 INDICATOR DROPDOWN (globaal)
# =====================================
@router.get("/technical/indicators")
async def get_all_indicators():
    conn, cur = get_db_cursor()
    try:
        cur.execute("""
            SELECT name, display_name
            FROM indicators
            WHERE active=TRUE
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
# 🧠 SCORING RULES (globaal)
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
