import os
import logging
from datetime import datetime
from fastapi import APIRouter, HTTPException, Request, Depends

from dotenv import load_dotenv

from backend.utils.db import get_db_connection
from backend.utils.auth_utils import get_current_user

# ⭐ Onboarding helper importeren
from backend.api.onboarding_api import mark_step_completed  

logger = logging.getLogger(__name__)
router = APIRouter()

dotenv_path = os.path.join(os.path.dirname(__file__), "..", ".env")
load_dotenv(dotenv_path=dotenv_path)

logger.info("🚀 macro_data_api.py geladen – user_id-systeem; onboarding alleen bij POST /macro_data.")


# =====================================
# 🔧 Helperfunctie
# =====================================
def get_db_cursor():
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="❌ [DB01] Geen databaseverbinding.")
    return conn, conn.cursor()


# =====================================
# ➕ Macro-indicator opslaan (met user_id + onboarding update)
# 👉 ENIGE plek waar onboarding 'macro' wordt gemarkeerd
# =====================================
@router.post("/macro_data")
async def add_macro_indicator(request: Request, current_user: dict = Depends(get_current_user)):
    """
    ➕ Voeg macro-data toe voor deze gebruiker.
    Onboarding wordt automatisch bijgewerkt → macro stap = voltooid.
    """
    user_id = current_user["id"]
    logger.info(f"📅 [add] Macro opslaan voor user_id={user_id}...")

    data = await request.json()
    name = data.get("name")

    if not name:
        raise HTTPException(status_code=400, detail="❌ 'name' veld is verplicht.")

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="❌ DB niet beschikbaar.")

    try:
        # Indicator ophalen uit configuratietabel
        with conn.cursor() as cur:
            cur.execute("""
                SELECT source, data_url
                FROM indicators
                WHERE LOWER(name) = LOWER(%s)
                  AND category = 'macro'
                  AND active = TRUE;
            """, (name,))
            indicator_info = cur.fetchone()

            if not indicator_info:
                raise HTTPException(
                    status_code=404,
                    detail=f"Indicator '{name}' bestaat niet of is inactief."
                )

            source, data_url = indicator_info

        # Waarde bepalen
        if "value" in data:
            value = float(data["value"])
        else:
            from backend.utils.macro_interpreter import fetch_macro_value
            result = await fetch_macro_value(name, source=source, link=data_url)

            if not result:
                raise HTTPException(status_code=500, detail=f"❌ Geen waarde ontvangen voor '{name}'")

            if isinstance(result, dict):
                if "value" in result:
                    value = float(result["value"])
                elif "data" in result and "value" in result["data"]:
                    value = float(result["data"]["value"])
                elif "result" in result:
                    value = float(result["result"])
                else:
                    raise HTTPException(
                        status_code=500,
                        detail=f"❌ Kan waarde niet parsen: {result}"
                    )
            else:
                value = float(result)

        # Score berekenen via DB-config
        from backend.utils.scoring_utils import generate_scores_db
        score_info = generate_scores_db(name, value)

        score = score_info.get("score", 10)
        trend = score_info.get("trend", "–")
        interpretation = score_info.get("interpretation", "–")
        action = score_info.get("action", "–")

        # Opslaan in macro_data (per user)
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO macro_data (
                    name, value, trend, interpretation, action,
                    score, timestamp, user_id
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                name,
                value,
                trend,
                interpretation,
                action,
                score,
                datetime.utcnow(),
                user_id
            ))
            conn.commit()

        # ⭐ ONBOARDING: macro step automatisch markeren
        mark_step_completed(conn, user_id, "macro")

        return {
            "message": f"Indicator '{name}' opgeslagen.",
            "value": value,
            "score": score,
            "trend": trend,
            "interpretation": interpretation,
            "action": action
        }

    except Exception as e:
        logger.error(f"❌ Macro save error: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Fout bij opslaan macro data: {e}"
        )

    finally:
        conn.close()


# =====================================
# 📄 Macro data ophalen (met user_id)
# ✅ GEEN onboarding hier
# =====================================
@router.get("/macro_data")
async def get_macro_indicators(current_user: dict = Depends(get_current_user)):
    logger.info(f"📄 [get] Macro-data voor user_id={current_user['id']}")

    conn, cur = get_db_cursor()
    try:
        cur.execute("""
            SELECT id, name, value, trend, interpretation, action, score, timestamp
            FROM macro_data
            WHERE user_id = %s
            ORDER BY timestamp DESC
            LIMIT 100;
        """, (current_user["id"],))

        rows = cur.fetchall()

        return [
            {
                "id": r[0],
                "name": r[1],
                "value": r[2],
                "trend": r[3],
                "interpretation": r[4],
                "action": r[5],
                "score": r[6],
                "timestamp": r[7].isoformat() if r[7] else None
            }
            for r in rows
        ]

    except Exception as e:
        logger.error(f"❌ Macro get error: {e}")
        raise HTTPException(status_code=500, detail="Macro data ophalen mislukt.")
    finally:
        conn.close()


# =====================================
# 📆 Macro dagdata ophalen
# ✅ GEEN onboarding hier
# =====================================
@router.get("/macro_data/day")
async def get_latest_macro_day_data(current_user: dict = Depends(get_current_user)):
    logger.info(f"📄 [get/day] Macro dagdata voor user_id={current_user['id']}")
    conn = get_db_connection()

    if not conn:
        raise HTTPException(500, "❌ Geen databaseverbinding.")

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT name, value, trend, interpretation, action, score, timestamp
                FROM macro_data
                WHERE user_id = %s
                  AND DATE(timestamp) = CURRENT_DATE
                ORDER BY timestamp DESC;
            """, (current_user["id"],))

            rows = cur.fetchall()

            # Fallback → laatste dag waarop data aanwezig is
            if not rows:
                cur.execute("""
                    SELECT timestamp
                    FROM macro_data
                    WHERE user_id = %s
                    ORDER BY timestamp DESC
                    LIMIT 1;
                """, (current_user["id"],))

                ts = cur.fetchone()
                if not ts:
                    return []

                fallback = ts[0].date()

                cur.execute("""
                    SELECT name, value, trend, interpretation, action, score, timestamp
                    FROM macro_data
                    WHERE user_id = %s
                      AND DATE(timestamp) = %s
                    ORDER BY timestamp DESC;
                """, (current_user["id"], fallback))

                rows = cur.fetchall()

        return [
            {
                "name": r[0],
                "value": r[1],
                "trend": r[2],
                "interpretation": r[3],
                "action": r[4],
                "score": r[5],
                "timestamp": r[6].isoformat() if r[6] else None
            }
            for r in rows
        ]

    except Exception as e:
        logger.error(f"❌ Macro day error: {e}")
        raise HTTPException(status_code=500, detail="Macro dagdata ophalen mislukt.")
    finally:
        conn.close()


# =====================================
# 📅 Weekdata ophalen (user_id)
# ✅ GEEN onboarding hier
# =====================================
@router.get("/macro_data/week")
async def get_macro_week_data(current_user: dict = Depends(get_current_user)):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(500, "❌ Geen databaseverbinding.")

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT DATE(timestamp)
                FROM macro_data
                WHERE user_id = %s
                ORDER BY DATE(timestamp) DESC
                LIMIT 7;
            """, (current_user["id"],))

            dagen = [r[0] for r in cur.fetchall()]
            if not dagen:
                return []

            cur.execute("""
                SELECT name, value, trend, interpretation, action, score, timestamp
                FROM macro_data
                WHERE user_id = %s
                  AND DATE(timestamp) = ANY(%s)
                ORDER BY timestamp DESC;
            """, (current_user["id"], dagen))

            rows = cur.fetchall()

        return [
            {
                "indicator": r[0],
                "waarde": r[1],
                "trend": r[2],
                "interpretation": r[3],
                "action": r[4],
                "score": r[5],
                "timestamp": r[6].isoformat() if r[6] else None
            }
            for r in rows
        ]

    finally:
        conn.close()


# =====================================
# 📅 Maanddata ophalen (user_id)
# ✅ GEEN onboarding hier
# =====================================
@router.get("/macro_data/month")
async def get_macro_month_data(current_user: dict = Depends(get_current_user)):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(500, "❌ Geen databaseverbinding.")

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT DATE_TRUNC('week', timestamp)::date
                FROM macro_data
                WHERE user_id = %s
                ORDER BY 1 DESC
                LIMIT 4;
            """, (current_user["id"],))

            weken = [r[0] for r in cur.fetchall()]
            if not weken:
                return []

            cur.execute("""
                SELECT name, value, trend, interpretation, action, score, timestamp
                FROM macro_data
                WHERE user_id = %s
                  AND DATE_TRUNC('week', timestamp)::date = ANY(%s)
                ORDER BY timestamp DESC;
            """, (current_user["id"], weken))

            rows = cur.fetchall()

        return [
            {
                "indicator": r[0],
                "waarde": r[1],
                "trend": r[2],
                "interpretation": r[3],
                "action": r[4],
                "score": r[5],
                "timestamp": r[6].isoformat() if r[6] else None
            }
            for r in rows
        ]

    finally:
        conn.close()


# =====================================
# 📅 Kwartaaldata ophalen (user_id)
# ✅ GEEN onboarding hier
# =====================================
@router.get("/macro_data/quarter")
async def get_macro_quarter_data(current_user: dict = Depends(get_current_user)):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(500, "❌ Geen databaseverbinding.")

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT DATE_TRUNC('week', timestamp)::date
                FROM macro_data
                WHERE user_id = %s
                ORDER BY 1 DESC
                LIMIT 12;
            """, (current_user["id"],))

            weken = [r[0] for r in cur.fetchall()]
            if not weken:
                return []

            cur.execute("""
                SELECT name, value, trend, interpretation, action, score, timestamp
                FROM macro_data
                WHERE user_id = %s
                  AND DATE_TRUNC('week', timestamp)::date = ANY(%s)
                ORDER BY timestamp DESC;
            """, (current_user["id"], weken))

            rows = cur.fetchall()

        return [
            {
                "indicator": r[0],
                "waarde": r[1],
                "trend": r[2],
                "interpretation": r[3],
                "action": r[4],
                "score": r[5],
                "timestamp": r[6].isoformat() if r[6] else None
            }
            for r in rows
        ]

    finally:
        conn.close()


# ===========================================
# 🔍 Dropdown lijst met macro indicatoren
# ✅ GEEN onboarding hier
# ===========================================
@router.get("/macro/indicators")
async def get_all_macro_indicators():
    conn = get_db_connection()
    if not conn:
        raise HTTPException(500, "❌ Geen databaseverbinding.")

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT name, display_name
                FROM indicators
                WHERE active = TRUE
                  AND category = 'macro'
                ORDER BY name;
            """)
            rows = cur.fetchall()

        return [{"name": r[0], "display_name": r[1]} for r in rows]

    finally:
        conn.close()


# =====================================
# 🔍 Scoreregels ophalen
# ✅ GEEN onboarding hier
# =====================================
@router.get("/macro_indicator_rules/{indicator_name}")
async def get_rules_for_macro_indicator(indicator_name: str):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(500, "❌ Geen databaseverbinding.")

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, indicator, range_min, range_max, score, trend, interpretation, action
                FROM macro_indicator_rules
                WHERE indicator = %s
                ORDER BY score ASC;
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
                "action": r[7]
            }
            for r in rows
        ]

    finally:
        conn.close()


# =====================================
# ❌ Verwijderen macro indicator
# ✅ GEEN onboarding hier
# =====================================
@router.delete("/macro_data/{name}")
async def delete_macro_indicator(name: str, current_user: dict = Depends(get_current_user)):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(500, "❌ Geen databaseverbinding.")

    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM macro_data WHERE name = %s AND user_id = %s",
                (name, current_user["id"]),
            )
            count = cur.fetchone()[0]

            if count == 0:
                raise HTTPException(
                    status_code=404,
                    detail=f"Indicator '{name}' niet gevonden voor deze gebruiker."
                )

            cur.execute(
                "DELETE FROM macro_data WHERE name = %s AND user_id = %s",
                (name, current_user["id"]),
            )
            conn.commit()

        return {
            "message": f"Indicator '{name}' verwijderd.",
            "rows_deleted": count
        }

    finally:
        conn.close()
