import os
import logging
from datetime import datetime
from fastapi import APIRouter, HTTPException, Request
from dotenv import load_dotenv
from backend.utils.db import get_db_connection

# ✅ .env laden
dotenv_path = os.path.join(os.path.dirname(__file__), "..", ".env")
load_dotenv(dotenv_path=dotenv_path)

# ✅ Logging instellen
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ✅ Router aanmaken
router = APIRouter()
logger.info("🚀 macro_data_api.py geladen – verbeterde DB-versie actief.")

# =====================================
# 🔧 Helperfunctie
# =====================================
def get_db_cursor():
    """Maak een veilige databasecursor aan."""
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="❌ [DB01] Geen databaseverbinding.")
    return conn, conn.cursor()


# =====================================
# ➕ Macro-indicator toevoegen of updaten
# =====================================
@router.post("/macro_data")
async def add_macro_indicator(request: Request):
    """
    ➕ Voeg een bestaande macro-indicator toe of sla nieuwe waarde op.
    - Controleert of de indicator bestaat in `indicators`.
    - Haalt automatische waarde op via `macro_interpreter` of gebruikt handmatige waarde.
    - Berekent score via `scoring_utils`.
    """
    logger.info("📅 [add] Macro-indicator toevoegen of bijwerken...")
    data = await request.json()
    name = data.get("name")

    if not name:
        raise HTTPException(status_code=400, detail="❌ 'name' is verplicht in de payload.")

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="❌ Geen databaseverbinding.")

    try:
        # ✅ Controleer of de indicator in de configuratie staat
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
                logger.warning(f"⚠️ Indicator '{name}' niet gevonden in configuratie.")
                raise HTTPException(
                    status_code=404,
                    detail=f"Indicator '{name}' bestaat niet in de database-configuratie."
                )

            source, data_url = indicator_info

        # ✅ Ophalen van waarde
        if "value" in data:
            value = float(data["value"])
            logger.info(f"📊 [add] Handmatige waarde ontvangen voor '{name}': {value}")
        else:
            logger.info(f"⚙️ [add] Ophalen actuele waarde voor '{name}' via source={source}")
            from backend.utils.macro_interpreter import fetch_macro_value
            result = await fetch_macro_value(name, source=source, link=data_url)

            if not result:
                raise HTTPException(status_code=500, detail=f"❌ Geen waarde ontvangen voor '{name}'")

            # ✅ Slimmere detectie van de waarde (ondersteunt meerdere formaten)
            if isinstance(result, dict):
                if "value" in result:
                    value = float(result["value"])
                elif "data" in result and isinstance(result["data"], dict) and "value" in result["data"]:
                    value = float(result["data"]["value"])
                elif "result" in result:
                    value = float(result["result"])
                else:
                    raise HTTPException(status_code=500, detail=f"❌ Ongeldig resultaatformaat voor '{name}': {result}")
            else:
                # Fallback als de functie direct een getal retourneert
                try:
                    value = float(result)
                except Exception:
                    raise HTTPException(status_code=500, detail=f"❌ Kan waarde niet converteren voor '{name}': {result}")

        # ✅ Score berekenen via scoreregels in DB
        from backend.utils.scoring_utils import generate_scores_db
        score_obj = generate_scores_db(name, value)
        score = score_obj.get("score", 10)
        trend = score_obj.get("trend", "–")
        interpretation = score_obj.get("interpretation", "–")
        action = score_obj.get("action", "–")

        # ✅ Opslaan in macro_data
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO macro_data (name, value, trend, interpretation, action, score, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (name, value, trend, interpretation, action, score, datetime.utcnow()))
            conn.commit()

        logger.info(f"✅ [add] '{name}' opgeslagen | value={value} | score={score} | trend={trend}")
        return {
            "message": f"Indicator '{name}' succesvol opgeslagen.",
            "value": value,
            "score": score,
            "trend": trend,
            "interpretation": interpretation,
            "action": action,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ [DB02] Fout bij opslaan macro data: {e}")
        raise HTTPException(status_code=500, detail=f"❌ [DB02] Fout bij opslaan macro data: {str(e)}")
    finally:
        conn.close()

# =====================================
# 📄 Ophalen macro-data
# =====================================
@router.get("/macro_data")
async def get_macro_indicators():
    logger.info("📄 [get] Ophalen macro-indicatoren...")
    conn, cur = get_db_cursor()
    try:
        cur.execute("""
            SELECT id, name, value, trend, interpretation, action, score, timestamp
            FROM macro_data
            ORDER BY timestamp DESC
            LIMIT 100;
        """)
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
        logger.error(f"❌ [get] Databasefout: {e}")
        raise HTTPException(status_code=500, detail="❌ [DB03] Ophalen macro-data mislukt.")
    finally:
        conn.close()

@router.get("/macro_data/day")
async def get_latest_macro_day_data():
    logger.info("📄 [get/day] Ophalen macro-dagdata (met fallback)...")
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Geen databaseverbinding.")
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT name, value, trend, interpretation, action, score, timestamp
                FROM macro_data
                WHERE DATE(timestamp) = CURRENT_DATE
                ORDER BY timestamp DESC;
            """)
            rows = cur.fetchall()
            if not rows:
                cur.execute("""
                    SELECT timestamp FROM macro_data ORDER BY timestamp DESC LIMIT 1;
                """)
                fallback_ts = cur.fetchone()
                if not fallback_ts:
                    logger.warning("⚠️ Geen fallback-timestamp gevonden.")
                    return []
                fallback_date = fallback_ts[0].date()
                cur.execute("""
                    SELECT name, value, trend, interpretation, action, score, timestamp
                    FROM macro_data
                    WHERE DATE(timestamp) = %s
                    ORDER BY timestamp DESC;
                """, (fallback_date,))
                rows = cur.fetchall()
        return [
            {
                "name": row[0],
                "value": row[1],
                "trend": row[2],
                "interpretation": row[3],
                "action": row[4],
                "score": row[5],
                "timestamp": row[6].isoformat() if row[6] else None
            }
            for row in rows
        ]
    except Exception as e:
        logger.error(f"❌ [get/day] Fout bij ophalen macro dagdata: {e}")
        raise HTTPException(status_code=500, detail="❌ [DB06] Ophalen macro dagdata mislukt.")
    finally:
        conn.close()

@router.get("/macro_data/week")
async def get_macro_week_data():
    logger.info("📤 [get/week] Ophalen macro-data (laatste 7 unieke dagen)...")
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Databaseverbinding mislukt.")
    try:
        with conn.cursor() as cur:
            # 🗓️ Stap 1: unieke datums ophalen
            cur.execute("""
                SELECT DISTINCT DATE(timestamp) AS dag
                FROM macro_data
                ORDER BY dag DESC
                LIMIT 7;
            """)
            dagen = [r[0] for r in cur.fetchall()]
            if not dagen:
                return []

            logger.info(f"📅 Weekdagen: {dagen}")

            # 🧮 Stap 2: data ophalen voor deze dagen
            cur.execute("""
                SELECT name, value, trend, interpretation, action, score, timestamp
                FROM macro_data
                WHERE DATE(timestamp) = ANY(%s)
                ORDER BY timestamp DESC;
            """, (dagen,))
            rows = cur.fetchall()

        return [
            {
                "indicator": row[0],
                "waarde": row[1],
                "trend": row[2],
                "interpretation": row[3],
                "action": row[4],
                "score": row[5],
                "timestamp": row[6].isoformat()
            } for row in rows
        ]
    except Exception as e:
        logger.error(f"❌ [get/week] Databasefout: {e}")
        raise HTTPException(status_code=500, detail="❌ [DB07] Ophalen weekdata mislukt.")
    finally:
        conn.close()

@router.get("/macro_data/month")
async def get_macro_month_data():
    logger.info("📤 [get/month] Ophalen macro-data (4 recente weken)...")
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Databaseverbinding mislukt.")
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT DATE_TRUNC('week', timestamp)::date AS week_start
                FROM macro_data
                ORDER BY week_start DESC
                LIMIT 4;
            """)
            weken = [r[0] for r in cur.fetchall()]
            if not weken:
                return []

            logger.info(f"📅 Maandweken: {weken}")

            cur.execute("""
                SELECT name, value, trend, interpretation, action, score, timestamp
                FROM macro_data
                WHERE DATE_TRUNC('week', timestamp)::date = ANY(%s)
                ORDER BY timestamp DESC;
            """, (weken,))
            rows = cur.fetchall()

        return [
            {
                "indicator": row[0],
                "waarde": row[1],
                "trend": row[2],
                "interpretation": row[3],
                "action": row[4],
                "score": row[5],
                "timestamp": row[6].isoformat()
            } for row in rows
        ]
    except Exception as e:
        logger.error(f"❌ [get/month] Databasefout: {e}")
        raise HTTPException(status_code=500, detail="❌ [DB08] Ophalen maanddata mislukt.")
    finally:
        conn.close()

@router.get("/macro_data/quarter")
async def get_macro_quarter_data():
    logger.info("📤 [get/quarter] Ophalen macro-data (12 recente weken)...")
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Databaseverbinding mislukt.")
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT DATE_TRUNC('week', timestamp)::date AS week_start
                FROM macro_data
                ORDER BY week_start DESC
                LIMIT 12;
            """)
            weken = [r[0] for r in cur.fetchall()]
            if not weken:
                return []

            logger.info(f"📅 Kwartaalweken: {weken}")

            cur.execute("""
                SELECT name, value, trend, interpretation, action, score, timestamp
                FROM macro_data
                WHERE DATE_TRUNC('week', timestamp)::date = ANY(%s)
                ORDER BY timestamp DESC;
            """, (weken,))
            rows = cur.fetchall()

        return [
            {
                "indicator": row[0],
                "waarde": row[1],
                "trend": row[2],
                "interpretation": row[3],
                "action": row[4],
                "score": row[5],
                "timestamp": row[6].isoformat()
            } for row in rows
        ]
    except Exception as e:
        logger.error(f"❌ [get/quarter] Databasefout: {e}")
        raise HTTPException(status_code=500, detail="❌ [DB09] Ophalen kwartaaldata mislukt.")
    finally:
        conn.close()

# ✅ 1. Alle beschikbare macro-indicatornamen ophalen (voor dropdown)
@router.get("/macro/indicators")
async def get_all_macro_indicators():
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Geen databaseverbinding.")
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT name, display_name
                FROM indicators
                WHERE active = TRUE AND category = 'macro'
                ORDER BY name;
            """)
            rows = cur.fetchall()
        return [{"name": r[0], "display_name": r[1]} for r in rows]
    except Exception as e:
        logger.error(f"❌ Fout bij ophalen macro-indicatornamen: {e}")
        raise HTTPException(status_code=500, detail="Fout bij ophalen macro-indicatornamen.")
    finally:
        conn.close()


# ✅ 2. Alle scoreregels ophalen per macro-indicator
@router.get("/macro_indicator_rules/{indicator_name}")
async def get_rules_for_macro_indicator(indicator_name: str):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Geen databaseverbinding.")
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
            } for r in rows
        ]
    except Exception as e:
        logger.error(f"❌ Fout bij ophalen scoreregels voor macro-indicator {indicator_name}: {e}")
        raise HTTPException(status_code=500, detail="Fout bij ophalen macro-scoreregels.")
    finally:
        conn.close()

# ✅ Verwijder één macro-indicator op basis van naam
@router.delete("/macro_data/{name}")
async def delete_macro_indicator(name: str):
    logger.info(f"🗑️ [delete] Verwijderen van macro-indicator: {name}")
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="❌ [DB10] Geen databaseverbinding.")
    try:
        with conn.cursor() as cur:
            # Check of de indicator bestaat
            cur.execute("SELECT COUNT(*) FROM macro_data WHERE name = %s;", (name,))
            count = cur.fetchone()[0]
            if count == 0:
                raise HTTPException(status_code=404, detail=f"❌ Indicator '{name}' niet gevonden.")

            # Verwijder alle entries van deze indicator (of voeg LIMIT 1 toe als je alleen laatste wilt)
            cur.execute("DELETE FROM macro_data WHERE name = %s;", (name,))
            conn.commit()

            logger.info(f"✅ [delete] Indicator '{name}' succesvol verwijderd ({count} rijen).")
            return {"message": f"Indicator '{name}' verwijderd.", "rows_deleted": count}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ [DB11] Fout bij verwijderen macro-indicator: {e}")
        raise HTTPException(status_code=500, detail="❌ [DB11] Verwijderen mislukt.")
    finally:
        conn.close()
