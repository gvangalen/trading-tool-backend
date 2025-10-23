import os
from dotenv import load_dotenv

# ✅ Forceren van laden van .env bestand
dotenv_path = os.path.join(os.path.dirname(__file__), "..", ".env")
load_dotenv(dotenv_path=dotenv_path)

import logging
from datetime import datetime
from fastapi import APIRouter, HTTPException, Request
from backend.utils.db import get_db_connection
from backend.utils.macro_interpreter import process_macro_indicator
from backend.config.config_loader import load_macro_config
from backend.utils.scoring_utils import generate_scores

router = APIRouter()
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

try:
    MACRO_CONFIG = load_macro_config()
    logger.info("🚀 macro_data_api.py geladen – alle macro-routes zijn actief.")
except Exception as e:
    MACRO_CONFIG = {}
    logger.error(f"❌ [INIT] Config niet geladen bij opstarten: {e}")

def get_db_cursor():
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="❌ [DB01] Geen databaseverbinding.")
    return conn, conn.cursor()
    
@router.post("/macro_data")
async def add_macro_indicator(request: Request):
    logger.info("📅 [add] Nieuwe macro-indicator toevoegen...")
    data = await request.json()
    name = data.get("name")
    if not name:
        raise HTTPException(status_code=400, detail="❌ [REQ01] Naam van indicator is verplicht.")

    config_data = MACRO_CONFIG
    if name not in config_data.get("indicators", {}):
        raise HTTPException(status_code=400, detail=f"❌ [CFG02] Indicator '{name}' niet gevonden in config.")

    indicator_config = config_data["indicators"][name]

    try:
        if "value" in data:
            logger.info(f"📅 [add] Externe waarde ontvangen voor '{name}' → {data['value']}")
            result = {
                "name": name,
                "value": data["value"],
                "symbol": indicator_config.get("symbol", ""),
                "source": indicator_config.get("source", ""),
            }
        else:
            logger.info(f"⚙️ [add] Ophalen via interpreter voor '{name}'")
            result = await process_macro_indicator(name, indicator_config)

        if not result or "value" not in result:
            raise ValueError(f"❌ Geen geldige waarde ontvangen voor {name}")

        value = float(result["value"])

    except Exception as e:
        logger.error(f"❌ [INT01] Interpreterfout: {e}")
        raise HTTPException(status_code=500, detail=f"❌ [INT01] Ophalen indicator mislukt: {e}")

    try:
        score_data = generate_scores({name: value}, {name: indicator_config})
        score_info = score_data["scores"].get(name, {})
        score = score_info.get("score", 0)
        trend = score_info.get("trend", "")
        interpretation = indicator_config.get("explanation", "")
        action = indicator_config.get("action", "")
    except Exception as e:
        logger.error(f"❌ [SCORE01] Fout bij berekenen score voor {name}: {e}")
        raise HTTPException(status_code=500, detail="❌ [SCORE01] Scoreberekening mislukt.")

    conn, cur = get_db_cursor()
    try:
        cur.execute("""
            INSERT INTO macro_data (name, value, trend, interpretation, action, score, timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            name,
            value,
            trend,
            interpretation,
            action,
            score,
            datetime.utcnow()
        ))
        conn.commit()
        logger.info(f"✅ [add] '{name}' opgeslagen met value={value}, score={score}, trend={trend}")
        return {"message": f"Indicator '{name}' succesvol opgeslagen."}
    except Exception as e:
        logger.error(f"❌ [DB02] Fout bij opslaan macro data: {e}")
        raise HTTPException(status_code=500, detail="❌ [DB02] Databasefout bij opslaan.")
    finally:
        conn.close()

@router.get("/macro_data")
async def get_macro_indicators():
    logger.info("📄 [get] Ophalen macro-indicatoren...")
    conn, cur = get_db_cursor()
    try:
        cur.execute("""
            SELECT id, name, value, trend, interpretation, action, score, timestamp
            FROM macro_data
            ORDER BY timestamp DESC
            LIMIT 100
        """)
        rows = cur.fetchall()
        return [
            {
                "id": row[0],
                "name": row[1],
                "value": row[2],
                "trend": row[3],
                "interpretation": row[4],
                "action": row[5],
                "score": row[6],
                "timestamp": row[7].isoformat() if row[7] else None
            }
            for row in rows
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
    logger.info("📄 [get/week] Ophalen macro-data (laatste 7 dagen)...")
    conn, cur = get_db_cursor()
    try:
        cur.execute("""
            SELECT name, value, trend, interpretation, action, score, timestamp
            FROM macro_data
            WHERE timestamp >= NOW() - INTERVAL '7 days'
            ORDER BY timestamp DESC
            LIMIT 50;
        """)
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
        logger.error(f"❌ [get/week] Databasefout: {e}")
        raise HTTPException(status_code=500, detail="❌ [DB07] Ophalen weekdata mislukt.")
    finally:
        conn.close()

@router.get("/macro_data/month")
async def get_macro_month_data():
    logger.info("📄 [get/month] Ophalen macro-data (laatste 30 dagen)...")
    conn, cur = get_db_cursor()
    try:
        cur.execute("""
            SELECT name, value, trend, interpretation, action, score, timestamp
            FROM macro_data
            WHERE timestamp >= NOW() - INTERVAL '30 days'
            ORDER BY timestamp DESC
            LIMIT 50;
        """)
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
        logger.error(f"❌ [get/month] Databasefout: {e}")
        raise HTTPException(status_code=500, detail="❌ [DB08] Ophalen maanddata mislukt.")
    finally:
        conn.close()

@router.get("/macro_data/quarter")
async def get_macro_quarter_data():
    logger.info("📄 [get/quarter] Ophalen macro-data (laatste 90 dagen)...")
    conn, cur = get_db_cursor()
    try:
        cur.execute("""
            SELECT name, value, trend, interpretation, action, score, timestamp
            FROM macro_data
            WHERE timestamp >= NOW() - INTERVAL '90 days'
            ORDER BY timestamp DESC
            LIMIT 50;
        """)
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
        logger.error(f"❌ [get/quarter] Databasefout: {e}")
        raise HTTPException(status_code=500, detail="❌ [DB09] Ophalen kwartaaldata mislukt.")
    finally:
        conn.close()
