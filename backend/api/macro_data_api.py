import logging
import json
from datetime import datetime
from fastapi import APIRouter, HTTPException, Request
from utils.db import get_db_connection
from utils.macro_interpreter import process_macro_indicator

router = APIRouter()
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

CONFIG_PATH = "macro_indicators_config.json"

# ✅ DB helper
def get_db_cursor():
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="❌ [DB01] Geen databaseverbinding.")
    return conn, conn.cursor()

# ✅ POST: Macro-indicator toevoegen
@router.post("/api/macro_data")
async def add_macro_indicator(request: Request):
    logger.info("📥 [add] Nieuwe macro-indicator toevoegen...")
    data = await request.json()
    name = data.get("name")

    if not name:
        raise HTTPException(status_code=400, detail="❌ [REQ01] Naam van indicator is verplicht.")

    try:
        with open(CONFIG_PATH) as f:
            config = json.load(f)
    except Exception as e:
        logger.error(f"❌ [CFG01] Config laden mislukt: {e}")
        raise HTTPException(status_code=500, detail="❌ [CFG01] Configbestand ongeldig of ontbreekt.")

    if name not in config:
        raise HTTPException(status_code=400, detail=f"❌ [CFG02] Indicator '{name}' niet gevonden in config.")

    try:
        result = await process_macro_indicator(name, config[name])
        if not result or "value" not in result or "interpretation" not in result or "action" not in result:
            raise ValueError("Incomplete result from macro interpreter")
    except Exception as e:
        logger.error(f"❌ [INT01] Interpreterfout: {e}")
        raise HTTPException(status_code=500, detail="❌ [INT01] Verwerking macro-indicator mislukt.")

    conn, cur = get_db_cursor()
    try:
        cur.execute("""
            INSERT INTO macro_data (name, value, trend, interpretation, action, timestamp)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            result["name"],
            result["value"],
            "",  # trend wordt later berekend of geüpdatet
            result["interpretation"],
            result["action"],
            datetime.utcnow()
        ))
        conn.commit()
        logger.info(f"✅ [add] '{name}' opgeslagen met waarde {result['value']}")
        return {"message": f"Indicator '{name}' succesvol opgeslagen."}

    except Exception as e:
        logger.error(f"❌ [DB02] Fout bij opslaan macro data: {e}")
        raise HTTPException(status_code=500, detail="❌ [DB02] Databasefout bij opslaan.")
    finally:
        conn.close()

# ✅ GET: Macro-indicatoren ophalen
@router.get("/api/macro_data")
async def get_macro_indicators():
    logger.info("📤 [get] Ophalen macro-indicatoren...")
    conn, cur = get_db_cursor()
    try:
        cur.execute("""
            SELECT id, name, value, trend, interpretation, action, timestamp
            FROM macro_data
            ORDER BY timestamp DESC
            LIMIT 100
        """)
        rows = cur.fetchall()
        result = [
            {
                "id": row[0],
                "name": row[1],
                "value": row[2],
                "trend": row[3],
                "interpretation": row[4],
                "action": row[5],
                "timestamp": row[6].isoformat() if row[6] else None
            }
            for row in rows
        ]
        return result
    except Exception as e:
        logger.error(f"❌ [get] Databasefout: {e}")
        raise HTTPException(status_code=500, detail="❌ [DB03] Ophalen macro-data mislukt.")
    finally:
        conn.close()
