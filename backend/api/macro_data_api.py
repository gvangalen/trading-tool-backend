# macro_data_api.py — FastAPI API voor macro-indicatoren

import logging
import json
from datetime import datetime
from fastapi import APIRouter, HTTPException, Request
from utils.db import get_db_connection  ✅  # correct
from utils.macro_interpreter import process_macro_indicator

router = APIRouter()

# ✅ Logging instellen
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ✅ Pad naar configuratiebestand
CONFIG_PATH = "macro_indicators_config.json"

# ✅ POST: Macro-indicator toevoegen
@router.post("/api/macro_data")
async def add_macro_indicator(request: Request):
    data = await request.json()
    name = data.get("name")
    if not name:
        raise HTTPException(status_code=400, detail="Indicator name is required.")

    try:
        with open(CONFIG_PATH) as f:
            config = json.load(f)
    except Exception as e:
        logger.error(f"❌ Config laden mislukt: {e}")
        raise HTTPException(status_code=500, detail="Failed to load macro config.")

    if name not in config:
        raise HTTPException(status_code=400, detail=f"Indicator '{name}' not found in config.")

    try:
        result = await process_macro_indicator(name, config[name])
    except Exception as e:
        logger.error(f"❌ Verwerking indicator mislukt: {e}")
        raise HTTPException(status_code=500, detail="Failed to process macro indicator.")

    if not result:
        raise HTTPException(status_code=500, detail="No result from interpreter")

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed.")

    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO macro_data (name, value, trend, interpretation, action, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (result["name"], result["value"], "", result["interpretation"], result["action"], datetime.utcnow()))
            conn.commit()

        logger.info(f"✅ '{name}' opgeslagen met waarde {result['value']}")
        return {"message": f"Indicator '{name}' successfully saved."}

    except Exception as e:
        logger.error(f"❌ Databasefout bij opslaan: {e}")
        raise HTTPException(status_code=500, detail="Database error while saving macro indicator.")
    finally:
        conn.close()

# ✅ GET: Macro-indicatoren ophalen
@router.get("/api/macro_data")
async def get_macro_indicators():
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed.")

    try:
        with conn.cursor() as cur:
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
        logger.error(f"❌ Databasefout bij ophalen: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch macro indicators.")
    finally:
        conn.close()
