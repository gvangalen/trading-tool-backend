import logging
import json
from datetime import datetime
from fastapi import APIRouter, HTTPException, Request
from utils.db import get_db_connection
from utils.macro_interpreter import process_macro_indicator
from celery_task.macro_task import fetch_macro_data  # ✅ Celery-taak importeren

router = APIRouter(prefix="/macro_data")
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

CONFIG_PATH = "macro_indicators_config.json"

def get_db_cursor():
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="❌ [DB01] Geen databaseverbinding.")
    return conn, conn.cursor()

# ✅ POST: Macro-indicator toevoegen op basis van config
@router.post("/")
async def add_macro_indicator(request: Request):
    logger.info("📥 [add] Nieuwe macro-indicator toevoegen...")
    data = await request.json()
    name = data.get("name")

    if not name:
        raise HTTPException(status_code=400, detail="❌ [REQ01] Naam van indicator is verplicht.")

    # Config laden
    try:
        with open(CONFIG_PATH) as f:
            config = json.load(f)
    except Exception as e:
        logger.error(f"❌ [CFG01] Config laden mislukt: {e}")
        raise HTTPException(status_code=500, detail="❌ [CFG01] Configbestand ongeldig of ontbreekt.")

    if name not in config:
        raise HTTPException(status_code=400, detail=f"❌ [CFG02] Indicator '{name}' niet gevonden in config.")

    # Interpreter uitvoeren
    try:
        result = await process_macro_indicator(name, config[name])
        if not result or "value" not in result or "interpretation" not in result or "action" not in result:
            raise ValueError("❌ Interpreterresultaat incompleet")
    except Exception as e:
        logger.error(f"❌ [INT01] Interpreterfout: {e}")
        raise HTTPException(status_code=500, detail="❌ [INT01] Verwerking indicator mislukt.")

    score = result.get("score", 0)

    # Opslaan in database
    conn, cur = get_db_cursor()
    try:
        cur.execute("""
            INSERT INTO macro_data (name, value, trend, interpretation, action, score, timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            result["name"],
            result["value"],
            "",  # trend eventueel later berekenen
            result["interpretation"],
            result["action"],
            score,
            datetime.utcnow()
        ))
        conn.commit()
        logger.info(f"✅ [add] '{name}' opgeslagen met waarde {result['value']} en score {score}")
        return {"message": f"Indicator '{name}' succesvol opgeslagen."}
    except Exception as e:
        logger.error(f"❌ [DB02] Fout bij opslaan macro data: {e}")
        raise HTTPException(status_code=500, detail="❌ [DB02] Databasefout bij opslaan.")
    finally:
        conn.close()

# ✅ GET: Laatste macro-indicatoren ophalen
@router.get("/")
async def get_macro_indicators():
    logger.info("📤 [get] Ophalen macro-indicatoren...")
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

# ✅ Alias: /macro_data/list → fallback route voor frontend
@router.get("/list")
async def get_macro_data_list():
    return await get_macro_indicators()

# ✅ DELETE: Macro-indicator verwijderen op basis van naam
@router.delete("/{name}")
async def delete_macro_indicator(name: str):
    logger.info(f"🗑️ [delete] Probeer macro-indicator '{name}' te verwijderen...")
    conn, cur = get_db_cursor()
    try:
        cur.execute("DELETE FROM macro_data WHERE name = %s RETURNING id;", (name,))
        deleted = cur.fetchone()
        if not deleted:
            raise HTTPException(status_code=404, detail=f"Indicator '{name}' niet gevonden.")
        conn.commit()
        logger.info(f"✅ [delete] Indicator '{name}' verwijderd")
        return {"message": f"Indicator '{name}' verwijderd."}
    except Exception as e:
        logger.error(f"❌ [delete] Verwijderen mislukt: {e}")
        raise HTTPException(status_code=500, detail="❌ [DB04] Verwijderen mislukt.")
    finally:
        conn.close()

# ✅ POST: Start Celery-task om macrodata automatisch op te halen
@router.post("/trigger")
def trigger_macro_data_task():
    """
    Start een achtergrondtaak om macrodata op te halen via Celery.
    """
    fetch_macro_data.delay()
    logger.info("🚀 Celery-taak 'fetch_macro_data' gestart via API.")
    return {"message": "📡 Macrodata taak gestart via Celery."}
