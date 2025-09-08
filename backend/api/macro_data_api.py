import logging
import json
import os
from datetime import datetime
from fastapi import APIRouter, HTTPException, Request
from backend.utils.db import get_db_connection
from backend.utils.macro_interpreter import process_macro_indicator

# ✅ Logger instellen
router = APIRouter()
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ✅ Absoluut pad naar configmap (werkt altijd)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, "..", "config", "macro_indicators_config.json")



def get_db_cursor():
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="❌ [DB01] Geen databaseverbinding.")
    return conn, conn.cursor()


# ✅ POST: Macro-indicator toevoegen op basis van config
@router.post("/macro_data")
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
        
        # ✅ Extra check op waarde
        try:
            value = float(result.get("value"))
        except (TypeError, ValueError):
            raise ValueError(f"❌ Ongeldige waarde voor indicator '{name}': {result.get('value')}")
        
    except Exception as e:
        logger.error(f"❌ [INT01] Interpreterfout: {e}")
        raise HTTPException(status_code=500, detail=f"❌ [INT01] Verwerking indicator mislukt: {e}")

    score = result.get("score", 0)

    # Opslaan in database
    conn, cur = get_db_cursor()
    try:
        cur.execute("""
            INSERT INTO macro_data (name, value, trend, interpretation, action, score, timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            result["name"],
            value,
            "",  # trend eventueel later berekenen
            result["interpretation"],
            result["action"],
            score,
            datetime.utcnow()
        ))
        conn.commit()
        logger.info(f"✅ [add] '{name}' opgeslagen met waarde {value} en score {score}")
        return {"message": f"Indicator '{name}' succesvol opgeslagen."}
    except Exception as e:
        logger.error(f"❌ [DB02] Fout bij opslaan macro data: {e}")
        raise HTTPException(status_code=500, detail="❌ [DB02] Databasefout bij opslaan.")
    finally:
        conn.close()


# ✅ GET: Laatste macro-indicatoren ophalen
@router.get("/macro_data")
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
@router.get("/macro_data/list")
async def get_macro_data_list():
    return await get_macro_indicators()


# ✅ DELETE: Macro-indicator verwijderen op basis van naam
@router.delete("/macro_data/{name}")
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


# ✅ PATCH: Macro-indicator bijwerken (waarde)
@router.patch("/macro_data/{name}")
async def update_macro_value(name: str, request: Request):
    logger.info(f"✏️ [patch] Bijwerken van '{name}'...")
    conn, cur = get_db_cursor()
    try:
        data = await request.json()
        value = float(data.get("value"))

        cur.execute("UPDATE macro_data SET value = %s, timestamp = %s WHERE name = %s RETURNING id;",
                    (value, datetime.utcnow(), name))
        updated = cur.fetchone()
        if not updated:
            raise HTTPException(status_code=404, detail=f"Indicator '{name}' niet gevonden.")
        conn.commit()
        logger.info(f"✅ [patch] Indicator '{name}' bijgewerkt naar {value}")
        return {"message": f"{name} bijgewerkt naar {value}."}
    except Exception as e:
        logger.error(f"❌ [patch] Bijwerken mislukt: {e}")
        raise HTTPException(status_code=500, detail="❌ [DB05] Bijwerken mislukt.")
    finally:
        conn.close()
