import logging
from datetime import datetime, timedelta
from fastapi import APIRouter, HTTPException, Request
from backend.utils.db import get_db_connection
from backend.utils.macro_interpreter import process_macro_indicator
from backend.config.config_loader import load_macro_config

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

def validate_macro_config(name: str, config: dict, full_config: dict):
    symbol = config.get("symbol")
    source = config.get("source")
    base_urls = full_config.get("base_urls", {})

    if not symbol or not source:
        raise ValueError(f"❌ [CFG03] 'symbol' of 'source' ontbreekt in config voor '{name}'")

    if source not in base_urls:
        raise ValueError(f"❌ [CFG04] Geen base_url gedefinieerd voor source '{source}'")

    api_url = base_urls[source].format(symbol=symbol)
    return api_url

@router.post("/macro_data")
async def add_macro_indicator(request: Request):
    logger.info("📥 [add] Nieuwe macro-indicator toevoegen...")
    data = await request.json()
    name = data.get("name")
    if not name:
        raise HTTPException(status_code=400, detail="❌ [REQ01] Naam van indicator is verplicht.")

    config_data = MACRO_CONFIG
    if name not in config_data.get("indicators", {}):
        raise HTTPException(status_code=400, detail=f"❌ [CFG02] Indicator '{name}' niet gevonden in config.")

    indicator_config = config_data["indicators"][name]
    try:
        _ = validate_macro_config(name, indicator_config, config_data)
    except Exception as e:
        logger.error(str(e))
        raise HTTPException(status_code=500, detail=str(e))

    try:
        if all(k in data for k in ("value", "score", "interpretation", "action")):
            logger.info(f"📥 [add] Externe data ontvangen voor '{name}'")
            result = {
                "name": name,
                "value": data["value"],
                "score": data["score"],
                "interpretation": data["interpretation"],
                "action": data["action"],
            }
        else:
            logger.info(f"⚙️ [add] Geen externe data, interpreter wordt aangeroepen voor '{name}'")
            result = await process_macro_indicator(name, indicator_config)

        if not result or "value" not in result or "score" not in result or "interpretation" not in result or "action" not in result:
            raise ValueError("❌ Interpreterresultaat incompleet")

        value = float(result.get("value"))
        score = result.get("score")

    except Exception as e:
        logger.error(f"❌ [INT01] Interpreterfout: {e}")
        raise HTTPException(status_code=500, detail=f"❌ [INT01] Verwerking indicator mislukt: {e}")

    conn, cur = get_db_cursor()
    try:
        cur.execute("""
            INSERT INTO macro_data (name, value, trend, interpretation, action, score, timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            result["name"],
            value,
            result.get("trend", ""),
            result["interpretation"],
            result["action"],
            score,
            datetime.utcnow()
        ))
        conn.commit()
        logger.info(f"✅ [add] '{name}' opgeslagen met waarde {value}, score {score}, trend {result.get('trend', '')}")
        return {"message": f"Indicator '{name}' succesvol opgeslagen."}
    except Exception as e:
        logger.error(f"❌ [DB02] Fout bij opslaan macro data: {e}")
        raise HTTPException(status_code=500, detail="❌ [DB02] Databasefout bij opslaan.")
    finally:
        conn.close()

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

@router.get("/macro_data/list")
async def get_macro_data_list():
    return await get_macro_indicators()

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


# ✅ GET: Macro-data per periode (gebaseerd op timestamp, niet meer op timeframe)

@router.get("/macro_data/day")
async def get_macro_day_data():
    logger.info("📤 [get/day] Ophalen macro-data (laatste 24 uur)...")
    conn, cur = get_db_cursor()
    try:
        cur.execute("""
            SELECT name, value, trend, interpretation, action, score, timestamp
            FROM macro_data
            WHERE timestamp >= NOW() - INTERVAL '1 day'
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
        logger.error(f"❌ [get/day] Databasefout: {e}")
        raise HTTPException(status_code=500, detail="❌ [DB06] Ophalen dagdata mislukt.")
    finally:
        conn.close()


@router.get("/macro_data/week")
async def get_macro_week_data():
    logger.info("📤 [get/week] Ophalen macro-data (laatste 7 dagen)...")
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
    logger.info("📤 [get/month] Ophalen macro-data (laatste 30 dagen)...")
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
    logger.info("📤 [get/quarter] Ophalen macro-data (laatste 90 dagen)...")
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
