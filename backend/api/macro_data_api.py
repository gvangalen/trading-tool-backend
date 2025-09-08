import logging
from datetime import datetime
from fastapi import APIRouter, HTTPException, Request, Query
from backend.utils.db import get_db_connection
from backend.utils.macro_interpreter import process_macro_indicator
from backend.config.config_loader import load_macro_config

router = APIRouter()
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def get_db_cursor():
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="❌ [DB01] Geen databaseverbinding.")
    return conn, conn.cursor()


def validate_macro_config(name: str, config: dict, full_config: dict):
    """
    ✅ Valideer macroconfig en geef API-URL terug
    """
    symbol = config.get("symbol")
    source = config.get("source")
    base_urls = full_config.get("base_urls", {})

    if not symbol or not source:
        raise ValueError(f"❌ [CFG03] 'symbol' of 'source' ontbreekt in config voor '{name}'")

    if source not in base_urls:
        raise ValueError(f"❌ [CFG04] Geen base_url gedefinieerd voor source '{source}'")

    api_url = base_urls[source].format(symbol=symbol)
    return api_url


# ✅ POST: Macro-indicator toevoegen op basis van config
@router.post("/macro_data")
async def add_macro_indicator(request: Request):
    logger.info("📥 [add] Nieuwe macro-indicator toevoegen...")
    data = await request.json()
    name = data.get("name")

    if not name:
        raise HTTPException(status_code=400, detail="❌ [REQ01] Naam van indicator is verplicht.")

    try:
        config_data = load_macro_config()
    except Exception as e:
        logger.error(f"❌ [CFG01] Config laden mislukt: {e}")
        raise HTTPException(status_code=500, detail=f"❌ [CFG01] Configbestand ongeldig of ontbreekt: {e}")

    if name not in config_data.get("indicators", {}):
        raise HTTPException(status_code=400, detail=f"❌ [CFG02] Indicator '{name}' niet gevonden in config.")

    indicator_config = config_data["indicators"][name]

    # ✅ Valideer config + genereer API-URL
    try:
        _ = validate_macro_config(name, indicator_config, config_data)
    except Exception as e:
        logger.error(str(e))
        raise HTTPException(status_code=500, detail=str(e))

    # ✅ Interpreter aanroepen
    try:
        result = await process_macro_indicator(name, indicator_config)
        if not result or "value" not in result or "interpretation" not in result or "action" not in result:
            raise ValueError("❌ Interpreterresultaat incompleet")

        try:
            value = float(result.get("value"))
        except (TypeError, ValueError):
            raise ValueError(f"❌ Ongeldige waarde voor indicator '{name}': {result.get('value')}")

    except Exception as e:
        logger.error(f"❌ [INT01] Interpreterfout: {e}")
        raise HTTPException(status_code=500, detail=f"❌ [INT01] Verwerking indicator mislukt: {e}")

    score = result.get("score", 0)

    # ✅ Opslaan in DB
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
