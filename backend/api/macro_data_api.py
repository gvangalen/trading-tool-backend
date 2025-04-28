# macro_data_api.py
import logging
import json
import httpx
from fastapi import APIRouter, HTTPException
from datetime import datetime
from db import get_db_connection

router = APIRouter()

CONFIG_PATH = "macro_indicators_config.json"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ✅ Helper: extractie met dot-notatie
def extract_from_path(data, path):
    try:
        keys = path.split(".")
        for key in keys:
            if isinstance(data, list) and key.isdigit():
                data = data[int(key)]
            else:
                data = data.get(key)
        return float(data)
    except Exception as e:
        logger.error(f"❌ Extractie mislukt voor path '{path}': {e}")
        return None

# ✅ Helper: interpretatie en actie bepalen
def interpret_value(value, rules):
    for rule in sorted(rules, key=lambda x: -x["threshold"]):
        if value >= rule["threshold"]:
            return rule["interpretation"], rule["action"]
    return "Onbekend", "Geen actie"

# ✅ POST: Macro-indicator ophalen en opslaan
@router.post("/api/macro_data")
async def add_macro_indicator(request_data: dict):
    name = request_data.get("name")
    if not name:
        raise HTTPException(status_code=400, detail="Naam van indicator is vereist")

    try:
        with open(CONFIG_PATH) as f:
            config = json.load(f)
    except Exception as e:
        logger.error(f"❌ Config laden mislukt: {e}")
        raise HTTPException(status_code=500, detail="Kan configuratie niet laden")

    if name not in config:
        raise HTTPException(status_code=400, detail=f"Indicator '{name}' niet gevonden in config")

    indicator = config[name]
    api_url = indicator.get("api_url")
    extract_key = indicator.get("extract_key")
    rules = indicator.get("interpretation_rules", [])

    if not api_url or not extract_key:
        raise HTTPException(status_code=400, detail="Ongeldige configuratie voor deze indicator")

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.get(api_url)
            response.raise_for_status()
            json_data = response.json()
    except Exception as e:
        logger.error(f"❌ API-call naar {api_url} mislukt: {e}")
        raise HTTPException(status_code=500, detail="API-call mislukt")

    if extract_key == "custom_dxy_calculation":
        try:
            rates = json_data["rates"]
            basket = ["EUR", "GBP", "JPY", "CAD", "SEK", "CHF"]
            value = sum(rates.get(cur, 1) for cur in basket) / len(basket)
        except Exception as e:
            logger.error(f"❌ DXY-berekening mislukt: {e}")
            raise HTTPException(status_code=500, detail="DXY-berekening mislukt")
    else:
        value = extract_from_path(json_data, extract_key)

    if value is None:
        raise HTTPException(status_code=500, detail="Kan waarde niet extraheren")

    interpretation, action = interpret_value(value, rules)

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Geen databaseverbinding")

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO macro_data (name, value, trend, interpretation, action, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (name, value, "", interpretation, action, datetime.utcnow())
            )
            conn.commit()
        logger.info(f"✅ Indicator '{name}' succesvol opgeslagen met waarde {value}")
        return {"message": f"Indicator '{name}' succesvol opgeslagen"}
    except Exception as e:
        logger.error(f"❌ Databasefout bij opslaan: {e}")
        raise HTTPException(status_code=500, detail="Databasefout bij opslaan")
    finally:
        conn.close()

# ✅ GET: Macro-indicatoren ophalen
@router.get("/api/macro_data")
async def get_macro_indicators():
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Geen databaseverbinding")

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, name, value, trend, interpretation, action, timestamp
                FROM macro_data
                ORDER BY timestamp DESC
                LIMIT 100
            """)
            rows = cur.fetchall()

        indicators = [
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
        return indicators
    except Exception as e:
        logger.error(f"❌ Fout bij ophalen macro-indicatoren: {e}")
        raise HTTPException(status_code=500, detail="Databasefout bij ophalen")
    finally:
        conn.close()
