# ✅ macro_data_api.py — FastAPI version

import logging
import json
import httpx
from fastapi import APIRouter, HTTPException, Request
from datetime import datetime
from db import get_db_connection

router = APIRouter()

# ✅ Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ✅ Path to macro indicators configuration
CONFIG_PATH = "macro_indicators_config.json"

# ✅ Helper: Extract value from nested JSON path
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
        logger.error(f"❌ Failed to extract value for path '{path}': {e}")
        return None

# ✅ Helper: Interpret value according to defined rules
def interpret_value(value, rules):
    for rule in sorted(rules, key=lambda x: -x["threshold"]):
        if value >= rule["threshold"]:
            return rule["interpretation"], rule["action"]
    return "Unknown", "No action"

# ✅ POST: Add a macro indicator
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
        logger.error(f"❌ Failed to load config: {e}")
        raise HTTPException(status_code=500, detail="Failed to load configuration file.")

    if name not in config:
        raise HTTPException(status_code=400, detail=f"Indicator '{name}' not found in config.")

    indicator = config[name]
    api_url = indicator.get("api_url")
    extract_key = indicator.get("extract_key")
    rules = indicator.get("interpretation_rules", [])

    if not api_url or not extract_key:
        raise HTTPException(status_code=400, detail="Invalid configuration for this indicator.")

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.get(api_url)
            response.raise_for_status()
            json_data = response.json()
    except Exception as e:
        logger.error(f"❌ Failed API call to {api_url}: {e}")
        raise HTTPException(status_code=500, detail="API call failed.")

    # Special case for custom DXY calculation
    if extract_key == "custom_dxy_calculation":
        try:
            rates = json_data["rates"]
            basket = ["EUR", "GBP", "JPY", "CAD", "SEK", "CHF"]
            value = sum(rates.get(cur, 1) for cur in basket) / len(basket)
        except Exception as e:
            logger.error(f"❌ DXY calculation failed: {e}")
            raise HTTPException(status_code=500, detail="DXY calculation failed.")
    else:
        value = extract_from_path(json_data, extract_key)

    if value is None:
        raise HTTPException(status_code=500, detail="Failed to extract value from API response.")

    interpretation, action = interpret_value(value, rules)

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed.")

    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO macro_data (name, value, trend, interpretation, action, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (name, value, "", interpretation, action, datetime.utcnow()))
            conn.commit()

        logger.info(f"✅ Indicator '{name}' successfully saved with value {value}")
        return {"message": f"Indicator '{name}' successfully saved."}

    except Exception as e:
        logger.error(f"❌ Database error while saving macro indicator: {e}")
        raise HTTPException(status_code=500, detail="Database error while saving macro indicator.")

    finally:
        conn.close()

# ✅ GET: Retrieve macro indicators
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
        logger.info(f"📈 Retrieved {len(indicators)} macro indicators.")
        return indicators

    except Exception as e:
        logger.error(f"❌ Error retrieving macro indicators: {e}")
        raise HTTPException(status_code=500, detail="Database error while retrieving macro indicators.")

    finally:
        conn.close()
