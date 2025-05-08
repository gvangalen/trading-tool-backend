# ✅ market_data_api.py — FastAPI router voor marktdata (opslaan, interpreteren, ophalen)

import logging
import json
import httpx
from fastapi import APIRouter, HTTPException
from datetime import datetime
from utils.db import get_db_connection
from utils.market_interpreter import interpret_market_data

router = APIRouter()

# ✅ Basisinstellingen
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

COINGECKO_API = "https://api.coingecko.com/api/v3/simple/price"
MARKET_CONFIG_PATH = "market_data_config.json"


# ✅ POST: Marktdata ophalen en opslaan (BTC en SOL)
@router.post("/api/market_data/save")
async def save_market_data():
    try:
        params = {
            "ids": "bitcoin,solana",
            "vs_currencies": "usd",
            "include_market_cap": "true",
            "include_24hr_vol": "true",
            "include_24hr_change": "true"
        }
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.get(COINGECKO_API, params=params)
            response.raise_for_status()
            data = response.json()

        crypto_data = {
            "BTC": {
                "price": data["bitcoin"]["usd"],
                "volume": data["bitcoin"]["usd_24h_vol"],
                "change_24h": data["bitcoin"]["usd_24h_change"],
            },
            "SOL": {
                "price": data["solana"]["usd"],
                "volume": data["solana"]["usd_24h_vol"],
                "change_24h": data["solana"]["usd_24h_change"],
            }
        }

    except Exception as e:
        logger.error(f"❌ CoinGecko API error: {e}")
        raise HTTPException(status_code=500, detail="Error fetching data from CoinGecko API.")

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Failed to connect to the database.")

    try:
        with conn.cursor() as cur:
            for symbol, info in crypto_data.items():
                cur.execute("""
                    INSERT INTO market_data (symbol, price, volume, change_24h, timestamp, is_updated)
                    VALUES (%s, %s, %s, %s, NOW(), TRUE)
                """, (symbol, info["price"], info["volume"], info["change_24h"]))
            conn.commit()

        logger.info("✅ Market data successfully saved.")
        return {"message": "Market data successfully saved."}

    except Exception as e:
        logger.error(f"❌ Database error while saving market data: {e}")
        raise HTTPException(status_code=500, detail="Database error while saving market data.")

    finally:
        conn.close()


# ✅ GET: Geïnterpreteerde marktdata ophalen (BTC)
@router.get("/api/market_data/interpreted")
async def get_interpreted_market_data():
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Databaseverbinding mislukt.")

    try:
        with open(MARKET_CONFIG_PATH) as f:
            config = json.load(f)
    except Exception as e:
        logger.error(f"❌ Config laden mislukt: {e}")
        raise HTTPException(status_code=500, detail="Configbestand ontbreekt of is ongeldig.")

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT symbol, price, change_24h, timestamp
                FROM market_data
                WHERE symbol = 'BTC'
                ORDER BY timestamp DESC
                LIMIT 1
            """)
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Geen marktdata gevonden.")

            raw_data = {
                "price": float(row[1]),
                "change_24h": float(row[2])
            }

            interpreted = interpret_market_data(raw_data, config)

            return {
                "symbol": row[0],
                "price": raw_data["price"],
                "change_24h": raw_data["change_24h"],
                "score": interpreted["score"],
                "labels": interpreted["labels"],
                "timestamp": row[3].isoformat() if row[3] else None
            }

    except Exception as e:
        logger.error(f"❌ Fout bij ophalen marktdata: {e}")
        raise HTTPException(status_code=500, detail="Fout bij ophalen marktdata.")
    finally:
        conn.close()


# ✅ GET: Alle opgeslagen marktdata ophalen (BTC en SOL)
@router.get("/api/market_data/list")
async def list_market_data():
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Databaseverbinding mislukt.")

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT symbol, price, volume, change_24h, timestamp
                FROM market_data
                ORDER BY timestamp DESC
                LIMIT 100
            """)
            rows = cur.fetchall()

        data = [
            {
                "symbol": row[0],
                "price": float(row[1]),
                "volume": float(row[2]),
                "change_24h": float(row[3]),
                "timestamp": row[4].isoformat() if row[4] else None
            }
            for row in rows
        ]

        return data

    except Exception as e:
        logger.error(f"❌ Fout bij ophalen market data lijst: {e}")
        raise HTTPException(status_code=500, detail="Databasefout.")
    finally:
        conn.close()


# ✅ Test endpoint
@router.get("/api/market_data/test")
async def test_market_data():
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Failed to connect to the database.")
    conn.close()
    return {"message": "✅ Market Data API is working correctly."}
