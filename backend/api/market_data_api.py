import logging
import json
import httpx
from fastapi import APIRouter, HTTPException
from datetime import datetime
from utils.db import get_db_connection
from utils.market_interpreter import interpret_market_data

router = APIRouter(prefix="/api/market_data")
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

COINGECKO_API = "https://api.coingecko.com/api/v3/simple/price"
MARKET_CONFIG_PATH = "market_data_config.json"

# ✅ Helper voor DB-connectie met cursor
def get_db_cursor():
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="❌ [DB01] Databaseverbinding mislukt.")
    return conn, conn.cursor()

# ✅ POST: Marktdata ophalen en opslaan
@router.post("/save")
async def save_market_data():
    logger.info("📡 [save] Ophalen van BTC & SOL market data...")
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

        if "bitcoin" not in data or "solana" not in data:
            raise HTTPException(status_code=502, detail="❌ [API01] Onvolledige CoinGecko-data.")

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
        logger.error(f"❌ [save] CoinGecko API error: {e}")
        raise HTTPException(status_code=500, detail="❌ [API02] Fout bij ophalen van CoinGecko API.")

    conn, cur = get_db_cursor()
    try:
        for symbol, info in crypto_data.items():
            cur.execute("""
                INSERT INTO market_data (symbol, price, volume, change_24h, timestamp, is_updated)
                VALUES (%s, %s, %s, %s, NOW(), TRUE)
            """, (symbol, info["price"], info["volume"], info["change_24h"]))
        conn.commit()
        logger.info("✅ [save] Market data succesvol opgeslagen.")
        return {"message": "Market data succesvol opgeslagen."}
    except Exception as e:
        logger.error(f"❌ [save] Databasefout: {e}")
        raise HTTPException(status_code=500, detail="❌ [DB02] Fout bij opslaan van market data.")
    finally:
        conn.close()

# ✅ GET: Geïnterpreteerde BTC-marktdata
@router.get("/interpreted")
async def get_interpreted_market_data():
    logger.info("📊 [interpreted] Ophalen geïnterpreteerde BTC-data")
    conn, cur = get_db_cursor()

    try:
        with open(MARKET_CONFIG_PATH) as f:
            config = json.load(f)
    except Exception as e:
        logger.error(f"❌ [interpreted] Configfout: {e}")
        raise HTTPException(status_code=500, detail="❌ [CFG01] Configbestand ontbreekt of ongeldig.")

    try:
        cur.execute("""
            SELECT symbol, price, change_24h, timestamp
            FROM market_data
            WHERE symbol = 'BTC'
            ORDER BY timestamp DESC
            LIMIT 1
        """)
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="❌ [DATA01] Geen BTC data gevonden.")

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
        logger.error(f"❌ [interpreted] Verwerkingsfout: {e}")
        raise HTTPException(status_code=500, detail="❌ [DATA02] Fout bij ophalen interpretatie.")
    finally:
        conn.close()

# ✅ GET: Lijst met alle marktdata
@router.get("/list")
async def list_market_data():
    logger.info("📦 [list] Ophalen van marktdata lijst")
    conn, cur = get_db_cursor()
    try:
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
        logger.error(f"❌ [list] Databasefout: {e}")
        raise HTTPException(status_code=500, detail="❌ [DB03] Fout bij ophalen lijst.")
    finally:
        conn.close()

# ✅ GET: Root alias voor '/list' (optioneel)
@router.get("")
async def get_recent_market_data():
    return await list_market_data()

# ✅ GET: Test endpoint
@router.get("/test")
async def test_market_data():
    try:
        conn = get_db_connection()
        if not conn:
            raise HTTPException(status_code=500, detail="❌ [TEST01] Geen databaseverbinding.")
        conn.close()
        return {"message": "✅ Market Data API werkt correct."}
    except Exception as e:
        logger.error(f"❌ [TEST] Fout bij test: {e}")
        raise HTTPException(status_code=500, detail="❌ [TEST02] Interne fout.")
