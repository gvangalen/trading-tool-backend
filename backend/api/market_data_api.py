# market_data_api.py
from fastapi import APIRouter, HTTPException
import httpx
import logging
from db import get_db_connection

router = APIRouter()

COINGECKO_API = "https://api.coingecko.com/api/v3/simple/price"

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
        logging.error(f"❌ CoinGecko API error: {e}")
        raise HTTPException(status_code=500, detail="CoinGecko API fout")

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Geen databaseverbinding")

    try:
        with conn.cursor() as cur:
            for symbol, info in crypto_data.items():
                cur.execute("""
                    INSERT INTO market_data (symbol, price, volume, change_24h, timestamp, is_updated)
                    VALUES (%s, %s, %s, %s, NOW(), TRUE)
                """, (symbol, info["price"], info["volume"], info["change_24h"]))
            conn.commit()
        logging.info("✅ Marktdata succesvol opgeslagen")
        return {"message": "Marktdata succesvol opgeslagen"}
    except Exception as e:
        logging.error(f"❌ Database error: {e}")
        raise HTTPException(status_code=500, detail="Databasefout bij opslaan marktdata")
    finally:
        conn.close()

@router.get("/api/market_data/test")
async def test_market_data():
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Databaseverbinding mislukt")
    conn.close()
    return {"message": "✅ Market Data API werkt correct"}
