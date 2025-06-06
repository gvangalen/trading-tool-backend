import logging
import httpx
from fastapi import APIRouter, HTTPException
from datetime import datetime
from utils.db import get_db_connection

router = APIRouter(prefix="/api/market_data")
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

COINGECKO_URL = "https://api.coingecko.com/api/v3/coins/{id}/ohlc?vs_currency=usd&days=1"
VOLUME_URL = "https://api.coingecko.com/api/v3/coins/{}?localization=false&tickers=false&market_data=true"

ASSETS = {
    "BTC": "bitcoin",
    "SOL": "solana"
}

# ✅ DB helper
def get_db_cursor():
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="❌ [DB] Geen databaseverbinding.")
    return conn, conn.cursor()

# ✅ POST: Haal OHLC-data + volume op en sla op
@router.post("/save")
async def save_market_data():
    logger.info("📡 [save] Ophalen van marktdata via CoinGecko...")
    try:
        crypto_data = {}
        async with httpx.AsyncClient(timeout=10) as client:
            for symbol, coingecko_id in ASSETS.items():
                # OHLC voor prijs
                url = COINGECKO_URL.format(id=coingecko_id)
                response = await client.get(url)
                response.raise_for_status()
                ohlc = response.json()

                if not ohlc:
                    raise HTTPException(status_code=502, detail=f"❌ [API] Geen OHLC-data voor {symbol}")

                latest = ohlc[-1]  # [timestamp, open, high, low, close]
                open_, high, low, close = map(float, latest[1:5])
                change = ((close - open_) / open_) * 100

                # Volume ophalen
                vol_response = await client.get(VOLUME_URL.format(coingecko_id))
                vol_response.raise_for_status()
                market_data = vol_response.json()
                volume = market_data.get("market_data", {}).get("total_volume", {}).get("usd", None)

                crypto_data[symbol] = {
                    "price": close,
                    "open": open_,
                    "high": high,
                    "low": low,
                    "change_24h": round(change, 2),
                    "volume": float(volume) if volume else None
                }

    except Exception as e:
        logger.error(f"❌ [save] Fout bij ophalen van CoinGecko-data: {e}")
        raise HTTPException(status_code=500, detail="❌ Fout bij ophalen van marktdata.")

    conn, cur = get_db_cursor()
    try:
        for symbol, data in crypto_data.items():
            cur.execute("""
                INSERT INTO market_data (symbol, price, open, high, low, change_24h, volume, timestamp, is_updated)
                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), TRUE)
            """, (
                symbol,
                data["price"],
                data["open"],
                data["high"],
                data["low"],
                data["change_24h"],
                data["volume"]
            ))
        conn.commit()
        logger.info("✅ [save] Marktdata succesvol opgeslagen.")
        return {"message": "✅ Marktdata opgeslagen"}
    except Exception as e:
        logger.error(f"❌ [save] DB-fout: {e}")
        raise HTTPException(status_code=500, detail="❌ DB-fout bij opslaan.")
    finally:
        conn.close()

# ✅ GET: Laatste 100 entries
@router.get("/list")
async def list_market_data():
    logger.info("📦 [list] Ophalen van marktdata lijst...")
    conn, cur = get_db_cursor()
    try:
        cur.execute("""
            SELECT symbol, price, open, high, low, change_24h, volume, timestamp
            FROM market_data
            ORDER BY timestamp DESC
            LIMIT 100
        """)
        rows = cur.fetchall()
        return [
            {
                "symbol": row[0],
                "price": float(row[1]) if row[1] is not None else None,
                "open": float(row[2]) if row[2] is not None else None,
                "high": float(row[3]) if row[3] is not None else None,
                "low": float(row[4]) if row[4] is not None else None,
                "change_24h": float(row[5]) if row[5] is not None else None,
                "volume": float(row[6]) if row[6] is not None else None,
                "timestamp": row[7].isoformat() if row[7] else None
            }
            for row in rows
        ]
    except Exception as e:
        logger.error(f"❌ [list] DB-fout: {e}")
        raise HTTPException(status_code=500, detail="❌ Fout bij ophalen data.")
    finally:
        conn.close()

# ✅ GET: Root alias → `/api/market_data`
@router.get("")
async def get_recent_market_data():
    return await list_market_data()

# ✅ GET: Test endpoint
@router.get("/test")
async def test_market_data():
    try:
        conn = get_db_connection()
        if not conn:
            raise HTTPException(status_code=500, detail="❌ Geen databaseverbinding.")
        conn.close()
        return {"message": "✅ Market Data API werkt correct."}
    except Exception as e:
        logger.error(f"❌ [test] Interne fout: {e}")
        raise HTTPException(status_code=500, detail="❌ Test endpoint faalde.")
