import logging
from fastapi import APIRouter, HTTPException
from backend.utils.db import get_db_connection  # ✅ juist
from backend.config.settings import COINGECKO_URL, VOLUME_URL, ASSETS
import httpx

router = APIRouter()  # ✅ Geen prefix — routes zijn volledig gedefinieerd

logger = logging.getLogger(__name__)


@router.get("/market_data/list")
async def list_market_data():
    try:
        conn, cur = get_db_cursor()
        cur.execute("""
            SELECT id, symbol, price, open, high, low, change_24h, volume, timestamp
            FROM market_data
            ORDER BY timestamp DESC
        """)
        rows = cur.fetchall()
        conn.close()

        data = [
            {
                "id": r[0],
                "symbol": r[1],
                "price": r[2],
                "open": r[3],
                "high": r[4],
                "low": r[5],
                "change_24h": r[6],
                "volume": r[7],
                "timestamp": r[8],
            } for r in rows
        ]
        return data
    except Exception as e:
        logger.error(f"❌ [list] DB-fout: {e}")
        raise HTTPException(status_code=500, detail="❌ Kon marktdata niet ophalen.")


@router.post("/market_data/save")
async def save_market_data():
    logger.info("📡 [save] Ophalen van marktdata via CoinGecko...")
    crypto_data = {}
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            for symbol, coingecko_id in ASSETS.items():
                url = COINGECKO_URL.format(id=coingecko_id)
                response = await client.get(url)
                response.raise_for_status()
                ohlc = response.json()

                if not ohlc:
                    logger.warning(f"⚠️ Geen OHLC-data voor {symbol}")
                    continue

                latest = ohlc[-1]
                open_, high, low, close = map(float, latest[1:5])
                change = ((close - open_) / open_) * 100

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

    if not crypto_data:
        logger.warning("⚠️ Geen geldige crypto-data ontvangen, niets opgeslagen.")
        return {"message": "⚠️ Geen marktdata opgeslagen (lege respons van CoinGecko)."}

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


@router.get("/market_data/interpreted")
async def fetch_interpreted_data():
    # Placeholder - implementeer logica voor interpretatie en score
    return {"message": "✅ Interpretatiedata ophalen werkt (nog geen scoreberekening geïmplementeerd)."}


@router.get("/market_data/test")
async def test_market_api():
    return {"success": True, "message": "🧪 Market API test werkt!"}


@router.delete("/market_data/{id}")
async def delete_market_asset(id: int):
    try:
        conn, cur = get_db_cursor()
        cur.execute("DELETE FROM market_data WHERE id = %s", (id,))
        conn.commit()
        conn.close()
        logger.info(f"🗑️ [delete] Markt asset met ID {id} verwijderd.")
        return {"message": f"🗑️ Asset {id} verwijderd."}
    except Exception as e:
        logger.error(f"❌ [delete] Fout bij verwijderen: {e}")
        raise HTTPException(status_code=500, detail="❌ Kon asset niet verwijderen.")
