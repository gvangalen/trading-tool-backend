import logging
from fastapi import APIRouter, HTTPException, Query
from backend.utils.db import get_db_connection  # ✅ juist
from backend.config.settings import COINGECKO_URL, VOLUME_URL, ASSETS
from backend.config.settings import MARKET_CONFIG
import os
import json
import httpx
from datetime import datetime, timedelta

router = APIRouter()
logger = logging.getLogger(__name__)
logger.info("🚀 market_data_api.py geladen – alle marktroutes zijn actief.")  # ✅ LOG TOEGEVOEGD

@router.get("/market_data/list")
async def list_market_data(since_minutes: int = Query(default=1440, description="Alleen data van de laatste X minuten")):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        time_threshold = datetime.utcnow() - timedelta(minutes=since_minutes)
        cur.execute("""
            SELECT id, symbol, price, open, high, low, change_24h, volume, timestamp
            FROM market_data
            WHERE timestamp >= %s
            ORDER BY timestamp DESC
        """, (time_threshold,))
        rows = cur.fetchall()
        conn.close()

        return [
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

    conn = get_db_connection()
    cur = conn.cursor()
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

@router.post("/market_data/btc/7d/fill")
async def fill_btc_7day_data():
    conn = get_db_connection()
    if not conn:
        return {"error": "❌ Geen databaseverbinding"}

    today = datetime.utcnow().date()

    try:
        with conn.cursor() as cur:
            for i in range(7):
                date = today - timedelta(days=i)

                # Check of record al bestaat
                cur.execute("SELECT 1 FROM market_data_7d WHERE symbol = 'BTC' AND date = %s", (date,))
                if cur.fetchone():
                    continue  # ⏭️ Skip als al bestaat

                # 🔁 Dummy data – later vervangen door echte API
                open_price = 27000 + i * 100
                high_price = open_price + 500
                low_price = open_price - 500
                close_price = open_price + 200
                change = round((close_price - open_price) / open_price * 100, 2)

                cur.execute("""
                    INSERT INTO market_data_7d (symbol, date, open, high, low, close, change)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, ('BTC', date, open_price, high_price, low_price, close_price, change))

            conn.commit()

        return {"status": "✅ BTC 7-daagse data succesvol gevuld"}

    except Exception as e:
        return {"error": f"❌ Fout bij vullen data: {str(e)}"}

@router.get("/market_data/btc/latest")
def get_latest_btc_price():
    conn = get_db_connection()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, symbol, price, change_24h, volume, timestamp
            FROM market_data
            WHERE symbol = 'BTC'
            ORDER BY timestamp DESC
            LIMIT 1
        """)
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Geen BTC data gevonden")
        keys = ['id', 'symbol', 'price', 'change_24h', 'volume', 'timestamp']
        return dict(zip(keys, row))


@router.get("/market_data/interpreted")
async def fetch_interpreted_data():
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute("""
            SELECT symbol, price, change_24h, volume, timestamp
            FROM market_data
            WHERE symbol = 'BTC'
            ORDER BY timestamp DESC
            LIMIT 1
        """)
        row = cur.fetchone()
        conn.close()

        if not row:
            raise HTTPException(status_code=404, detail="Geen BTC data gevonden")

        price = row[1]
        change = row[2]
        volume = row[3]

        score = 0
        advies = []

        if change > 2:
            score += 2
            advies.append("Sterke prijsstijging – mogelijk momentum")
        elif change < -2:
            score -= 2
            advies.append("Prijsdaling – verhoogde voorzichtigheid")

        if volume and volume > 10_000_000_000:
            score += 1
            advies.append("Hoog volume – bevestigt beweging")

        interpretatie = "Neutraal"
        if score >= 2:
            interpretatie = "Bullish"
        elif score <= -2:
            interpretatie = "Bearish"

        return {
            "symbol": row[0],
            "timestamp": row[4].isoformat(),
            "price": price,
            "change_24h": change,
            "volume": volume,
            "score": score,
            "advies": advies,
            "interpretatie": interpretatie
        }
    except Exception as e:
        logger.error(f"❌ [interpreted] Fout bij interpretatie: {e}")
        raise HTTPException(status_code=500, detail="❌ Interpretatiefout.")


@router.get("/market_data/test")
async def test_market_api():
    return {"success": True, "message": "🧪 Market API test werkt!"}


@router.delete("/market_data/{id}")
async def delete_market_asset(id: int):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("DELETE FROM market_data WHERE id = %s", (id,))
        conn.commit()
        conn.close()
        logger.info(f"🗑️ [delete] Markt asset met ID {id} verwijderd.")
        return {"message": f"🗑️ Asset {id} verwijderd."}
    except Exception as e:
        logger.error(f"❌ [delete] Fout bij verwijderen: {e}")
        raise HTTPException(status_code=500, detail="❌ Kon asset niet verwijderen.")


@router.get("/market_data/7d")
async def get_market_data_7d():
    """
    Haalt de 7-daagse historische BTC-marktdata op uit market_data_7d.
    Alleen voor symbol 'BTC'.
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT id, symbol, date, open, high, low, close, change, created_at
            FROM market_data_7d
            WHERE symbol = 'BTC'
            ORDER BY date DESC
        """)
        rows = cur.fetchall()
        conn.close()

        data = [{
            "id": r[0],
            "symbol": r[1],
            "date": r[2].isoformat(),
            "open": float(r[3]) if r[3] else None,
            "high": float(r[4]) if r[4] else None,
            "low": float(r[5]) if r[5] else None,
            "close": float(r[6]) if r[6] else None,
            "change": float(r[7]) if r[7] else None,
            "created_at": r[8].isoformat() if r[8] else None
        } for r in rows]

        return data

    except Exception as e:
        logger.error(f"❌ [7d] Fout bij ophalen market_data_7d: {e}")
        raise HTTPException(status_code=500, detail="❌ Fout bij ophalen van 7-daagse data.")

@router.get("/market_data/forward")
async def get_market_forward_returns():
    """
    Haalt de forward returns op uit market_forward_returns – alleen voor BTC.
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # ✅ Alleen BTC records ophalen
        cur.execute("""
            SELECT id, symbol, period, start_date, end_date, change, avg_daily, created_at
            FROM market_forward_returns
            WHERE symbol = 'BTC'
            ORDER BY period, start_date DESC
        """)

        rows = cur.fetchall()
        conn.close()

        data = [{
            "id": r[0],
            "symbol": r[1],
            "period": r[2],
            "start": r[3].isoformat(),
            "end": r[4].isoformat(),
            "change": float(r[5]) if r[5] is not None else None,
            "avgDaily": float(r[6]) if r[6] is not None else None,
            "created_at": r[7].isoformat() if r[7] else None
        } for r in rows]

        return data

    except Exception as e:
        logger.error(f"❌ [forward] Fout bij ophalen market_forward_returns: {e}")
        raise HTTPException(status_code=500, detail="❌ Fout bij ophalen van forward returns.")

@router.post("/market_data/7d/save")
async def save_market_data_7d(data: list[dict]):
    if not data:
        raise HTTPException(status_code=400, detail="❌ Geen data ontvangen.")

    try:
        conn = get_db_connection()
        cur = conn.cursor()
        inserted = 0

        for row in data:
            cur.execute("""
                SELECT 1 FROM market_data_7d
                WHERE symbol = %s AND date = %s
            """, (row["symbol"], row["date"]))
            if cur.fetchone():
                continue

            cur.execute("""
                INSERT INTO market_data_7d (symbol, date, open, high, low, close, change, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
            """, (
                row["symbol"],
                row["date"],
                row["open"],
                row["high"],
                row["low"],
                row["close"],
                row["change"]
            ))
            inserted += 1

        conn.commit()
        conn.close()
        logger.info(f"✅ [7d/save] {inserted} rijen opgeslagen.")
        return {"message": f"✅ {inserted} rijen opgeslagen."}
    except Exception as e:
        logger.error(f"❌ [7d/save] Fout bij opslaan: {e}")
        raise HTTPException(status_code=500, detail="❌ Fout bij opslaan van 7-daagse data.")

@router.post("/market_data/forward/save")
async def save_forward_returns(data: list[dict]):
    if not data:
        raise HTTPException(status_code=400, detail="❌ Geen data ontvangen.")

    try:
        conn = get_db_connection()
        cur = conn.cursor()
        inserted = 0

        for row in data:
            cur.execute("""
                SELECT 1 FROM market_forward_returns
                WHERE symbol = %s AND period = %s AND start_date = %s AND end_date = %s
            """, (row["symbol"], row["period"], row["start_date"], row["end_date"]))
            if cur.fetchone():
                continue

            cur.execute("""
                INSERT INTO market_forward_returns (symbol, period, start_date, end_date, change, avg_daily, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, NOW())
            """, (
                row["symbol"],
                row["period"],
                row["start_date"],
                row["end_date"],
                row["change"],
                row["avg_daily"]
            ))
            inserted += 1

        conn.commit()
        conn.close()
        logger.info(f"✅ [forward/save] {inserted} rijen opgeslagen.")
        return {"message": f"✅ {inserted} rijen opgeslagen."}
    except Exception as e:
        logger.error(f"❌ [forward/save] Fout bij opslaan: {e}")
        raise HTTPException(status_code=500, detail="❌ Fout bij opslaan van forward returns.")


@router.get("/market_data/returns/{periode}")
async def get_market_return_by_period(periode: str):
    """
    Haal forward return data op voor een bepaalde periode: week, maand, kwartaal, jaar
    Uit market_forward_returns
    """
    valid_periods = ["week", "maand", "kwartaal", "jaar"]
    if periode.lower() not in valid_periods:
        raise HTTPException(status_code=400, detail="❌ Ongeldige periode.")

    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT id, symbol, period, start_date, end_date, change, avg_daily, created_at
            FROM market_forward_returns
            WHERE LOWER(period) = %s AND symbol = 'BTC'
            ORDER BY start_date DESC
            LIMIT 1
        """, (periode.lower(),))
        row = cur.fetchone()
        conn.close()

        if not row:
            raise HTTPException(status_code=404, detail="❌ Geen data gevonden.")

        return {
            "id": row[0],
            "symbol": row[1],
            "period": row[2],
            "start": row[3].isoformat(),
            "end": row[4].isoformat(),
            "change": float(row[5]) if row[5] else None,
            "avgDaily": float(row[6]) if row[6] else None,
            "created_at": row[7].isoformat() if row[7] else None
        }

    except Exception as e:
        logger.error(f"❌ [returns/{periode}] Fout bij ophalen returns: {e}")
        raise HTTPException(status_code=500, detail="❌ Fout bij ophalen van returns.")
