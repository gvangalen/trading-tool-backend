import logging
import traceback
from fastapi import APIRouter, HTTPException, Request, Query
from datetime import datetime, timedelta
import httpx
from backend.utils.db import get_db_connection
from backend.config.config_loader import load_market_config

router = APIRouter()
logger = logging.getLogger(__name__)
logger.info("🚀 market_data_api.py geladen – alle marktroutes zijn actief.")

MARKET_CONFIG = load_market_config()
COINGECKO_URL = MARKET_CONFIG["coingecko_url"]
VOLUME_URL = MARKET_CONFIG["volume_url"]
ASSETS = MARKET_CONFIG["assets"]

@router.get("/market_data/list")
async def list_market_data(since_minutes: int = Query(default=1440)):
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
        return [{
            "id": r[0], "symbol": r[1], "price": r[2], "open": r[3], "high": r[4], "low": r[5],
            "change_24h": r[6], "volume": r[7], "timestamp": r[8]
        } for r in rows]
    except Exception as e:
        logger.error(f"❌ [list] DB-fout: {e}")
        logger.debug(traceback.format_exc())
        raise HTTPException(status_code=500, detail="❌ Kon marktdata niet ophalen.")

@router.post("/market_data")
async def save_market_data(request: Request):
    try:
        data = await request.json()
        symbol = data.get("symbol", "BTC")
        coingecko_id = MARKET_CONFIG["assets"].get(symbol, "bitcoin")

        async with httpx.AsyncClient(timeout=10) as client:
            url = COINGECKO_URL.format(id=coingecko_id)
            response = await client.get(url)
            response.raise_for_status()
            prices = response.json()
            if not prices:
                raise ValueError("⚠️ Geen prijsdata ontvangen")
            price = float(prices[-1][4])

            vol_response = await client.get(VOLUME_URL.format(id=coingecko_id))
            vol_response.raise_for_status()
            vol_data = vol_response.json()
            change_24h = vol_data.get("market_data", {}).get("price_change_percentage_24h", 0.0)
            volume = vol_data.get("market_data", {}).get("total_volume", {}).get("usd", 0.0)

        timestamp = datetime.utcnow()
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO market_data (symbol, price, change_24h, volume, timestamp)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (symbol, date) DO UPDATE SET
                    price = EXCLUDED.price,
                    change_24h = EXCLUDED.change_24h,
                    volume = EXCLUDED.volume,
                    timestamp = EXCLUDED.timestamp;
            """, (symbol, price, change_24h, volume, timestamp))
            conn.commit()

        logger.info(f"✅ Marktdata opgeslagen voor {symbol}: prijs={price}, 24h={change_24h}, volume={volume}")
        return {"status": "success", "price": price, "change_24h": change_24h, "volume": volume}

    except Exception as e:
        logger.error(f"❌ Fout bij opslaan marktdata: {e}")
        logger.debug(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/market_data/btc/7d/fill")
async def fill_btc_7day_data():
    """
    Haalt de laatste 7 dagen BTC marktdata + volume op via CoinGecko.
    Slaat alles op in market_data_7d (indien nog niet aanwezig).
    """
    logger.info("📥 Handmatig ophalen BTC 7d market data gestart")
    conn = get_db_connection()
    if not conn:
        return {"error": "❌ Geen databaseverbinding"}

    try:
        coingecko_id = "bitcoin"
        url_ohlc = f"https://api.coingecko.com/api/v3/coins/{coingecko_id}/ohlc?vs_currency=usd&days=7"
        url_volume = f"https://api.coingecko.com/api/v3/coins/{coingecko_id}/market_chart?vs_currency=usd&days=7"

        with httpx.Client(timeout=10.0) as client:
            ohlc_resp = client.get(url_ohlc)
            ohlc_resp.raise_for_status()
            ohlc_data = ohlc_resp.json()

            volume_resp = client.get(url_volume)
            volume_resp.raise_for_status()
            volume_data = volume_resp.json().get("total_volumes", [])

        # 🔁 Zet volume-data om naar {date: volume}
        volume_by_date = {}
        for ts, volume in volume_data:
            date = datetime.utcfromtimestamp(ts / 1000).date()
            volume_by_date[date] = volume

        inserted = 0
        with conn.cursor() as cur:
            for entry in ohlc_data:
                ts, open_price, high_price, low_price, close_price = entry
                date = datetime.utcfromtimestamp(ts / 1000).date()
                change = round((close_price - open_price) / open_price * 100, 2)
                volume = volume_by_date.get(date, None)

                cur.execute("SELECT 1 FROM market_data_7d WHERE symbol = %s AND date = %s", ('BTC', date))
                if cur.fetchone():
                    continue  # ⏭️ Skip als al bestaat

                cur.execute("""
                    INSERT INTO market_data_7d (symbol, date, open, high, low, close, change, volume, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                """, ('BTC', date, open_price, high_price, low_price, close_price, change, volume))
                inserted += 1

        conn.commit()
        return {"status": f"✅ Gegevens opgeslagen voor {inserted} dagen."}

    except Exception as e:
        logger.error(f"❌ Fout bij ophalen en opslaan BTC market data: {e}")
        return {"error": f"❌ Fout: {str(e)}"}
    finally:
        conn.close()


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
    Haalt de laatste 7 dagen BTC-marktdata op uit market_data_7d.
    Fallback naar meest recente beschikbare datum als vandaag geen data bevat.
    Alleen voor symbol 'BTC'.
    """
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Geen databaseverbinding.")

    try:
        with conn.cursor() as cur:
            # ✅ Stap 1: check of er data van vandaag is
            cur.execute("""
                SELECT 1 FROM market_data_7d
                WHERE symbol = 'BTC' AND DATE(date) = CURRENT_DATE
                LIMIT 1;
            """)
            today_exists = cur.fetchone() is not None

            # 🔁 Stap 2: fallback naar laatste datum met data
            if not today_exists:
                cur.execute("""
                    SELECT MAX(date) FROM market_data_7d
                    WHERE symbol = 'BTC';
                """)
                fallback_date = cur.fetchone()[0]
                if not fallback_date:
                    logger.warning("⚠️ Geen data beschikbaar in market_data_7d.")
                    return []

                logger.info(f"🔁 Geen data van vandaag — fallback naar {fallback_date}")

            # 📦 Stap 3: laatste 7 dagen ophalen vanaf fallback/latest
            cur.execute("""
                SELECT id, symbol, date, open, high, low, close, change, volume, created_at
                FROM market_data_7d
                WHERE symbol = 'BTC'
                ORDER BY date DESC
                LIMIT 7;
            """)
            rows = cur.fetchall()

        # ✅ Omkeren zodat oudste eerst komt (chronologisch)
        rows.reverse()

        return [{
            "id": r[0],
            "symbol": r[1],
            "date": r[2].isoformat(),
            "open": float(r[3]) if r[3] else None,
            "high": float(r[4]) if r[4] else None,
            "low": float(r[5]) if r[5] else None,
            "close": float(r[6]) if r[6] else None,
            "change": float(r[7]) if r[7] else None,
            "volume": float(r[8]) if r[8] is not None else None,  # ✅ volume
            "created_at": r[9].isoformat() if r[9] else None
        } for r in rows]

    except Exception as e:
        logger.error(f"❌ [7d] Fout bij ophalen market_data_7d: {e}")
        raise HTTPException(status_code=500, detail="❌ Fout bij ophalen van 7-daagse data.")

    finally:
        conn.close()

# 📈 Automatisch forward returns genereren op basis van historische data
@router.post("/market_data/forward/generate")
def generate_forward_returns():
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        logger.info("📈 Start genereren van forward returns...")

        periods = {
            "week": 7,
            "month": 30,
            "quarter": 90,
        }

        # Haal alle historische prijzen op
        cur.execute("SELECT date, price FROM btc_price_history ORDER BY date ASC")
        rows = cur.fetchall()
        prices = {row[0]: float(row[1]) for row in rows}
        dates = list(prices.keys())

        inserted_count = 0

        for i, start_date in enumerate(dates):
            start_price = prices[start_date]
            for period, delta_days in periods.items():
                end_date = start_date + timedelta(days=delta_days)

                # Zoek dichtstbijzijnde end_date
                end_price = None
                for j in range(i + 1, len(dates)):
                    if dates[j] >= end_date:
                        end_price = prices[dates[j]]
                        actual_end_date = dates[j]
                        break

                if not end_price:
                    continue  # Niet genoeg data vooruit

                change = round((end_price - start_price) / start_price * 100, 2)
                avg_daily = round(change / delta_days, 2)

                # Check of deze al bestaat
                cur.execute("""
                    SELECT 1 FROM market_forward_returns
                    WHERE symbol = %s AND period = %s AND start_date = %s
                """, ("BTC", period, start_date))
                if cur.fetchone():
                    continue  # Skip dubbele invoer

                # Insert nieuwe return
                cur.execute("""
                    INSERT INTO market_forward_returns (symbol, period, start_date, end_date, change, avg_daily)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, ("BTC", period, start_date, actual_end_date, change, avg_daily))
                inserted_count += 1

        conn.commit()
        logger.info(f"✅ {inserted_count} forward returns toegevoegd.")
        return {"inserted": inserted_count}

    except Exception as e:
        logger.error(f"❌ Fout bij forward return generatie: {e}")
        raise HTTPException(status_code=500, detail="Fout bij forward return generatie")
    finally:
        cur.close()
        conn.close()

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
