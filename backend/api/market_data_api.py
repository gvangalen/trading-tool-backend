import logging
import traceback
from fastapi import APIRouter, HTTPException, Request, Query
from datetime import datetime, timedelta
import httpx
from backend.utils.db import get_db_connection
from backend.config.config_loader import load_market_config
from backend.utils.scoring_utils import get_scores_for_symbol

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
def save_market_data():
    """Haalt marktgegevens van CoinGecko op en slaat deze op in de database (met conflict-oplossing)."""
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {
        "ids": "bitcoin",
        "vs_currencies": "usd",
        "include_24hr_vol": "true",
        "include_24hr_change": "true",
    }

    try:
        response = httpx.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()["bitcoin"]
        logger.debug(f"📥 Ontvangen marktdata: {data}")

        price = data.get("usd")
        volume = data.get("usd_24h_vol")
        change_24h = data.get("usd_24h_change")

        if price is None or volume is None or change_24h is None:
            raise ValueError("Ontbrekende velden in CoinGecko response")

        now = datetime.utcnow()
        today = now.date()

        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO market_data (symbol, price, volume, change_24h, timestamp)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (symbol, date)
            DO UPDATE SET
                price = EXCLUDED.price,
                volume = EXCLUDED.volume,
                change_24h = EXCLUDED.change_24h,
                timestamp = EXCLUDED.timestamp
        """, ("BTC", price, volume, change_24h, now))

        conn.commit()
        cursor.close()
        conn.close()
        logger.info("✅ Marktdata succesvol opgeslagen of bijgewerkt.")
        return {"message": "Marktdata succesvol opgeslagen of bijgewerkt"}

    except httpx.HTTPStatusError as e:
        logger.error(f"❌ Fout bij ophalen marktdata: {e}")
        raise HTTPException(status_code=e.response.status_code, detail=str(e))
    except Exception as e:
        logger.error(f"❌ Onverwachte fout bij opslaan marktdata: {e}")
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
    """
    Geeft de laatst bekende BTC-marketdata inclusief automatische score,
    interpretatie en actie vanuit scoring_util.
    """
    try:
        from backend.utils.scoring_util import get_scores_for_symbol

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

        symbol = row[0]
        price = float(row[1])
        change = float(row[2])
        volume = float(row[3])
        timestamp = row[4]

        # ✅ Bereken automatisch market score + interpretatie
        scores = get_scores_for_symbol(symbol)
        market_details = scores.get("scores", {}).get("market", {})
        market_score = scores.get("market_score", 0)

        return {
            "symbol": symbol,
            "timestamp": timestamp.isoformat(),
            "price": price,
            "change_24h": change,
            "volume": volume,
            "score": market_score,
            "trend": market_details.get("trend", "Onbekend"),
            "interpretation": market_details.get("interpretation", "Geen interpretatie beschikbaar"),
            "action": market_details.get("action", "Geen actie"),
        }

    except Exception as e:
        logger.error(f"❌ [interpreted] Fout bij interpretatie via scoring_util: {e}")
        raise HTTPException(status_code=500, detail="❌ Interpretatiefout via scoring_util.")

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




@router.post("/market_data/history/save")
async def save_price_history(data: list[dict]):
    if not data:
        raise HTTPException(status_code=400, detail="❌ Geen data ontvangen.")

    try:
        conn = get_db_connection()
        cur = conn.cursor()
        inserted = 0

        for row in data:
            cur.execute("""
                INSERT INTO btc_price_history (date, price)
                VALUES (%s, %s)
                ON CONFLICT (date) DO NOTHING
            """, (row["date"], row["price"]))
            inserted += 1

        conn.commit()
        logger.info(f"✅ {inserted} entries opgeslagen in btc_price_history.")
        return {"inserted": inserted}
    except Exception as e:
        logger.error(f"❌ Fout bij opslaan geschiedenis: {e}")
        raise HTTPException(status_code=500, detail="❌ Opslaan mislukt.")

@router.get("/market_data/forward/week")
def get_week_returns():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        # 👇 hier '7d' gebruiken i.p.v. 'week'
        cur.execute("""
            SELECT start_date, change
            FROM market_forward_returns
            WHERE symbol = 'BTC' AND period = '7d'
            ORDER BY start_date ASC
        """)
        rows = cur.fetchall()
        conn.close()

        from collections import defaultdict
        data = defaultdict(lambda: [None] * 53)

        for start_date, change in rows:
            year = start_date.year
            week = int(start_date.strftime("%U"))  # Weeknummer 0-52
            if week < 53:
                data[year][week] = float(change)

        return [{"year": year, "values": values} for year, values in sorted(data.items())]

    except Exception as e:
        logger.error(f"❌ Fout bij ophalen week returns: {e}")
        raise HTTPException(status_code=500, detail="Fout bij ophalen week returns.")

@router.get("/market_data/forward/maand")
def get_month_returns():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT start_date, change
            FROM market_forward_returns
            WHERE symbol = 'BTC' AND period = '30d'
            ORDER BY start_date ASC
        """)
        rows = cur.fetchall()
        conn.close()

        from collections import defaultdict
        data = defaultdict(lambda: [None] * 12)

        for start_date, change in rows:
            year = start_date.year
            month = start_date.month  # 1–12
            data[year][month - 1] = float(change)

        return [{"year": year, "values": values} for year, values in sorted(data.items())]
    except Exception as e:
        logger.error(f"❌ Fout bij ophalen maand returns: {e}")
        raise HTTPException(status_code=500, detail="Fout bij ophalen maand returns.")

@router.get("/market_data/forward/kwartaal")
def get_quarter_returns():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT start_date, change
            FROM market_forward_returns
            WHERE symbol = 'BTC' AND period = '90d'
            ORDER BY start_date ASC
        """)
        rows = cur.fetchall()
        conn.close()

        from collections import defaultdict
        data = defaultdict(lambda: [None] * 4)

        for start_date, change in rows:
            year = start_date.year
            quarter = (start_date.month - 1) // 3  # 0–3
            data[year][quarter] = float(change)

        return [{"year": year, "values": values} for year, values in sorted(data.items())]
    except Exception as e:
        logger.error(f"❌ Fout bij ophalen kwartaal returns: {e}")
        raise HTTPException(status_code=500, detail="Fout bij ophalen kwartaal returns.")

@router.get("/market_data/forward/jaar")
def get_year_returns():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT start_date, change
            FROM market_forward_returns
            WHERE symbol = 'BTC' AND period = '365d'
            ORDER BY start_date ASC
        """)
        rows = cur.fetchall()
        conn.close()

        from collections import defaultdict
        data = defaultdict(lambda: [None])  # Eén waarde per jaar

        for start_date, change in rows:
            year = start_date.year
            data[year][0] = float(change)

        return [{"year": year, "values": values} for year, values in sorted(data.items())]
    except Exception as e:
        logger.error(f"❌ Fout bij ophalen jaar returns: {e}")
        raise HTTPException(status_code=500, detail="Fout bij ophalen jaar returns.")

@router.post("/market_data/indicator")
async def save_market_indicator(request: Request):
    """
    Ontvangt en slaat individuele market indicator (zoals price, volume, change_24h) op.
    """
    try:
        data = await request.json()
        symbol = data.get("symbol", "BTC")
        indicator = data.get("indicator")
        value = data.get("value")
        score = data.get("score")
        trend = data.get("trend", "–")
        advies = data.get("advies", "")
        uitleg = data.get("uitleg", "")
        source = data.get("source", "coingecko")
        timestamp = data.get("timestamp", datetime.utcnow().isoformat())

        if not indicator or value is None:
            raise HTTPException(status_code=400, detail="Indicator en waarde zijn verplicht.")

        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO market_data_indicators (symbol, indicator, value, score, trend, advies, uitleg, source, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                symbol, indicator, value, score, trend, advies, uitleg, source, timestamp
            ))
            conn.commit()

        logger.info(f"✅ Indicator '{indicator}' opgeslagen voor {symbol} ({value})")
        return {"status": "success", "indicator": indicator}

    except Exception as e:
        logger.error(f"❌ Fout bij opslaan market indicator: {e}")
        logger.debug(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Fout bij opslaan indicator.")
