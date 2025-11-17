import logging
import traceback
from fastapi import APIRouter, HTTPException, Query
from datetime import datetime, timedelta
import httpx
from collections import defaultdict

from backend.utils.db import get_db_connection
from backend.utils.scoring_utils import get_scores_for_symbol

# =========================================================
# ⚙️ Router setup
# =========================================================
router = APIRouter()
logger = logging.getLogger(__name__)
logger.info("🚀 market_data_api.py geladen – alle market-data routes actief.")


# =========================================================
# 🔄 Dynamisch laden van API endpoints uit database
# =========================================================
def get_market_raw_endpoints():
    """Haalt ALLE actieve market_raw endpoints uit de database."""
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT name, link
                FROM indicators
                WHERE category = 'market_raw' AND active = TRUE
            """)
            result = {row[0]: row[1] for row in cur.fetchall()}

        conn.close()
        logger.info(f"✅ Market RAW endpoints geladen: {list(result.keys())}")
        return result

    except Exception as e:
        logger.error(f"❌ Fout bij ophalen market_raw endpoints: {e}")
        return {}
        
MARKET_RAW_ENDPOINTS = get_market_raw_endpoints()


# =========================================================
# 📌 POST /market_data — BTC prijs/volume/change opslaan
# =========================================================
@router.post("/market_data")
def save_market_data():
    """Haalt BTC prijs/volume/change op via market_raw endpoints en slaat op."""
    try:
        # Endpoints ophalen
        price_url = MARKET_RAW_ENDPOINTS.get(
            "btc_price",
            "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd"
        )

        change_url = MARKET_RAW_ENDPOINTS.get(
            "btc_change_24h",
            "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd&include_24hr_change=true"
        )

        volume_url = MARKET_RAW_ENDPOINTS.get(
            "btc_volume",
            "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart?vs_currency=usd&days=1"
        )

        # HTTP client hergebruiken
        with httpx.Client(timeout=10.0) as client:
            resp_change = client.get(change_url).json()

            # Change + Price
            if "bitcoin" in resp_change:
                price = resp_change["bitcoin"].get("usd")
                change_24h = resp_change["bitcoin"].get("usd_24h_change")
            else:
                md = resp_change["market_data"]
                price = md["current_price"]["usd"]
                change_24h = md["price_change_percentage_24h"]

            # Volume
            resp_volume = client.get(volume_url).json()
            if "total_volumes" in resp_volume:
                volume = resp_volume["total_volumes"][-1][1]
            else:
                volume = None

        if price is None or volume is None or change_24h is None:
            raise ValueError("Ontbrekende price/change/volume in API")

        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute("""
            INSERT INTO market_data (symbol, price, volume, change_24h, timestamp)
            VALUES ('BTC', %s, %s, %s, NOW())
        """, (price, volume, change_24h))

        conn.commit()
        conn.close()

        return {
            "status": "ok",
            "price": price,
            "change_24h": change_24h,
            "volume": volume
        }

    except Exception as e:
        logger.error(f"❌ Error in save_market_data: {e}")
        raise HTTPException(500, str(e))

# =========================================================
# 📅 GET /market_data/day — DAGTABEL 
# =========================================================
@router.get("/market_data/day")
async def get_latest_market_day_data():
    logger.info("📄 [market/day] Ophalen market-dagdata (met fallback)...")

    conn = get_db_connection()
    if not conn:
        raise HTTPException(500, "❌ Geen databaseverbinding.")

    try:
        with conn.cursor() as cur:

            # 1️⃣ Eerst vandaag proberen
            cur.execute("""
                SELECT name, value, trend, interpretation, action, score, timestamp
                FROM market_data_indicators
                WHERE DATE(timestamp) = CURRENT_DATE
                ORDER BY timestamp DESC;
            """)
            rows = cur.fetchall()

            # 2️⃣ FALLBACK naar meest recente dag
            if not rows:
                logger.warning("⚠️ Geen market-data voor vandaag — fallback gebruiken.")

                cur.execute("""
                    SELECT timestamp
                    FROM market_data_indicators
                    ORDER BY timestamp DESC
                    LIMIT 1;
                """)
                last = cur.fetchone()

                if not last:
                    return []

                fallback_date = last[0].date()

                cur.execute("""
                    SELECT name, value, trend, interpretation, action, score, timestamp
                    FROM market_data_indicators
                    WHERE DATE(timestamp) = %s
                    ORDER BY timestamp DESC;
                """, (fallback_date,))
                rows = cur.fetchall()

        return [
            {
                "name": r[0],
                "value": r[1],
                "trend": r[2],
                "interpretation": r[3],
                "action": r[4],
                "score": r[5],
                "timestamp": r[6].isoformat() if r[6] else None
            }
            for r in rows
        ]

    except Exception as e:
        logger.error(f"❌ [market/day] Fout bij ophalen market dagdata: {e}")
        raise HTTPException(status_code=500, detail="❌ Ophalen market dagdata mislukt.")

    finally:
        conn.close()



# =========================================================
# 📌 GET /market/indicator_names — lijst beschikbare indicators
# =========================================================
@router.get("/market/indicator_names")
def get_market_indicator_names():
    """
    Geeft alle MARKET-indicators terug die:
    - in de tabel 'indicators' staan (met API-link/config)
    - én een entry hebben in 'market_indicator_rules' (scoreregels)
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT i.name, i.display_name
            FROM indicators i
            JOIN market_indicator_rules r
                ON r.indicator = i.name
            WHERE i.category = 'market'
            GROUP BY i.name, i.display_name
            ORDER BY i.display_name ASC;
        """)
        rows = cur.fetchall()
        conn.close()

        return [
            {
                "name": r[0],          # bv. 'btc_change_24h'
                "display_name": r[1],  # bv. 'BTC Change 24h'
            }
            for r in rows
        ]

    except Exception as e:
        logger.error(f"❌ [indicator_names] {e}")
        raise HTTPException(status_code=500, detail=str(e))



# =========================================================
# 📌 GET /market/indicator_rules/{name}
# =========================================================
@router.get("/market/indicator_rules/{name}")
def get_market_indicator_rules(name: str):
    """
    Haalt alle scoreregels op voor één market-indicator
    vanuit 'market_indicator_rules'.
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT range_min, range_max, score, trend, interpretation, action
            FROM market_indicator_rules
            WHERE indicator = %s
            ORDER BY range_min ASC;
        """, (name,))
        rows = cur.fetchall()
        conn.close()

        return [
            {
                "range_min": r[0],
                "range_max": r[1],
                "score": r[2],
                "trend": r[3],
                "interpretation": r[4],
                "action": r[5],
            }
            for r in rows
        ]

    except Exception as e:
        logger.error(f"❌ [indicator_rules] {e}")
        raise HTTPException(status_code=500, detail=str(e))

# =========================================================
# GET /market_data/list — ruwe BTC data
# =========================================================
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
            "id": r[0], "symbol": r[1], "price": r[2], "open": r[3],
            "high": r[4], "low": r[5], "change_24h": r[6],
            "volume": r[7], "timestamp": r[8]
        } for r in rows]
    except Exception as e:
        logger.error(f"❌ [list] DB-fout: {e}")
        raise HTTPException(500, "❌ Kon marktdata niet ophalen.")



# =========================================================
# GET /market_data/btc/7d/fill — BTC 7d ophalen & opslaan
# =========================================================
@router.post("/market_data/btc/7d/fill")
async def fill_btc_7day_data():
    logger.info("📥 Handmatig ophalen BTC 7d market data gestart")
    conn = get_db_connection()
    if not conn:
        return {"error": "❌ Geen databaseverbinding"}

    try:
        coingecko_id = "bitcoin"
        url_ohlc = f"https://api.coingecko.com/api/v3/coins/{coingecko_id}/ohlc?vs_currency=usd&days=7"
        url_volume = MARKET_ENDPOINTS.get(
            "btc_volume", f"https://api.coingecko.com/api/v3/coins/{coingecko_id}/market_chart?vs_currency=usd&days=7"
        )

        with httpx.Client(timeout=10.0) as client:
            ohlc_data = client.get(url_ohlc).json()
            volume_data = client.get(url_volume).json().get("total_volumes", [])

        volume_by_date = {
            datetime.utcfromtimestamp(ts / 1000).date(): vol
            for ts, vol in volume_data
        }

        inserted = 0
        with conn.cursor() as cur:
            for entry in ohlc_data:
                ts, open_p, high_p, low_p, close_p = entry
                date = datetime.utcfromtimestamp(ts / 1000).date()
                change = round((close_p - open_p) / open_p * 100, 2)
                volume = volume_by_date.get(date)

                cur.execute("SELECT 1 FROM market_data_7d WHERE symbol = %s AND date = %s", ('BTC', date))
                if cur.fetchone():
                    continue

                cur.execute("""
                    INSERT INTO market_data_7d (symbol, date, open, high, low, close, change, volume, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                """, ('BTC', date, open_p, high_p, low_p, close_p, change, volume))
                inserted += 1

        conn.commit()
        return {"status": f"✅ Gegevens opgeslagen voor {inserted} dagen."}

    except Exception as e:
        logger.error(f"❌ Fout bij ophalen en opslaan BTC market data: {e}")
        return {"error": f"❌ {str(e)}"}

    finally:
        conn.close()



# =========================================================
# GET /market_data/btc/latest
# =========================================================
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
            raise HTTPException(404, "Geen BTC data gevonden")

        keys = ['id', 'symbol', 'price', 'change_24h', 'volume', 'timestamp']
        return dict(zip(keys, row))



# =========================================================
# GET /market_data/interpreted — interpretatie + score
# =========================================================
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
            raise HTTPException(404, "Geen BTC data gevonden")

        symbol, price, change, volume, timestamp = row

        scores = get_scores_for_symbol(include_metadata=True)

        return {
            "symbol": symbol,
            "timestamp": timestamp.isoformat(),
            "price": float(price),
            "change_24h": float(change),
            "volume": float(volume),
            "score": scores.get("market_score", 0),
            "trend": "–",
            "interpretation": scores.get("market_interpretation", ""),
            "action": "Geen actie",
        }

    except Exception as e:
        logger.error(f"❌ [interpreted] Fout bij interpretatie: {e}")
        raise HTTPException(500, "❌ Interpretatiefout via scoring_util.")



# =========================================================
# GET /market_data/7d — laatste 7 dagen
# =========================================================
@router.get("/market_data/7d")
async def get_market_data_7d():
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, symbol, date, open, high, low, close, change, volume, created_at
                FROM market_data_7d
                WHERE symbol = 'BTC'
                ORDER BY date DESC
                LIMIT 7;
            """)
            rows = cur.fetchall()

        rows.reverse()  # van oud → nieuw
        return [{
            "id": r[0], "symbol": r[1], "date": r[2].isoformat(),
            "open": float(r[3]) if r[3] else None,
            "high": float(r[4]) if r[4] else None,
            "low": float(r[5]) if r[5] else None,
            "close": float(r[6]) if r[6] else None,
            "change": float(r[7]) if r[7] else None,
            "volume": float(r[8]) if r[8] else None,
            "created_at": r[9].isoformat() if r[9] else None
        } for r in rows]

    except Exception as e:
        logger.error(f"❌ [7d] Fout bij ophalen market_data_7d: {e}")
        raise HTTPException(500, "Fout bij ophalen 7-daagse data.")

    finally:
        conn.close()



# =========================================================
# GET /market_data/forward — alle forward returns
# =========================================================
@router.get("/market_data/forward")
async def get_market_forward_returns():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT id, symbol, period, start_date, end_date, change, avg_daily, created_at
            FROM market_forward_returns
            WHERE symbol = 'BTC'
            ORDER BY period, start_date DESC
        """)
        rows = cur.fetchall()
        conn.close()

        return [{
            "id": r[0], "symbol": r[1], "period": r[2],
            "start": r[3].isoformat(),
            "end": r[4].isoformat(),
            "change": float(r[5]) if r[5] else None,
            "avgDaily": float(r[6]) if r[6] else None,
            "created_at": r[7].isoformat() if r[7] else None
        } for r in rows]

    except Exception as e:
        logger.error(f"❌ [forward] Fout bij ophalen returns: {e}")
        raise HTTPException(500, "Fout bij ophalen forward returns.")



# =========================================================
# GET /market_data/forward/week
# =========================================================
@router.get("/market_data/forward/week")
def get_week_returns():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT start_date, change
            FROM market_forward_returns
            WHERE symbol = 'BTC' AND period = '7d'
            ORDER BY start_date ASC
        """)
        rows = cur.fetchall()
        conn.close()

        data = defaultdict(lambda: [None] * 53)
        for start_date, change in rows:
            data[start_date.year][int(start_date.strftime("%U"))] = float(change)

        return [{"year": y, "values": v} for y, v in sorted(data.items())]

    except Exception as e:
        logger.error(f"❌ Week returns error: {e}")
        raise HTTPException(500, "Fout bij ophalen week returns.")



# =========================================================
# GET /market_data/forward/maand
# =========================================================
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

        data = defaultdict(lambda: [None] * 12)
        for s, c in rows:
            data[s.year][s.month - 1] = float(c)

        return [{"year": y, "values": v} for y, v in sorted(data.items())]

    except Exception as e:
        logger.error(f"❌ Month returns error: {e}")
        raise HTTPException(500, "Fout bij ophalen maand returns.")



# =========================================================
# GET /market_data/forward/kwartaal
# =========================================================
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

        data = defaultdict(lambda: [None] * 4)
        for s, c in rows:
            data[s.year][(s.month - 1) // 3] = float(c)

        return [{"year": y, "values": v} for y, v in sorted(data.items())]

    except Exception as e:
        logger.error(f"❌ Quarter returns error: {e}")
        raise HTTPException(500, "Fout bij ophalen kwartaal returns.")



# =========================================================
# GET /market_data/forward/jaar
# =========================================================
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

        data = defaultdict(lambda: [None])
        for s, c in rows:
            data[s.year][0] = float(c)

        return [{"year": y, "values": v} for y, v in sorted(data.items())]

    except Exception as e:
        logger.error(f"❌ Year returns error: {e}")
        raise HTTPException(500, "Fout bij ophalen jaar returns.")



# =========================================================
# POST /market_data/7d/save
# =========================================================
@router.post("/market_data/7d/save")
async def save_market_data_7d(data: list[dict]):
    if not data:
        raise HTTPException(400, "❌ Geen data ontvangen.")

    try:
        conn = get_db_connection()
        cur = conn.cursor()

        for row in data:
            cur.execute("""
                INSERT INTO market_data_7d (symbol, date, open, high, low, close, change, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (symbol, date) DO NOTHING
            """, (
                row["symbol"], row["date"], row["open"], row["high"],
                row["low"], row["close"], row["change"]
            ))

        conn.commit()
        conn.close()
        return {"status": "✅ 7d data opgeslagen."}

    except Exception as e:
        logger.error(f"❌ [7d/save] {e}")
        raise HTTPException(500, "Fout bij opslaan 7d data.")



# =========================================================
# POST /market_data/forward/save
# =========================================================
@router.post("/market_data/forward/save")
async def save_forward_returns(data: list[dict]):
    if not data:
        raise HTTPException(400, "❌ Geen data ontvangen.")

    try:
        conn = get_db_connection()
        cur = conn.cursor()

        for row in data:
            cur.execute("""
                INSERT INTO market_forward_returns (symbol, period, start_date, end_date, change, avg_daily, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT DO NOTHING
            """, (
                row["symbol"], row["period"], row["start_date"],
                row["end_date"], row["change"], row["avg_daily"]
            ))

        conn.commit()
        conn.close()
        return {"status": "✅ Forward returns opgeslagen."}

    except Exception as e:
        logger.error(f"❌ [forward/save] {e}")
        raise HTTPException(500, "Fout bij opslaan forward returns.")
