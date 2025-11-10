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
def get_market_endpoints():
    """Haalt actieve market-indicator endpoints uit de database."""
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT name, data_url
                FROM indicators
                WHERE category = 'market' AND active = true
            """)
            result = {row[0]: row[1] for row in cur.fetchall()}
        conn.close()
        logger.info(f"✅ Market endpoints geladen: {list(result.keys())}")
        return result
    except Exception as e:
        logger.error(f"❌ Fout bij ophalen market endpoints: {e}")
        return {}


MARKET_ENDPOINTS = get_market_endpoints()
if not MARKET_ENDPOINTS:
    logger.warning("⚠️ Geen actieve market endpoints in DB – gebruik standaard CoinGecko URLs.")


# =========================================================
# ✅ /market_data — actuele data ophalen via DB-config
# =========================================================
@router.post("/market_data")
def save_market_data():
    """Haalt BTC-marktgegevens op via de URLs uit de database en slaat ze op."""
    try:
        # URLs uit DB of fallback
        price_url = MARKET_ENDPOINTS.get(
            "btc_price", "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd"
        )
        change_url = MARKET_ENDPOINTS.get(
            "btc_change_24h", "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd&include_24hr_change=true"
        )
        volume_url = MARKET_ENDPOINTS.get(
            "btc_volume", "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart?vs_currency=usd&days=7"
        )

        with httpx.Client(timeout=10.0) as client:
            # Haal prijs & 24h change op
            price_resp = client.get(change_url)
            price_resp.raise_for_status()
            price_data = price_resp.json()

            if "bitcoin" in price_data:
                price = price_data["bitcoin"].get("usd")
                change_24h = price_data["bitcoin"].get("usd_24h_change")
                volume = price_data["bitcoin"].get("usd_24h_vol")
            elif "market_data" in price_data:
                md = price_data["market_data"]
                price = md.get("current_price", {}).get("usd")
                change_24h = md.get("price_change_percentage_24h")
                volume = md.get("total_volume", {}).get("usd")
            else:
                raise ValueError("Onverwacht JSON-formaat voor prijs/24h change")

            # Als volume nog niet opgehaald is, probeer apart endpoint
            if not volume:
                vol_resp = client.get(volume_url)
                vol_resp.raise_for_status()
                vol_json = vol_resp.json()
                if "total_volumes" in vol_json and vol_json["total_volumes"]:
                    volume = float(vol_json["total_volumes"][-1][1])

        if price is None or volume is None or change_24h is None:
            raise ValueError("Ontbrekende velden in CoinGecko response")

        now = datetime.utcnow()
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO market_data (symbol, price, volume, change_24h, timestamp)
            VALUES (%s, %s, %s, %s, %s)
        """, ("BTC", price, volume, change_24h, now))
        conn.commit()
        conn.close()

        logger.info(f"✅ Marktdata opgeslagen: prijs={price}, volume={volume}, change={change_24h}")
        return {
            "message": "✅ Marktdata succesvol opgeslagen.",
            "price": price,
            "volume": volume,
            "change_24h": change_24h
        }

    except Exception as e:
        logger.error(f"❌ Fout bij opslaan marktdata: {e}\n{traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=str(e))


# =========================================================
# ✅ /market_data/list — recente marketdata ophalen
# =========================================================
@router.get("/market_data/list")
async def list_market_data(since_minutes: int = Query(default=1440)):
    """Haalt recente market_data records op uit de database."""
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
        raise HTTPException(status_code=500, detail="❌ Kon marktdata niet ophalen.")


# =========================================================
# ✅ /market_data/btc/7d/fill — 7-daagse BTC data ophalen en opslaan
# =========================================================
@router.post("/market_data/btc/7d/fill")
async def fill_btc_7day_data():
    """Haalt 7-daagse BTC-marktdata van CoinGecko en slaat op."""
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

        volume_by_date = {datetime.utcfromtimestamp(ts / 1000).date(): vol for ts, vol in volume_data}

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
        logger.info(f"✅ {inserted} dagen aan market_data_7d opgeslagen")
        return {"status": f"✅ Gegevens opgeslagen voor {inserted} dagen."}
    except Exception as e:
        logger.error(f"❌ Fout bij ophalen en opslaan BTC market data: {e}")
        return {"error": f"❌ {str(e)}"}
    finally:
        conn.close()


# =========================================================
# ✅ /market_data/btc/latest — laatste BTC prijs
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
            raise HTTPException(status_code=404, detail="Geen BTC data gevonden")
        keys = ['id', 'symbol', 'price', 'change_24h', 'volume', 'timestamp']
        return dict(zip(keys, row))


# =========================================================
# ✅ /market_data/interpreted — marketdata met score/advies
# =========================================================
@router.get("/market_data/interpreted")
async def fetch_interpreted_data():
    """Geeft de laatst bekende BTC-marketdata inclusief automatische score."""
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
        raise HTTPException(status_code=500, detail="❌ Interpretatiefout via scoring_util.")


# =========================================================
# ✅ /market_data/7d — laatste 7 dagen uit DB
# =========================================================
@router.get("/market_data/7d")
async def get_market_data_7d():
    """Haalt de laatste 7 dagen BTC-marktdata op uit market_data_7d."""
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
        rows.reverse()
        return [{
            "id": r[0], "symbol": r[1], "date": r[2].isoformat(),
            "open": float(r[3]) if r[3] else None, "high": float(r[4]) if r[4] else None,
            "low": float(r[5]) if r[5] else None, "close": float(r[6]) if r[6] else None,
            "change": float(r[7]) if r[7] else None, "volume": float(r[8]) if r[8] else None,
            "created_at": r[9].isoformat() if r[9] else None
        } for r in rows]
    except Exception as e:
        logger.error(f"❌ [7d] Fout bij ophalen market_data_7d: {e}")
        raise HTTPException(status_code=500, detail="Fout bij ophalen 7-daagse data.")
    finally:
        conn.close()


# =========================================================
# ✅ /market_data/forward — forward returns ophalen
# =========================================================
@router.get("/market_data/forward")
async def get_market_forward_returns():
    """Haalt de forward returns op uit market_forward_returns."""
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
            "start": r[3].isoformat(), "end": r[4].isoformat(),
            "change": float(r[5]) if r[5] is not None else None,
            "avgDaily": float(r[6]) if r[6] is not None else None,
            "created_at": r[7].isoformat() if r[7] else None
        } for r in rows]
    except Exception as e:
        logger.error(f"❌ [forward] Fout bij ophalen returns: {e}")
        raise HTTPException(status_code=500, detail="Fout bij ophalen forward returns.")


# =========================================================
# ✅ Forward returns per periode (week / maand / kwartaal / jaar)
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
        data = defaultdict(lambda: [None] * 12)
        for s, c in rows:
            data[s.year][s.month - 1] = float(c)
        return [{"year": y, "values": v} for y, v in sorted(data.items())]
    except Exception as e:
        logger.error(f"❌ Month returns error: {e}")
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
        data = defaultdict(lambda: [None] * 4)
        for s, c in rows:
            data[s.year][(s.month - 1) // 3] = float(c)
        return [{"year": y, "values": v} for y, v in sorted(data.items())]
    except Exception as e:
        logger.error(f"❌ Quarter returns error: {e}")
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
        data = defaultdict(lambda: [None])
        for s, c in rows:
            data[s.year][0] = float(c)
        return [{"year": y, "values": v} for y, v in sorted(data.items())]
    except Exception as e:
        logger.error(f"❌ Year returns error: {e}")
        raise HTTPException(status_code=500, detail="Fout bij ophalen jaar returns.")


# =========================================================
# ✅ Save routes
# =========================================================
@router.post("/market_data/7d/save")
async def save_market_data_7d(data: list[dict]):
    if not data:
        raise HTTPException(status_code=400, detail="❌ Geen data ontvangen.")
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        for row in data:
            cur.execute("""
                INSERT INTO market_data_7d (symbol, date, open, high, low, close, change, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (symbol, date) DO NOTHING
            """, (row["symbol"], row["date"], row["open"], row["high"], row["low"], row["close"], row["change"]))
        conn.commit()
        conn.close()
        return {"status": "✅ 7d data opgeslagen."}
    except Exception as e:
        logger.error(f"❌ [7d/save] {e}")
        raise HTTPException(status_code=500, detail="Fout bij opslaan 7d data.")


@router.post("/market_data/forward/save")
async def save_forward_returns(data: list[dict]):
    if not data:
        raise HTTPException(status_code=400, detail="❌ Geen data ontvangen.")
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        for row in data:
            cur.execute("""
                INSERT INTO market_forward_returns (symbol, period, start_date, end_date, change, avg_daily, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT DO NOTHING
            """, (
                row["symbol"],
                row["period"],
                row["start_date"],
                row["end_date"],
                row["change"],
                row["avg_daily"]
            ))
        conn.commit()
        conn.close()
        return {"status": "✅ Forward returns opgeslagen."}
    except Exception as e:
        logger.error(f"❌ [forward/save] Fout bij opslaan forward returns: {e}")
        raise HTTPException(status_code=500, detail="Fout bij opslaan forward returns.")

# =========================================================
# ✅ Delete indicator
# =========================================================
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
