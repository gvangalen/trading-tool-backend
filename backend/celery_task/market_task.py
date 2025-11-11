import logging
import traceback
import requests
from datetime import datetime
from pathlib import Path
from tenacity import retry, stop_after_attempt, wait_exponential
from celery import shared_task

# Eigen imports
from backend.utils.db import get_db_connection
from backend.celery_task.btc_price_history_task import update_btc_history  # blijft beschikbaar

# === Config ===
TIMEOUT = 10
HEADERS = {"Content-Type": "application/json"}
CACHE_FILE = "/tmp/last_market_data_fetch.txt"
ASSETS = {"BTC": "bitcoin"}

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# =====================================================
# 🔁 Safe HTTP-get met retry
# =====================================================
@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=5, max=20), reraise=True)
def safe_get(url, params=None):
    resp = requests.get(url, headers=HEADERS, params=params, timeout=TIMEOUT)
    resp.raise_for_status()
    return resp.json()


# =====================================================
# 🌐 CoinGecko fetch helper
# =====================================================
def fetch_coingecko_market(symbol_id="bitcoin"):
    """Haalt marktdata op van CoinGecko (USD)."""
    try:
        url = f"https://api.coingecko.com/api/v3/coins/{symbol_id}"
        resp = requests.get(url, params={"localization": "false"}, timeout=TIMEOUT)
        resp.raise_for_status()

        data = resp.json()
        md = data.get("market_data", {}) or {}

        price = float(md.get("current_price", {}).get("usd", 0))
        volume = float(md.get("total_volume", {}).get("usd", 0))
        change_24h = float(md.get("price_change_percentage_24h", 0) or 0)

        logger.info(f"📊 Fetched market data: price={price}, volUSD={volume}, 24h={change_24h}")
        return {"price": price, "volume": volume, "change_24h": change_24h}

    except Exception:
        logger.error("❌ Fout bij ophalen CoinGecko market:")
        logger.error(traceback.format_exc())
        return None


# =====================================================
# 💾 Opslaan in market_data (correcte kolommen)
# =====================================================
def store_market_data_db(symbol, price, volume, change_24h):
    """Slaat live marktdata op met upsert per (symbol, date)."""
    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding bij market-opslag.")
        return

    try:
        ts = datetime.utcnow().replace(microsecond=0)
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO market_data (symbol, price, volume, change_24h, timestamp)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (symbol, date)
                DO UPDATE SET
                    price = EXCLUDED.price,
                    volume = EXCLUDED.volume,
                    change_24h = EXCLUDED.change_24h,
                    timestamp = EXCLUDED.timestamp;
            """, (symbol, price, volume, change_24h, ts))
        conn.commit()
        logger.info(f"✅ Upsert market_data: {symbol} prijs={price}, volumeUSD={volume}, 24h={change_24h}")
    except Exception:
        logger.error("❌ Fout bij opslaan market_data:")
        logger.error(traceback.format_exc())
    finally:
        conn.close()


# =====================================================
# 📊 Marktdata ophalen en opslaan
# =====================================================
def process_market_now():
    """Haalt huidige BTC-marketdata op en slaat op."""
    cg_id = ASSETS.get("BTC", "bitcoin")
    live = fetch_coingecko_market(cg_id)
    if not live:
        logger.warning("⚠️ Geen live marketdata ontvangen.")
        return
    store_market_data_db("BTC", live["price"], live["volume"], live["change_24h"])


# =====================================================
# 🚀 Celery-taken
# =====================================================

# 1️⃣ Live marketdata (elke ±15 min)
@shared_task(name="backend.celery_task.market_task.fetch_market_data")
def fetch_market_data_task():
    """Haalt live BTC-marktdata op en slaat deze op."""
    logger.info("📈 Start live marktdata taak...")
    try:
        process_market_now()
        Path(CACHE_FILE).touch()
        logger.info("✅ Live marktdata verwerkt.")
    except Exception:
        logger.error("❌ Fout in fetch_market_data_task()")
        logger.error(traceback.format_exc())


# 2️⃣ Dagelijkse snapshot (voor rapporten + 7d met USD-volume)
@shared_task(name="backend.celery_task.market_task.save_market_data_daily")
def save_market_data_daily():
    """
    Dagelijkse snapshot van de BTC-marktdata + OHLC naar market_data_7d.
    ⚠️ Volume wordt als USD opgeslagen (Binance quoteAssetVolume).
    """
    logger.info("🕛 Dagelijkse market snapshot...")
    try:
        # 🔹 CoinGecko snapshot (prijs/24h/volumeUSD)
        process_market_now()

        # 🔹 Binance 1d kline voor OHLC + USD-volume (quoteAssetVolume = index 7)
        logger.info("📊 Ophalen Binance OHLC candle (1d) met USD-volume voor market_data_7d...")
        url = "https://api.binance.com/api/v3/klines"
        params = {"symbol": "BTCUSDT", "interval": "1d", "limit": 1}
        candles = safe_get(url, params=params)

        if candles:
            k = candles[-1]
            open_p = float(k[1])
            high_p = float(k[2])
            low_p  = float(k[3])
            close_p = float(k[4])
            base_vol_btc = float(k[5])
            quote_vol_usd = float(k[7])  # ✅ dit is al USD/USDT-volume
            change = round(((close_p - open_p) / open_p) * 100, 2) if open_p else None

            conn = get_db_connection()
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO market_data_7d (symbol, date, open, high, low, close, change, volume, created_at)
                    VALUES ('BTC', CURRENT_DATE, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (symbol, date) DO UPDATE SET
                        open = EXCLUDED.open,
                        high = EXCLUDED.high,
                        low = EXCLUDED.low,
                        close = EXCLUDED.close,
                        change = EXCLUDED.change,
                        volume = EXCLUDED.volume,
                        created_at = NOW();
                """, (open_p, high_p, low_p, close_p, change, quote_vol_usd))
            conn.commit()
            conn.close()
            logger.info(
                f"✅ OHLC bijgewerkt voor {datetime.utcnow().date()} "
                f"| O:{open_p}, H:{high_p}, L:{low_p}, C:{close_p}, "
                f"Δ{change:+.2f}% | VolUSD:{quote_vol_usd:,.0f} (baseBTC:{base_vol_btc:,.2f})"
            )
        else:
            logger.warning("⚠️ Geen Binance candles ontvangen.")

        logger.info("✅ Dagelijkse snapshot opgeslagen (incl. 7d OHLC + USD-volume).")

    except Exception:
        logger.error("❌ Fout in save_market_data_daily()")
        logger.error(traceback.format_exc())


# 3️⃣ 7-daagse OHLC + Volume data
@shared_task(name="backend.celery_task.market_task.fetch_market_data_7d")
def fetch_market_data_7d():
    """
    Haalt 7-daagse BTC OHLC (via Binance) en volume (via CoinGecko) op
    en slaat gecombineerde data op in market_data_7d.
    Binance → open, high, low, close
    CoinGecko → totaal volume in USD
    """
    logger.info("📆 Start fetch_market_data_7d (hybride Binance + CoinGecko)...")

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding.")
        return

    try:
        # === 1️⃣ Binance OHLC ophalen ===
        binance_url = "https://api.binance.com/api/v3/klines"
        params = {"symbol": "BTCUSDT", "interval": "1d", "limit": 7}
        resp = requests.get(binance_url, params=params, timeout=10)
        resp.raise_for_status()
        binance_data = resp.json()

        if not binance_data:
            logger.warning("⚠️ Geen Binance OHLC-data ontvangen.")
            return

        # === 2️⃣ CoinGecko volume ophalen ===
        coingecko_url = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart"
        params = {"vs_currency": "usd", "days": "7"}
        resp_vol = requests.get(coingecko_url, params=params, timeout=10)
        resp_vol.raise_for_status()
        volume_json = resp_vol.json()
        volume_points = volume_json.get("total_volumes", [])

        # volume gemiddeld per dag
        volume_by_day = {}
        for ts, vol in volume_points:
            day = datetime.utcfromtimestamp(ts / 1000).date()
            volume_by_day.setdefault(day, []).append(vol)
        avg_volume = {d: sum(vs) / len(vs) for d, vs in volume_by_day.items()}

        # === 3️⃣ Gecombineerde opslag ===
        inserted = 0
        with conn.cursor() as cur:
            for candle in binance_data:
                ts = int(candle[0])
                open_p = float(candle[1])
                high_p = float(candle[2])
                low_p = float(candle[3])
                close_p = float(candle[4])
                change = round(((close_p - open_p) / open_p) * 100, 2)
                date = datetime.utcfromtimestamp(ts / 1000).date()

                volume_usd = float(avg_volume.get(date, 0.0))

                cur.execute("""
                    INSERT INTO market_data_7d (symbol, date, open, high, low, close, change, volume, created_at)
                    VALUES ('BTC', %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (symbol, date) DO UPDATE SET
                        open   = EXCLUDED.open,
                        high   = EXCLUDED.high,
                        low    = EXCLUDED.low,
                        close  = EXCLUDED.close,
                        change = EXCLUDED.change,
                        volume = EXCLUDED.volume,
                        created_at = NOW();
                """, (date, open_p, high_p, low_p, close_p, change, volume_usd))
                inserted += 1

                logger.info(
                    f"📅 {date} | O:{open_p:.0f} H:{high_p:.0f} L:{low_p:.0f} C:{close_p:.0f} "
                    f"Δ{change:+.2f}% | Vol(USD): {volume_usd/1e9:.2f}B"
                )

        conn.commit()
        logger.info(f"✅ market_data_7d succesvol bijgewerkt ({inserted} rijen, hybride model).")

    except Exception:
        logger.error("❌ Fout bij fetch_market_data_7d():")
        logger.error(traceback.format_exc())
    finally:
        conn.close()


# 4️⃣ Forward returns berekenen
@shared_task(name="backend.celery_task.market_task.calculate_and_save_forward_returns")
def calculate_and_save_forward_returns():
    """Bereken en sla forward returns op uit btc_price_history."""
    logger.info("📈 Forward returns berekenen...")
    try:
        conn = get_db_connection()
        if not conn:
            logger.error("❌ Geen DB-verbinding.")
            return

        with conn.cursor() as cur:
            cur.execute("SELECT date, price FROM btc_price_history ORDER BY date ASC")
            rows = cur.fetchall()

        if not rows:
            logger.warning("⚠️ Geen data in btc_price_history.")
            return

        data = [{"date": r[0], "price": float(r[1])} for r in rows]
        periods = [7, 30, 90]
        results = []

        for i, start in enumerate(data):
            for d in periods:
                j = i + d
                if j >= len(data):
                    continue
                end = data[j]
                change = ((end["price"] - start["price"]) / start["price"]) * 100
                avg_daily = change / d
                results.append((start["date"], end["date"], d, change, avg_daily))

        with conn.cursor() as cur:
            for s_date, e_date, days, change, avg_daily in results:
                cur.execute("""
                    INSERT INTO market_forward_returns
                        (symbol, period, start_date, end_date, change, avg_daily)
                    VALUES ('BTC', %s, %s, %s, %s, %s)
                    ON CONFLICT (symbol, period, start_date)
                    DO UPDATE SET
                        end_date = EXCLUDED.end_date,
                        change = EXCLUDED.change,
                        avg_daily = EXCLUDED.avg_daily;
                """, (f"{days}d", s_date, e_date, round(change, 2), round(avg_daily, 3)))
        conn.commit()
        logger.info(f"✅ Forward returns opgeslagen ({len(results)} rijen).")

    except Exception:
        logger.error("❌ Fout in calculate_and_save_forward_returns()")
        logger.error(traceback.format_exc())
    finally:
        if 'conn' in locals() and conn:
            conn.close()
