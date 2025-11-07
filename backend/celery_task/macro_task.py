import os
import logging
import traceback
import requests
from datetime import datetime
from tenacity import retry, stop_after_attempt, wait_exponential
from celery import shared_task

# ✅ Eigen utils
from backend.utils.db import get_db_connection
from backend.utils.scoring_utils import generate_scores_db  # nieuwe DB-versie

# === ✅ Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

TIMEOUT = 10
HEADERS = {"Content-Type": "application/json"}


# =====================================================
# 🔁 Retry wrapper
# =====================================================
@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=5, max=20), reraise=True)
def safe_request(url, params=None):
    """Veilige HTTP-aanroep met retries."""
    try:
        resp = requests.get(url, headers=HEADERS, params=params, timeout=TIMEOUT)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.error(f"❌ API-fout bij {url}: {e}")
        raise


# =====================================================
# 📡 Indicatoren ophalen uit database
# =====================================================
def get_active_macro_indicators():
    """Haalt alle actieve macro-indicatoren op uit de database."""
    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding.")
        return []

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, name, source, link, symbol
                FROM indicators
                WHERE category = 'macro'
            """)
            rows = cur.fetchall()
            return [{"id": r[0], "name": r[1], "source": r[2], "link": r[3], "symbol": r[4]} for r in rows]
    except Exception as e:
        logger.error(f"❌ Fout bij ophalen macro-indicatoren: {e}")
        return []
    finally:
        conn.close()


# =====================================================
# 📅 Dubbele invoer voorkomen
# =====================================================
def already_fetched_today(indicator_name: str) -> bool:
    """Controleert of indicator vandaag al is opgeslagen in de database."""
    conn = get_db_connection()
    if not conn:
        return False
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 1 FROM macro_data
                WHERE name = %s AND DATE(timestamp) = CURRENT_DATE
            """, (indicator_name,))
            return cur.fetchone() is not None
    except Exception as e:
        logger.error(f"⚠️ Fout bij controleren bestaande macro-data: {e}")
        return False
    finally:
        conn.close()


# =====================================================
# 🧠 Generieke waarde-fetcher
# =====================================================
def fetch_value_from_source(indicator: dict):
    """
    Haalt de actuele waarde van een indicator op op basis van de DB-velden 'source' en 'link'.
    Elk bron-type kan een eigen extractielogica hebben.
    """
    name = indicator["name"]
    source = indicator.get("source", "").lower()
    link = indicator.get("link")

    if not link:
        logger.warning(f"⚠️ Geen API-link opgegeven voor {name}")
        return None

    try:
        data = safe_request(link)

        # 🧩 Per-bron extractielogica
        if "alternative.me" in source or "feargreed" in link:
            # CNN Fear & Greed Index
            return float(data["data"][0]["value"])

        elif "exchangerate" in source or "dxy" in name.lower():
            # DXY-achtige index (proxy via USD-exchange rate)
            return float(data["rates"].get("EUR", 0))

        elif "coingecko" in source:
            # BTC-dominantie via CoinGecko
            return float(data["data"]["market_cap_percentage"]["btc"])

        elif "fred" in source:
            # FRED API (macro-data uit VS)
            val = data.get("observations", [{}])[-1].get("value")
            return float(val) if val not in (None, ".") else None

        else:
            logger.warning(f"⚠️ Geen extractielogica gedefinieerd voor bron '{source}' ({name})")
            return None

    except Exception as e:
        logger.error(f"❌ Fout bij ophalen waarde voor {name}: {e}")
        return None


# =====================================================
# 💾 Opslaan in macro_data
# =====================================================
def store_macro_score_db(payload: dict):
    """Slaat de macro-score op in de database."""
    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding bij macro-opslag.")
        return
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO macro_data 
                    (name, value, score, trend, interpretation, action, symbol, source, link, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                payload.get("name"),
                payload.get("value"),
                payload.get("score"),
                payload.get("trend"),
                payload.get("interpretation"),
                payload.get("action"),
                payload.get("symbol", "BTC"),
                payload.get("source"),
                payload.get("link"),
                datetime.utcnow().replace(microsecond=0)
            ))
        conn.commit()
        logger.info(f"💾 Macro-score opgeslagen voor {payload.get('name')}")
    except Exception as e:
        logger.error(f"❌ Fout bij opslaan macro-score: {e}")
        logger.error(traceback.format_exc())
    finally:
        conn.close()


# =====================================================
# 🧠 Hoofdfunctie: dynamische macroverwerking
# =====================================================
def fetch_and_process_macro():
    """Haal alle macro-indicatoren op en sla scores op via DB-config."""
    logger.info("🚀 Start dynamische macro-verwerking...")

    indicators = get_active_macro_indicators()
    if not indicators:
        logger.warning("⚠️ Geen macro-indicatoren gevonden in DB.")
        return

    for item in indicators:
        name = item["name"]

        if already_fetched_today(name):
            logger.info(f"⏩ {name} is vandaag al opgehaald, overslaan.")
            continue

        logger.info(f"➡️ Verwerk macro-indicator: {name}")

        try:
            # 1️⃣ Waarde ophalen
            value = fetch_value_from_source(item)
            if value is None:
                logger.warning(f"⚠️ Geen waarde opgehaald voor {name}")
                continue

            # 2️⃣ Score berekenen via scoreregels in DB
            score_info = generate_scores_db(name, value)
            if not score_info:
                logger.warning(f"⚠️ Geen scoreregels gevonden voor {name}")
                continue

            # 3️⃣ Opslaan in DB
            payload = {
                "name": name,
                "value": value,
                "score": score_info.get("score", 10),
                "trend": score_info.get("trend", "–"),
                "interpretation": score_info.get("interpretation", "–"),
                "action": score_info.get("action", "–"),
                "symbol": item.get("symbol", "BTC"),
                "source": item.get("source"),
                "link": item.get("link"),
            }
            store_macro_score_db(payload)

        except Exception as e:
            logger.error(f"❌ Fout bij verwerken van {name}: {e}")
            logger.error(traceback.format_exc())

    logger.info("✅ Alle macro-indicatoren succesvol verwerkt.")


# =====================================================
# 🚀 Celery-taak
# =====================================================
@shared_task(name="backend.celery_task.macro_task.fetch_macro_data")
def fetch_macro_data():
    """Dagelijkse taak: haalt macrodata op uit DB en verwerkt via scoreregels."""
    fetch_and_process_macro()
