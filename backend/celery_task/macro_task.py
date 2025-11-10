import logging
import traceback
import requests
from datetime import datetime
from tenacity import retry, stop_after_attempt, wait_exponential
from celery import shared_task

# ✅ Eigen utils
from backend.utils.db import get_db_connection
from backend.utils.scoring_utils import generate_scores_db

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
                SELECT id, name, source, link
                FROM indicators
                WHERE category = 'macro' AND active = TRUE
            """)
            rows = cur.fetchall()
            return [{"id": r[0], "name": r[1], "source": r[2], "link": r[3]} for r in rows]
    except Exception as e:
        logger.error(f"❌ Fout bij ophalen macro-indicatoren: {e}")
        return []
    finally:
        conn.close()


# =====================================================
# 📅 Check of al verwerkt vandaag
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
# 🌐 Macrowaarde ophalen
# =====================================================
def fetch_value_from_source(indicator: dict):
    """Haalt de actuele waarde van een indicator op op basis van bron en link."""
    name = indicator["name"]
    source = indicator.get("source", "").lower()
    link = indicator.get("link")

    if not link:
        logger.warning(f"⚠️ Geen API-link voor {name}")
        return None

    try:
        data = safe_request(link)

        if "alternative" in source or "feargreed" in link:
            # Fear & Greed Index
            return float(data["data"][0]["value"])

        elif "coingecko" in source:
            # BTC Dominance
            return float(data["data"]["market_cap_percentage"]["btc"])

        elif "yahoo" in source:
            # S&P500 / VIX / Oil Price
            meta = data["chart"]["result"][0]["meta"]
            return float(meta["regularMarketPrice"])

        elif "fred" in source:
            # FRED API (inflatie / rente)
            val = data.get("observations", [{}])[-1].get("value")
            return float(val) if val not in (None, ".") else None

        elif "dxy" in name.lower():
            # DXY zelf (yahoo finance proxy)
            meta = data["chart"]["result"][0]["meta"]
            return float(meta["regularMarketPrice"])

        else:
            logger.warning(f"⚠️ Geen parser voor {name} ({source})")
            return None

    except Exception as e:
        logger.error(f"❌ Fout bij ophalen waarde voor {name}: {e}")
        return None


# =====================================================
# 💾 Opslaan in macro_data
# =====================================================
def store_macro_score_db(payload: dict):
    """Slaat de macro-score op in de database (altijd nieuwe rij per dag)."""
    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding bij macro-opslag.")
        return

    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO macro_data (name, value, trend, interpretation, action, score, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, NOW())
            """, (
                payload.get("name"),
                payload.get("value"),
                payload.get("trend"),
                payload.get("interpretation"),
                payload.get("action"),
                payload.get("score"),
            ))
        conn.commit()
        logger.info(f"💾 Nieuw record toegevoegd voor {payload.get('name')}")
    except Exception as e:
        logger.error(f"❌ Fout bij opslaan macro-score: {e}")
        logger.error(traceback.format_exc())
    finally:
        conn.close()


# =====================================================
# 🧠 Hoofdfunctie
# =====================================================
def fetch_and_process_macro():
    """Haal alle macro-indicatoren op en sla nieuwe rijen per dag op."""
    logger.info("🚀 Start macro-data verwerking...")

    indicators = get_active_macro_indicators()
    if not indicators:
        logger.warning("⚠️ Geen macro-indicatoren gevonden in DB.")
        return

    for item in indicators:
        name = item["name"]

        if already_fetched_today(name):
            logger.info(f"⏩ {name} is vandaag al verwerkt, overslaan.")
            continue

        logger.info(f"➡️ Verwerk macro-indicator: {name}")

        try:
            value = fetch_value_from_source(item)
            if value is None:
                logger.warning(f"⚠️ Geen waarde opgehaald voor {name}")
                continue

            score_info = generate_scores_db("macro", {name: value})
            result = score_info["scores"].get(name) if score_info and "scores" in score_info else None
            if not result:
                logger.warning(f"⚠️ Geen scoreregels voor {name}")
                continue

            payload = {
                "name": name,
                "value": value,
                "score": result.get("score", 50),
                "trend": result.get("trend", "–"),
                "interpretation": result.get("interpretation", "–"),
                "action": result.get("action", "–"),
            }
            store_macro_score_db(payload)

        except Exception as e:
            logger.error(f"❌ Fout bij {name}: {e}")
            logger.error(traceback.format_exc())

    logger.info("✅ Alle macro-indicatoren succesvol verwerkt.")


# =====================================================
# 🚀 Celery-taak
# =====================================================
@shared_task(name="backend.celery_task.macro_task.fetch_macro_data")
def fetch_macro_data():
    """Dagelijkse taak: haalt macrodata op en slaat nieuwe rijen op."""
    fetch_and_process_macro()
