import os
import logging
import traceback
import requests
import asyncio
from datetime import datetime
from tenacity import retry, stop_after_attempt, wait_exponential, RetryError
from celery import shared_task

from backend.config.config_loader import load_macro_config
from backend.utils.macro_interpreter import process_macro_indicator
from backend.utils.db import get_db_connection
from backend.utils.scoring_utils import generate_scores, load_config  # ✅ Centrale scoringlogica

# === ✅ Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# === ✅ Basisconfig
API_BASE_URL = os.getenv("API_BASE_URL", "http://127.0.0.1:5002/api")
TIMEOUT = 10
HEADERS = {"Content-Type": "application/json"}


# === ✅ Retry wrapper voor POST
@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=3, max=20), reraise=True)
def safe_post(url, payload=None):
    try:
        response = requests.post(url, json=payload, headers=HEADERS, timeout=TIMEOUT)
        response.raise_for_status()
        logger.info(f"✅ API-call succesvol: {url}")
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ RequestError naar {url}: {e}")
        raise
    except Exception as e:
        logger.error(f"⚠️ Onverwachte fout bij {url}: {e}")
        raise


# === ✅ Check of macro-indicator vandaag al in database zit
def already_fetched_today(indicator_name: str) -> bool:
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id FROM macro_data
                WHERE name = %s AND DATE(timestamp) = CURRENT_DATE
            """, (indicator_name,))
            return cur.fetchone() is not None
    except Exception as e:
        logger.error(f"⚠️ Fout bij controleren op bestaande macro-data: {e}")
        return False


# === ✅ Directe opslag in macro_data tabel
def store_macro_score_db(payload: dict):
    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding bij macro-data opslaan.")
        return
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO macro_data 
                (name, value, score, trend, interpretation, action, symbol, source, category, correlation, link, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                payload.get("name"),
                payload.get("value"),
                payload.get("score"),
                payload.get("trend"),
                payload.get("interpretation"),
                payload.get("action"),
                payload.get("symbol", "BTC"),
                payload.get("source"),
                payload.get("category"),
                payload.get("correlation"),
                payload.get("link"),
                datetime.utcnow().replace(microsecond=0)
            ))
        conn.commit()
        logger.info(f"🗃️ Macro-score opgeslagen in DB voor {payload.get('name')}")
    except Exception as e:
        logger.error(f"❌ Fout bij DB-opslag macro-score: {e}")
        logger.error(traceback.format_exc())
    finally:
        conn.close()


# === ✅ Celery-task: macrodata ophalen en verwerken
@shared_task(name="backend.celery_task.macro_task.fetch_macro_data")
def fetch_macro_data():
    logger.info("🚀 Start ophalen + verwerken van macro-indicatoren...")

    try:
        config = load_macro_config()
        indicators = config.get("indicators", {})
        if not indicators:
            logger.warning("⚠️ Geen indicatoren gevonden in config.")
            return

        whitelist = ["fear_greed", "dxy"]  # ✅ Pas aan indien gewenst

        for name, indicator_config in indicators.items():
            if name not in whitelist:
                logger.info(f"⏩ Skip {name} (niet in whitelist)")
                continue

            if already_fetched_today(name):
                logger.info(f"⏩ {name} is vandaag al opgehaald. Skip.")
                continue

            logger.info(f"➡️ Verwerk: {name}...")

            try:
                # 🔄 Ophalen van actuele waarde
                try:
                    result = asyncio.run(process_macro_indicator(name, indicator_config))
                except Exception as async_error:
                    logger.error(f"❌ [ASYNC] Fout in asyncio.run() voor {name}: {async_error}")
                    logger.error(traceback.format_exc())
                    continue

                if not result or "value" not in result:
                    logger.warning(f"⚠️ Geen geldige data voor {name} → result={result}")
                    continue

                try:
                    float(result["value"])
                except Exception:
                    logger.warning(f"⚠️ Ongeldige waarde voor {name}: {result.get('value')}")
                    continue

                # ✅ Bereken score + uitleg via scoring_utils
                all_scores = generate_scores(
                    {name: result["value"]},
                    {name: indicator_config}
                )
                score_info = all_scores["scores"].get(name, {})

                # ✅ Payload op basis van dynamische score-info
                payload = {
                    "name": name,
                    "value": result["value"],
                    "score": score_info.get("score", 10),
                    "trend": score_info.get("trend", "–"),
                    "interpretation": score_info.get("interpretation", "–"),
                    "action": score_info.get("action", "–"),
                    "symbol": result.get("symbol", "BTC"),
                    "source": result.get("source", ""),
                    "category": indicator_config.get("category", ""),
                    "correlation": indicator_config.get("correlation", ""),
                    "link": result.get("link", ""),
                }

                logger.info(
                    f"📤 POST {name} | value={result['value']} | score={payload['score']} | trend={payload['trend']}"
                )

                # 🔁 API sync
                safe_post(f"{API_BASE_URL}/macro_data", payload=payload)

                # 💾 DB opslag
                store_macro_score_db(payload)

            except RetryError:
                logger.error(f"❌ Alle retries mislukt voor {name}")
            except Exception as e:
                logger.error(f"❌ Verwerking mislukt voor {name}: {e}")
                logger.error(traceback.format_exc())

        logger.info("✅ Alle macro-indicatoren verwerkt.")

    except Exception as e:
        logger.error(f"❌ Fout in fetch_macro_data(): {e}")
        logger.error(traceback.format_exc())
