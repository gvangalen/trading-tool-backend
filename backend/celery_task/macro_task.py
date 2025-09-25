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
from backend.utils.db import get_db_connection  # ✅ Toegevoegd voor check

# ✅ Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ✅ Basisconfig
API_BASE_URL = os.getenv("API_BASE_URL", "http://127.0.0.1:5002/api")
TIMEOUT = 10
HEADERS = {"Content-Type": "application/json"}

# ✅ API-call met retries
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

# ✅ Check of indicator vandaag al is opgeslagen
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
        return False  # fallback: doorgaan met ophalen

# ✅ Hoofd Celery-task
@shared_task(name="backend.celery_task.macro_task.fetch_macro_data")
def fetch_macro_data():
    logger.info("🚀 Start ophalen + verwerken van macro-indicatoren...")

    try:
        config = load_macro_config()
        indicators = config.get("indicators", {})
        if not indicators:
            logger.warning("⚠️ Geen indicatoren gevonden in config.")
            return

        # ✅ Tijdelijk alleen deze indicatoren ophalen
        whitelist = ["fear_greed", "dxy"]

        for name, indicator_config in indicators.items():
            if name not in whitelist:
                logger.info(f"⏩ Skip {name} (niet in whitelist)")
                continue

            if already_fetched_today(name):
                logger.info(f"⏩ {name} is vandaag al opgehaald. Skip.")
                continue

            logger.info(f"➡️ Verwerk: {name}...")
            try:
                # 🔒 Beveiligde async-call
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
                    float(result["value"])  # validatie
                except Exception:
                    logger.warning(f"⚠️ Ongeldige waarde voor {name}: {result.get('value')}")
                    continue

                # ✅ Payload (BTC hardcoded als symbool)
                payload = {
                    "name": result["name"],
                    "value": result["value"],
                    "score": result.get("score", 0),
                    "trend": result.get("trend", ""),
                    "interpretation": result.get("interpretation", ""),
                    "action": result.get("action", ""),
                    "symbol": "BTC",  # of laat dit leeg of als "macro" in toekomst
                    "source": result.get("source", ""),
                    "category": result.get("category", ""),
                    "correlation": result.get("correlation", ""),
                    "link": result.get("link", ""),
                }

                logger.info(
                    f"📤 POST {name} | value={result['value']} | score={payload['score']} | trend={payload['trend']}"
                )
                safe_post(f"{API_BASE_URL}/macro_data", payload=payload)

            except RetryError:
                logger.error(f"❌ Alle retries mislukt voor {name}")
            except Exception as e:
                logger.error(f"❌ Verwerking mislukt voor {name}: {e}")
                logger.error(traceback.format_exc())

        logger.info("✅ Alle macro-indicatoren verwerkt.")

    except Exception as e:
        logger.error(f"❌ Fout in fetch_macro_data(): {e}")
        logger.error(traceback.format_exc())
