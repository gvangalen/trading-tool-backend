import logging
import requests
from tenacity import retry, stop_after_attempt, wait_exponential
from celery import shared_task

from backend.utils.db import get_db_connection
from backend.utils.scoring_utils import (
    generate_scores_db,
    normalize_indicator_name,
)

# ✅ AI-agent logica blijft gescheiden
from backend.ai_agents.macro_ai_agent import run_macro_agent

# =====================================================
# 🪵 Logging
# =====================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

TIMEOUT = 10
HEADERS = {"Content-Type": "application/json"}


# =====================================================
# 🔁 Retry wrapper (429 → gecontroleerd afvangen)
# =====================================================
@retry(
    stop=stop_after_attempt(2),
    wait=wait_exponential(min=5, max=15),
    reraise=True,
)
def safe_request(url, params=None):
    resp = requests.get(url, headers=HEADERS, params=params, timeout=TIMEOUT)

    if resp.status_code == 429:
        raise requests.exceptions.HTTPError("429_RATE_LIMIT")

    resp.raise_for_status()
    return resp.json()


# =====================================================
# 📡 Actieve macro-indicatoren
# =====================================================
def get_active_macro_indicators():
    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding (macro indicators)")
        return []

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT name, source, link
                FROM indicators
                WHERE category = 'macro'
                  AND active = TRUE
            """)
            rows = cur.fetchall()

        logger.info(f"📡 {len(rows)} actieve macro-indicatoren gevonden")
        return [
            {"name": r[0], "source": r[1], "link": r[2]}
            for r in rows
        ]
    except Exception:
        logger.error("❌ Fout bij ophalen macro-indicatoren", exc_info=True)
        return []
    finally:
        conn.close()


# =====================================================
# 🌐 Waarde ophalen uit bron
# =====================================================
def fetch_value_from_source(indicator: dict):
    raw_name = indicator["name"]
    source = (indicator.get("source") or "").lower()
    link = indicator.get("link")

    if not link:
        logger.warning(f"⚠️ Geen link voor {raw_name}")
        return None

    try:
        data = safe_request(link)
    except Exception as e:
        if "429" in str(e):
            logger.warning(f"⏩ Rate-limit (429) voor {raw_name} — overgeslagen")
            return None
        raise

    try:
        if "fear" in link or "alternative" in source:
            return float(data["data"][0]["value"])

        if "coingecko" in source:
            return float(data["data"]["market_cap_percentage"]["btc"])

        if "yahoo" in source:
            return float(data["chart"]["result"][0]["meta"]["regularMarketPrice"])

        if "fred" in source:
            val = data["observations"][-1]["value"]
            return float(val) if val not in (None, ".") else None

        if "dxy" in raw_name.lower():
            return float(data["chart"]["result"][0]["meta"]["regularMarketPrice"])

        logger.warning(f"⚠️ Geen parser voor {raw_name} ({source})")
        return None

    except Exception:
        logger.error(f"❌ Parse-fout bij {raw_name}", exc_info=True)
        return None


# =====================================================
# 💾 Opslaan macro_data
# =====================================================
def store_macro_data(payload: dict, user_id: int):
    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding (macro store)")
        return

    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO macro_data
                    (user_id, name, value, trend, interpretation, action, score, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
            """, (
                user_id,
                payload["name"],
                payload["value"],
                payload["trend"],
                payload["interpretation"],
                payload["action"],
                payload["score"],
            ))
        conn.commit()
        logger.info(f"💾 Macro opgeslagen: {payload['name']} (user_id={user_id})")
    except Exception:
        conn.rollback()
        logger.error("❌ Fout bij opslaan macro_data", exc_info=True)
    finally:
        conn.close()


# =====================================================
# 🧠 Macro ingestie
# =====================================================
def fetch_and_process_macro(user_id: int):
    logger.info(f"🚀 Macro ingestie gestart (user_id={user_id})")

    indicators = get_active_macro_indicators()
    if not indicators:
        logger.warning("⚠️ Geen actieve macro-indicatoren")
        return

    success = 0
    skipped = 0

    for ind in indicators:
        raw_name = ind["name"]
        name = normalize_indicator_name(raw_name)

        try:
            value = fetch_value_from_source(ind)
            if value is None:
                skipped += 1
                continue

            # ✅ FIX: GEEN data= ARGUMENT MEER
            score_data = generate_scores_db("macro", user_id=user_id)
            score = score_data.get("scores", {}).get(name)

            if not score:
                logger.warning(f"⚠️ Geen scoreregels voor {name}")
                skipped += 1
                continue

            payload = {
                "name": name,
                "value": value,
                "score": score["score"],
                "trend": score["trend"],
                "interpretation": score["interpretation"],
                "action": score["action"],
            }

            store_macro_data(payload, user_id)
            success += 1

        except Exception:
            skipped += 1
            logger.error(f"❌ Fout bij macro {name}", exc_info=True)

    logger.info(
        f"✅ Macro ingestie afgerond | user_id={user_id} | "
        f"success={success} | skipped={skipped}"
    )


# =====================================================
# 🚀 Celery tasks
# =====================================================
@shared_task(name="backend.celery_task.macro_task.fetch_macro_data")
def fetch_macro_data(user_id: int):
    try:
        fetch_and_process_macro(user_id=user_id)
    except Exception:
        logger.error("❌ Macro ingestie task crash", exc_info=True)


@shared_task(name="backend.celery_task.macro_task.run_macro_agent_daily")
def run_macro_agent_daily(user_id: int):
    try:
        logger.info(f"🧠 Macro AI Agent gestart (user_id={user_id})")
        run_macro_agent(user_id=user_id)
        logger.info(f"✅ Macro AI Agent voltooid (user_id={user_id})")
    except Exception:
        logger.error("❌ Macro AI Agent crash", exc_info=True)


@shared_task(name="backend.celery_task.macro_task.generate_macro_insight")
def generate_macro_insight(user_id: int):
    try:
        logger.info(f"🧠 Macro AI insight gestart (user_id={user_id})")
        run_macro_agent(user_id=user_id)
        logger.info(f"✅ Macro AI insight klaar (user_id={user_id})")
    except Exception:
        logger.error("❌ Macro AI insight task crash", exc_info=True)
