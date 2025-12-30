import logging
import requests
from tenacity import retry, stop_after_attempt, wait_exponential
from celery import shared_task

from backend.utils.db import get_db_connection
from backend.utils.scoring_utils import (
    generate_scores_db,
    normalize_indicator_name,
)

# AI-agent (gescheiden verantwoordelijkheden)
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
# 🔁 Retry wrapper (externe APIs)
# =====================================================
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(min=5, max=20),
    reraise=True,
)
def safe_request(url, params=None):
    resp = requests.get(url, headers=HEADERS, params=params, timeout=TIMEOUT)
    resp.raise_for_status()
    return resp.json()

# =====================================================
# 📡 Actieve macro-indicatoren (GLOBAAL)
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

        indicators = [
            {"name": r[0], "source": r[1], "link": r[2]}
            for r in rows
        ]

        logger.info(f"📡 {len(indicators)} actieve macro-indicatoren gevonden")
        return indicators

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
        logger.warning(f"⚠️ Geen link voor macro-indicator: {raw_name}")
        return None

    data = safe_request(link)

    try:
        if "fear" in link or "alternative" in source:
            return float(data["data"][0]["value"])

        if "coingecko" in source:
            return float(data["data"]["market_cap_percentage"]["btc"])

        if "yahoo" in source:
            return float(
                data["chart"]["result"][0]["meta"]["regularMarketPrice"]
            )

        if "fred" in source:
            val = data["observations"][-1]["value"]
            return float(val) if val not in (None, ".") else None

        if "dxy" in raw_name.lower():
            return float(
                data["chart"]["result"][0]["meta"]["regularMarketPrice"]
            )

        logger.warning(f"⚠️ Geen parser voor macro-indicator {raw_name}")
        return None

    except Exception:
        logger.error(f"❌ Parse-fout bij macro {raw_name}", exc_info=True)
        return None

# =====================================================
# 💾 Opslaan macro_data (PER USER)
# =====================================================
def store_macro_data(payload: dict, user_id: int):
    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding (macro store)")
        return False

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
        logger.info(
            f"💾 Macro opgeslagen: {payload['name']} = {payload['value']} "
            f"(score={payload['score']}) | user_id={user_id}"
        )
        return True

    except Exception:
        conn.rollback()
        logger.error("❌ Fout bij opslaan macro_data", exc_info=True)
        return False

    finally:
        conn.close()

# =====================================================
# 🧠 Macro ingestie (GEEN Celery decorator)
# =====================================================
def fetch_and_process_macro(user_id: int):
    logger.info(f"🚀 Macro ingestie gestart | user_id={user_id}")

    indicators = get_active_macro_indicators()
    if not indicators:
        logger.warning("⚠️ Geen actieve macro-indicatoren gevonden")
        return

    stored_count = 0
    attempted_count = 0

    for ind in indicators:
        raw_name = ind["name"]
        name = normalize_indicator_name(raw_name)
        attempted_count += 1

        try:
            value = fetch_value_from_source(ind)
            if value is None:
                logger.warning(f"⚠️ Geen waarde voor macro {name}")
                continue

            score_data = generate_scores_db(
                "macro",
                data={name: value},
                user_id=user_id,
            )

            score = score_data.get("scores", {}).get(name)
            if not score:
                logger.warning(
                    f"⚠️ Geen scoreregels voor macro {name} (value={value})"
                )
                continue

            payload = {
                "name": name,
                "value": value,
                "score": score["score"],
                "trend": score["trend"],
                "interpretation": score["interpretation"],
                "action": score["action"],
            }

            if store_macro_data(payload, user_id):
                stored_count += 1

        except Exception:
            logger.error(f"❌ Fout bij macro-indicator {name}", exc_info=True)

    # =====================================================
    # 📊 EINDLOG
    # =====================================================
    if stored_count == 0:
        logger.warning(
            f"⚠️ Macro ingestie leverde GEEN nieuwe records op "
            f"(attempted={attempted_count}, user_id={user_id})"
        )
    else:
        logger.info(
            f"✅ Macro ingestie afgerond | opgeslagen={stored_count} / "
            f"geprobeerd={attempted_count} | user_id={user_id}"
        )

# =====================================================
# 🚀 Celery task: INGESTIE
# =====================================================
@shared_task(name="backend.celery_task.macro_task.fetch_macro_data")
def fetch_macro_data(user_id: int):
    try:
        fetch_and_process_macro(user_id=user_id)
    except Exception:
        logger.error("❌ Macro ingestie task crash", exc_info=True)

# =====================================================
# 🚀 Celery task: AI AGENT
# =====================================================
@shared_task(name="backend.celery_task.macro_task.run_macro_agent_daily")
def run_macro_agent_daily(user_id: int):
    try:
        logger.info(f"🧠 Macro AI Agent gestart | user_id={user_id}")
        run_macro_agent(user_id=user_id)
        logger.info(f"✅ Macro AI Agent voltooid | user_id={user_id}")
    except Exception:
        logger.error("❌ Macro AI Agent crash", exc_info=True)
