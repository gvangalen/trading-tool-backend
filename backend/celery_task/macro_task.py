import logging
import requests
from tenacity import retry, stop_after_attempt, wait_exponential
from celery import shared_task

from backend.utils.db import get_db_connection
from backend.utils.scoring_utils import (
    generate_scores_db,
    normalize_indicator_name,
)

# ✅ AI-agent logica blijft gescheiden (zoals afgesproken)
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
# 🔁 Retry wrapper
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
# 📡 Actieve macro-indicatoren per user
# =====================================================
def get_active_macro_indicators(user_id: int):
    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding (macro indicators)")
        return []

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, name, source, link
                FROM indicators
                WHERE category = 'macro'
                  AND active = TRUE
                  AND user_id = %s
                """,
                (user_id,),
            )
            return [
                {"id": r[0], "name": r[1], "source": r[2], "link": r[3]}
                for r in cur.fetchall()
            ]
    except Exception:
        logger.error("❌ Fout bij ophalen macro-indicatoren", exc_info=True)
        return []
    finally:
        conn.close()


# =====================================================
# 📅 Check of vandaag al verwerkt
# =====================================================
def already_fetched_today(indicator_name: str, user_id: int) -> bool:
    conn = get_db_connection()
    if not conn:
        return False

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1
                FROM macro_data
                WHERE name = %s
                  AND user_id = %s
                  AND timestamp::date = CURRENT_DATE
                LIMIT 1
                """,
                (indicator_name, user_id),
            )
            return cur.fetchone() is not None
    except Exception:
        logger.error("⚠️ Fout bij check macro_data", exc_info=True)
        return False
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

    data = safe_request(link)

    try:
        # Fear & Greed / Alternative.me
        if "fear" in link or "alternative" in source:
            return float(data["data"][0]["value"])

        # CoinGecko dominance
        if "coingecko" in source:
            return float(data["data"]["market_cap_percentage"]["btc"])

        # Yahoo meta price
        if "yahoo" in source:
            return float(data["chart"]["result"][0]["meta"]["regularMarketPrice"])

        # FRED observations
        if "fred" in source:
            val = data["observations"][-1]["value"]
            return float(val) if val not in (None, ".") else None

        # DXY via chart meta
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
            cur.execute(
                """
                INSERT INTO macro_data
                    (user_id, name, value, trend, interpretation, action, score, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                """,
                (
                    user_id,
                    payload["name"],
                    payload["value"],
                    payload["trend"],
                    payload["interpretation"],
                    payload["action"],
                    payload["score"],
                ),
            )
        conn.commit()
        logger.info(f"💾 Macro opgeslagen: {payload['name']} (user_id={user_id})")
    except Exception:
        conn.rollback()
        logger.error("❌ Fout bij opslaan macro_data", exc_info=True)
    finally:
        conn.close()


# =====================================================
# 🧠 Hoofdverwerking ingestie (GEEN Celery)
# =====================================================
def fetch_and_process_macro(user_id: int):
    logger.info(f"🚀 Macro ingestie gestart (user_id={user_id})")

    indicators = get_active_macro_indicators(user_id)
    if not indicators:
        logger.warning(f"⚠️ Geen macro-indicatoren (user_id={user_id})")
        return

    for ind in indicators:
        raw_name = ind["name"]
        name = normalize_indicator_name(raw_name)

        if already_fetched_today(name, user_id):
            logger.info(f"⏩ {name} al verwerkt vandaag (user_id={user_id})")
            continue

        try:
            value = fetch_value_from_source(ind)
            if value is None:
                continue

            # Score per indicator (DB rules)
            score_data = generate_scores_db(
                "macro",
                data={name: value},
                user_id=user_id,
            )

            score = score_data.get("scores", {}).get(name)
            if not score:
                logger.warning(f"⚠️ Geen scoreregels voor {name}")
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

        except Exception:
            logger.error(f"❌ Fout bij macro {name}", exc_info=True)

    logger.info(f"✅ Macro ingestie afgerond (user_id={user_id})")


# =====================================================
# 🚀 Celery task 1: INGESTIE (PER USER)
# =====================================================
@shared_task(name="backend.celery_task.macro_task.fetch_macro_data")
def fetch_macro_data(user_id: int):
    """
    Wordt aangeroepen via dispatcher.dispatch_for_all_users
    -> vult macro_data + score/interpretation/action per indicator
    """
    try:
        fetch_and_process_macro(user_id=user_id)
    except Exception:
        logger.error("❌ Macro ingestie task crash", exc_info=True)


# =====================================================
# 🚀 Celery task 2: AI ANALYSE (PER USER) ✅ NIEUW
# =====================================================
@shared_task(name="backend.celery_task.macro_task.generate_macro_insight")
def generate_macro_insight(user_id: int):
    """
    Wordt aangeroepen via dispatcher.dispatch_for_all_users
    -> schrijft ai_category_insights (macro) + ai_reflections (macro)
    (logica zit in backend.ai_agents.macro_ai_agent.run_macro_agent)
    """
    try:
        logger.info(f"🧠 Macro AI insight task gestart (user_id={user_id})")
        run_macro_agent(user_id=user_id)
        logger.info(f"✅ Macro AI insight task klaar (user_id={user_id})")
    except Exception:
        logger.error("❌ Macro AI insight task crash", exc_info=True)
