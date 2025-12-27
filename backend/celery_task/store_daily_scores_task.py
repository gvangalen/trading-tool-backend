import logging
import json
from celery import shared_task

from backend.utils.db import get_db_connection
from backend.utils.scoring_utils import generate_scores_db
from backend.ai_agents.score_ai_agent import generate_master_score

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def _jsonb(value):
    """Zorgt dat we altijd geldige JSON naar jsonb casten."""
    return json.dumps(value or [], ensure_ascii=False)


# =========================================================
# 🔎 Setup-score ophalen UIT SETUP AGENT
# =========================================================
def fetch_setup_score_from_setup_agent(conn, user_id: int):
    """
    Setup-score is BRON:
    ai_category_insights WHERE category='setup'
    (gevuld door run_setup_agent)
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT avg_score
            FROM ai_category_insights
            WHERE user_id = %s
              AND category = 'setup'
              AND date = CURRENT_DATE
            LIMIT 1;
            """,
            (user_id,),
        )
        row = cur.fetchone()

    if not row or row[0] is None:
        logger.warning(f"⚠️ Geen setup-score gevonden (user_id={user_id})")
        return None

    return float(row[0])


# =========================================================
# 1️⃣ BUILD DAILY SCORES (RULE-BASED) — PER USER
# =========================================================
def build_daily_scores_for_user(user_id: int):
    """
    Bouwt daily_scores voor één user.

    BRONNEN:
    - macro      → generate_scores_db
    - technical  → generate_scores_db
    - market     → generate_scores_db
    - setup      → SETUP AGENT (ai_category_insights)

    ❌ GEEN eigen setup-berekening meer
    """

    logger.info(f"🧮 Daily scores bouwen (user_id={user_id})")

    macro = generate_scores_db("macro", user_id=user_id)
    technical = generate_scores_db("technical", user_id=user_id)
    market = generate_scores_db("market", user_id=user_id)

    macro_score = macro.get("total_score", 0)
    technical_score = technical.get("total_score", 0)
    market_score = market.get("total_score", 0)

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding")
        return

    try:
        # 🔥 Setup-score UIT setup agent
        setup_score = fetch_setup_score_from_setup_agent(conn, user_id)

        # fallback: geen setup agent gedraaid → score = 0 (of NULL)
        if setup_score is None:
            setup_score = 0

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO daily_scores (
                    report_date,
                    user_id,
                    macro_score,
                    technical_score,
                    market_score,
                    setup_score,

                    macro_interpretation,
                    technical_interpretation,
                    market_interpretation,

                    macro_top_contributors,
                    technical_top_contributors,
                    market_top_contributors
                )
                VALUES (
                    CURRENT_DATE,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s,
                    %s::jsonb, %s::jsonb, %s::jsonb
                )
                ON CONFLICT (user_id, report_date)
                DO UPDATE SET
                    macro_score = EXCLUDED.macro_score,
                    technical_score = EXCLUDED.technical_score,
                    market_score = EXCLUDED.market_score,
                    setup_score = EXCLUDED.setup_score,

                    macro_interpretation = EXCLUDED.macro_interpretation,
                    technical_interpretation = EXCLUDED.technical_interpretation,
                    market_interpretation = EXCLUDED.market_interpretation,

                    macro_top_contributors = EXCLUDED.macro_top_contributors,
                    technical_top_contributors = EXCLUDED.technical_top_contributors,
                    market_top_contributors = EXCLUDED.market_top_contributors;
                """,
                (
                    user_id,
                    macro_score,
                    technical_score,
                    market_score,
                    setup_score,

                    "Rule-based macro score",
                    "Rule-based technical score",
                    "Rule-based market score",

                    _jsonb(list(macro.get("scores", {}).keys())),
                    _jsonb(list(technical.get("scores", {}).keys())),
                    _jsonb(list(market.get("scores", {}).keys())),
                ),
            )

        conn.commit()
        logger.info(f"💾 daily_scores opgeslagen (user_id={user_id})")

    except Exception:
        conn.rollback()
        logger.error("❌ Fout bij opslaan daily_scores", exc_info=True)

    finally:
        conn.close()


# =========================================================
# 2️⃣ CELERY TASK: RULE-BASED DAILY SCORES (ALLE USERS)
# =========================================================
@shared_task(
    name="backend.celery_task.store_daily_scores_task.run_rule_based_daily_scores"
)
def run_rule_based_daily_scores():
    """
    Draait rule-based scoring voor alle users.

    ⚠️ BELANGRIJK:
    Deze task VERWACHT dat de setup agent
    AL GEDRAAID heeft voor vandaag.
    """

    logger.info("🚀 Start RULE-BASED daily_scores (alle users)")

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding")
        return

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM users;")
            users = [r[0] for r in cur.fetchall()]
    finally:
        conn.close()

    for user_id in users:
        build_daily_scores_for_user(user_id)

    logger.info("✅ RULE-BASED daily_scores klaar")


# =========================================================
# 3️⃣ CELERY TASK: MASTER SCORE AI (ALLE USERS)
# =========================================================
@shared_task(
    name="backend.celery_task.store_daily_scores_task.run_master_score_ai"
)
def run_master_score_ai():
    """
    Draait de MASTER orchestrator AI.

    Leest:
      - daily_scores
      - ai_category_insights (incl. setup)

    Schrijft:
      - ai_category_insights (category='master')
    """

    logger.info("🧠 Start MASTER Score AI (alle users)")

    try:
        generate_master_score()
        logger.info("✅ MASTER Score AI afgerond")
    except Exception:
        logger.error("❌ Fout tijdens MASTER Score AI", exc_info=True)
        raise
