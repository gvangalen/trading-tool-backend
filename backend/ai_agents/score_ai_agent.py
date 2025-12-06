import json
import logging
import traceback
from datetime import datetime, timedelta
from decimal import Decimal

from celery import shared_task

from backend.utils.db import get_db_connection
from backend.utils.openai_client import ask_gpt  # JSON-engine

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# ============================================================
# ✔ Categorie-architectuur conform jouw DB
# ============================================================
CATEGORIES = ["macro", "market", "technical", "setup", "strategy", "master"]


# ============================================================
# ⚙️ Decimal → float converter
# ============================================================
def convert_decimal(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, dict):
        return {k: convert_decimal(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [convert_decimal(i) for i in obj]
    return obj


def safe_json(obj, fallback):
    if isinstance(obj, dict):
        return obj
    if isinstance(obj, str):
        try:
            return json.loads(obj)
        except:
            return fallback
    return fallback


# ============================================================
# 📥 1. Insights ophalen → ai_category_insights
# ============================================================
def fetch_today_insights(conn, user_id=None):
    insights = {}
    today = datetime.utcnow().date()
    lookback = [today, today - timedelta(days=1), today - timedelta(days=2)]

    with conn.cursor() as cur:
        for cat in CATEGORIES:
            result = None

            for d in lookback:
                if user_id is not None:
                    cur.execute("""
                        SELECT category, avg_score, trend, bias, risk, summary, top_signals
                        FROM ai_category_insights
                        WHERE category = %s AND user_id = %s AND date = %s
                        LIMIT 1;
                    """, (cat, user_id, d))
                else:
                    # Fallback mode (zou niet meer gebruikt worden)
                    cur.execute("""
                        SELECT category, avg_score, trend, bias, risk, summary, top_signals
                        FROM ai_category_insights
                        WHERE category = %s AND date = %s
                        LIMIT 1;
                    """, (cat, d))

                row = cur.fetchone()
                if row:
                    result = {
                        "category": row[0],
                        "avg_score": float(row[1]) if row[1] else None,
                        "trend": row[2],
                        "bias": row[3],
                        "risk": row[4],
                        "summary": row[5] or "",
                        "top_signals": safe_json(row[6] or "[]", []),
                        "date": str(d),
                    }
                    break

            if result:
                insights[cat] = result

    return insights


# ============================================================
# 📊 2. Numerieke context uit daily_scores + ai_reflections
# ============================================================
def fetch_numeric_scores(conn, user_id=None):
    numeric = {"daily_scores": {}, "ai_reflections": {}}

    with conn.cursor() as cur:
        # daily_scores ophalen
        if user_id is not None:
            cur.execute("""
                SELECT macro_score, market_score, technical_score, setup_score
                FROM daily_scores
                WHERE report_date = CURRENT_DATE AND user_id = %s
                LIMIT 1;
            """, (user_id,))
        else:
            # Only for legacy mode (should be avoided)
            cur.execute("""
                SELECT macro_score, market_score, technical_score, setup_score
                FROM daily_scores
                WHERE report_date = CURRENT_DATE
                LIMIT 1;
            """)

        row = cur.fetchone()
        if row:
            numeric["daily_scores"] = {
                "macro": row[0],
                "market": row[1],
                "technical": row[2],
                "setup": row[3],
            }

        # ai_reflections aggregatie
        if user_id is not None:
            cur.execute("""
                SELECT category,
                       ROUND(AVG(COALESCE(ai_score, 0))::numeric, 1),
                       ROUND(AVG(COALESCE(compliance, 0))::numeric, 1)
                FROM ai_reflections
                WHERE date = CURRENT_DATE AND user_id = %s
                GROUP BY category;
            """, (user_id,))
        else:
            cur.execute("""
                SELECT category,
                       ROUND(AVG(COALESCE(ai_score, 0))::numeric, 1),
                       ROUND(AVG(COALESCE(compliance, 0))::numeric, 1)
                FROM ai_reflections
                WHERE date = CURRENT_DATE
                GROUP BY category;
            """)

        for cat, ai_score, comp in cur.fetchall() or []:
            numeric["ai_reflections"][cat] = {
                "avg_ai_score": float(ai_score),
                "avg_compliance": float(comp),
            }

    return convert_decimal(numeric)


# ============================================================
# 🧠 3. Master prompt bouwen
# ============================================================
def build_prompt(insights, numeric):

    def block(cat):
        i = insights.get(cat)
        if not i:
            return f"[{cat}] — GEEN DATA"
        sigs = ", ".join(i["top_signals"]) if i["top_signals"] else "-"
        return (
            f"[{cat}] score={i['avg_score']} | trend={i['trend']} | bias={i['bias']} | risk={i['risk']}\n"
            f"summary: {i['summary']}\n"
            f"signals: {sigs}"
        )

    text = "\n".join(block(cat) for cat in CATEGORIES)
    numeric_json = json.dumps(numeric, indent=2, ensure_ascii=False)

    return f"""
Je bent de MASTER Orchestrator AI voor trading.

Antwoord ALLEEN met geldige JSON:

{{
  "master_trend": "",
  "master_bias": "",
  "master_risk": "",
  "master_score": 0,
  "alignment_score": 0,
  "weights": {{
    "macro": 0.25,
    "market": 0.25,
    "technical": 0.25,
    "setup": 0.15,
    "strategy": 0.10
  }},
  "data_warnings": [],
  "summary": "",
  "outlook": "",
  "domains": {{
    "macro": {{}},
    "market": {{}},
    "technical": {{}},
    "setup": {{}},
    "strategy": {{}}
  }}
}}

=== INPUT DATA ===
{text}

=== NUMBERS ===
{numeric_json}
"""


# ============================================================
# 💾 4. Opslaan → ai_category_insights (categorie: 'master')
# ============================================================
def store_master_result(conn, result, user_id=None):
    if not isinstance(result, dict):
        logger.error("❌ Geen geldige JSON van AI.")
        return

    meta = {
        "weights": result.get("weights"),
        "alignment_score": result.get("alignment_score"),
        "data_warnings": result.get("data_warnings"),
        "domains": result.get("domains"),
        "outlook": result.get("outlook"),
    }

    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO ai_category_insights
                (category, user_id, avg_score, trend, bias, risk, summary, top_signals)
            VALUES ('master', %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (user_id, category, date)
            DO UPDATE SET
                avg_score = EXCLUDED.avg_score,
                trend = EXCLUDED.trend,
                bias = EXCLUDED.bias,
                risk = EXCLUDED.risk,
                summary = EXCLUDED.summary,
                top_signals = EXCLUDED.top_signals,
                created_at = NOW();
        """, (
            user_id,
            result.get("master_score"),
            result.get("master_trend"),
            result.get("master_bias"),
            result.get("master_risk"),
            result.get("summary"),
            json.dumps(meta, ensure_ascii=False),
        ))


# ============================================================
# 🕗 daily_scores vullen — nu per user
# ============================================================
def store_daily_scores(conn, insights, user_id=None):
    macro = insights.get("macro", {}).get("avg_score")
    market = insights.get("market", {}).get("avg_score")
    technical = insights.get("technical", {}).get("avg_score")

    macro_sum = insights.get("macro", {}).get("summary")
    market_sum = insights.get("market", {}).get("summary")
    tech_sum = insights.get("technical", {}).get("summary")

    if macro is None or market is None or technical is None:
        logger.error(f"❌ daily_scores NIET opgeslagen — ontbrekende AI categorieën voor user_id={user_id}.")
        return

    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO daily_scores
                (report_date, user_id, macro_score, market_score, technical_score,
                 macro_interpretation, market_interpretation, technical_interpretation,
                 setup_score, macro_top_contributors, market_top_contributors, technical_top_contributors)
            VALUES (
                CURRENT_DATE, %s, %s, %s, %s,
                %s, %s, %s,
                NULL, '[]', '[]', '[]'
            )
            ON CONFLICT (report_date, user_id)
            DO UPDATE SET
                macro_score = EXCLUDED.macro_score,
                market_score = EXCLUDED.market_score,
                technical_score = EXCLUDED.technical_score,
                macro_interpretation = EXCLUDED.macro_interpretation,
                market_interpretation = EXCLUDED.market_interpretation,
                technical_interpretation = EXCLUDED.technical_interpretation,
                updated_at = NOW();
        """, (
            user_id,
            macro, market, technical,
            macro_sum, market_sum, tech_sum,
        ))

    logger.info(f"💾 daily_scores opgeslagen voor user_id={user_id}.")


# ============================================================
# 🚀 Multi-user helper functie
# ============================================================
def generate_master_score_for_user(user_id):
    logger.info(f"🧩 MASTER Orchestrator voor user_id={user_id}")

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding.")
        return

    try:
        # 1️⃣ Insights ophalen
        insights = fetch_today_insights(conn, user_id=user_id)

        # 2️⃣ daily_scores opslaan
        store_daily_scores(conn, insights, user_id=user_id)

        # 3️⃣ Numerieke context
        numeric = fetch_numeric_scores(conn, user_id=user_id)

        # 4️⃣ Prompt
        prompt = build_prompt(insights, numeric)

        # 5️⃣ AI aanroepen
        result = ask_gpt(prompt)

        # 6️⃣ Opslaan master category
        store_master_result(conn, result, user_id=user_id)

        conn.commit()
        logger.info(f"✅ Master score opgeslagen voor user_id={user_id}")

    except Exception:
        logger.error(f"❌ Crash in master-score voor user_id={user_id}:")
        logger.error(traceback.format_exc())

    finally:
        conn.close()


# ============================================================
# 🚀 Celery task — draait nu voor ALLE users
# ============================================================
@shared_task(name="backend.ai_agents.score_ai_agent.generate_master_score")
def generate_master_score():
    logger.info("🧠 Start MASTER Score AI — MULTI USER MODE...")

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen databaseverbinding.")
        return

    try:
        # 🔎 Haal alle users op
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM users;")
            users = [row[0] for row in cur.fetchall()]

        logger.info(f"👥 {len(users)} gebruikers gevonden. Genereren per user...")

    finally:
        conn.close()

    # 🚀 Voor elke user master-score draaien
    for user_id in users:
        generate_master_score_for_user(user_id)
