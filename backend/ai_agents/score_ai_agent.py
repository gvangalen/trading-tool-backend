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
# ⚙️ Helper: Decimal → float converter (FIX)
# ============================================================
def convert_decimal(obj):
    """Recursively convert Decimal objects into float so json.dumps never crashes."""
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, dict):
        return {k: convert_decimal(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [convert_decimal(i) for i in obj]
    return obj


# ============================================================
# Helper safe_json
# ============================================================
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
def fetch_today_insights(conn):
    insights = {}
    today = datetime.utcnow().date()
    lookback = [today, today - timedelta(days=1), today - timedelta(days=2)]

    with conn.cursor() as cur:
        for cat in CATEGORIES:
            result = None

            for d in lookback:
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
# 📊 2. Numerieke context uit daily_scores
# ============================================================
def fetch_numeric_scores(conn):
    numeric = {
        "daily_scores": {},
        "ai_reflections": {},
    }

    with conn.cursor() as cur:
        # 🔥 daily_scores gebruikt report_date
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

        # 🔥 ai_reflections gebruikt date
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

    return convert_decimal(numeric)  # JSON-safe


# ============================================================
# 🧠 3. Master prompt bouwen
# ============================================================
def build_prompt(insights, numeric):

    def format_block(cat):
        i = insights.get(cat)
        if not i:
            return f"[{cat}] — GEEN DATA"
        sigs = ", ".join(i["top_signals"]) if i["top_signals"] else "-"
        return (
            f"[{cat}] score={i['avg_score']} | trend={i['trend']} | bias={i['bias']} | risk={i['risk']}\n"
            f"summary: {i['summary']}\n"
            f"signals: {sigs}"
        )

    data_text = "\n".join(format_block(cat) for cat in CATEGORIES)

    numeric_json = json.dumps(numeric, indent=2, ensure_ascii=False)

    return f"""
Je bent de MASTER Orchestrator AI voor trading. 
Combineer macro, market, technical, setup, strategy, master in één totaalbeeld.

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
{data_text}

=== NUMBERS ===
{numeric_json}
"""


# ============================================================
# 💾 4. Opslaan → ai_category_insights (categorie: 'master')
# ============================================================
def store_master_result(conn, result):
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
                (category, avg_score, trend, bias, risk, summary, top_signals)
            VALUES ('master', %s, %s, %s, %s, %s, %s)
            ON CONFLICT (category, date)
            DO UPDATE SET
                avg_score = EXCLUDED.avg_score,
                trend = EXCLUDED.trend,
                bias = EXCLUDED.bias,
                risk = EXCLUDED.risk,
                summary = EXCLUDED.summary,
                top_signals = EXCLUDED.top_signals,
                created_at = NOW();
        """, (
            result.get("master_score"),
            result.get("master_trend"),
            result.get("master_bias"),
            result.get("master_risk"),
            result.get("summary"),
            json.dumps(meta, ensure_ascii=False),
        ))


# ============================================================
# 🕗 FIX: daily_scores vullen voordat master draait
# ============================================================
def store_daily_scores(conn, insights):
    macro = insights.get("macro", {}).get("avg_score")
    market = insights.get("market", {}).get("avg_score")
    technical = insights.get("technical", {}).get("avg_score")

    macro_sum = insights.get("macro", {}).get("summary")
    market_sum = insights.get("market", {}).get("summary")
    tech_sum = insights.get("technical", {}).get("summary")

    if macro is None or market is None or technical is None:
        logger.error("❌ daily_scores NIET opgeslagen — ontbrekende AI categorieën.")
        return

    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO daily_scores
                (report_date, macro_score, market_score, technical_score,
                 macro_interpretation, market_interpretation, technical_interpretation,
                 setup_score, macro_top_contributors, market_top_contributors, technical_top_contributors)
            VALUES (
                CURRENT_DATE, %s, %s, %s,
                %s, %s, %s,
                NULL, '[]', '[]', '[]'
            )
            ON CONFLICT (report_date)
            DO UPDATE SET
                macro_score = EXCLUDED.macro_score,
                market_score = EXCLUDED.market_score,
                technical_score = EXCLUDED.technical_score,
                macro_interpretation = EXCLUDED.macro_interpretation,
                market_interpretation = EXCLUDED.market_interpretation,
                technical_interpretation = EXCLUDED.technical_interpretation,
                updated_at = NOW();
        """, (
            macro, market, technical,
            macro_sum, market_sum, tech_sum,
        ))

    logger.info("💾 daily_scores succesvol opgeslagen.")


# ============================================================
# 🚀 5. Celery task — orchestrator
# ============================================================
@shared_task(name="backend.ai_agents.score_ai_agent.generate_master_score")
def generate_master_score():
    logger.info("🧠 Start MASTER Score AI...")

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen databaseverbinding.")
        return

    try:
        # 1️⃣ Insights ophalen
        insights = fetch_today_insights(conn)

        # 2️⃣ daily_scores vullen
        store_daily_scores(conn, insights)

        # 3️⃣ Numerieke context
        numeric = fetch_numeric_scores(conn)

        # 4️⃣ Prompt bouwen
        prompt = build_prompt(insights, numeric)

        # 5️⃣ AI call
        result = ask_gpt(prompt)

        # 6️⃣ Opslaan master
        store_master_result(conn, result)
        conn.commit()

        logger.info("✅ Master score opgeslagen.")

    except Exception:
        logger.error("❌ Score orchestrator crash:")
        logger.error(traceback.format_exc())

    finally:
        conn.close()
