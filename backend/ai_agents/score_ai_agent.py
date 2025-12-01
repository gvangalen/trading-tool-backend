import json
import logging
import traceback
from datetime import datetime, timedelta

from celery import shared_task

from backend.utils.db import get_db_connection
from backend.utils.openai_client import ask_gpt  # JSON engine

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# ============================================================
# ✔ Nieuwe categorie-architectuur
# ============================================================
CATEGORIES = ["macro", "market", "technical", "setup", "strategy", "master"]


# ============================================================
# 🔧 Kleine helpers
# ============================================================
def safe_json(obj, fallback):
    if isinstance(obj, dict):
        return obj
    if isinstance(obj, str):
        try:
            return json.loads(obj)
        except:
            logger.warning("⚠️ Kon JSON niet parsen, fallback gebruiken.")
            return fallback
    return fallback


# ============================================================
# 📥 1. Insights ophalen (laatste 3 dagen)
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
                        "avg_score": int(row[1]) if row[1] else None,
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
# 📊 2. Numerieke context ophalen
# ============================================================
def fetch_numeric_scores(conn):
    numeric = {
        "daily_scores": {},
        "ai_reflections": {},
    }

    with conn.cursor() as cur:
        # Daily scores ophalen + juiste kolomnaam 'date'
        cur.execute("""
            SELECT macro_score, market_score, technical_score, setup_score
            FROM daily_scores
            ORDER BY date DESC
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

        # AI reflections averages
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

    return numeric


# ============================================================
# 🧠 3. Prompt bouwen voor master orchestrator
# ============================================================
def build_prompt(insights, numeric):
    def format_block(cat):
        i = insights.get(cat)
        if not i:
            return f"[{cat}] — GEEN DATA"
        sigs = ", ".join(i["top_signals"]) if i["top_signals"] else "-"
        return (
            f"[{cat}] score≈{i['avg_score']} | trend={i['trend']} | bias={i['bias']} | risk={i['risk']}\n"
            f" summary: {i['summary']}\n"
            f" signals: {sigs}"
        )

    blocks = "\n".join(format_block(cat) for cat in CATEGORIES)

    return f"""
Je bent de MASTER Orchestrator AI voor trading. 
Combineer macro, market, technical, setup, strategy en eerdere master-score tot één totaalbeeld.

Antwoord UITSLUITEND in geldige JSON:

{{
  "master_trend": "",
  "master_bias": "",
  "master_risk": "",
  "master_score": 0,
  "weights": {{
    "macro": 0.25, "market": 0.25, "technical": 0.25, "setup": 0.15, "strategy": 0.10
  }},
  "alignment_score": 0,
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
{blocks}

Raw numeric context:
{json.dumps(numeric, indent=2)}
"""


# ============================================================
# 💾 4. Opslaan in ai_category_insights als categorie 'master'
# ============================================================
def store_master_result(conn, result):
    if not isinstance(result, dict):
        logger.error("❌ AI-output was geen dict.")
        return

    meta = {
        "outlook": result.get("outlook"),
        "weights": result.get("weights", {}),
        "alignment_score": result.get("alignment_score"),
        "data_warnings": result.get("data_warnings", []),
        "domains": result.get("domains", {}),
    }

    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO ai_category_insights
                (category, avg_score, trend, bias, risk, summary, top_signals)
            VALUES ('master', %s, %s, %s, %s, %s, %s)
            ON CONFLICT (category, date) DO UPDATE SET
                avg_score = EXCLUDED.avg_score,
                trend = EXCLUDED.trend,
                bias = EXCLUDED.bias,
                risk = EXCLUDED.risk,
                summary = EXCLUDED.summary,
                top_signals = EXCLUDED.top_signals,
                created_at = NOW();
        """, (
            int(result.get("master_score")) if result.get("master_score") else None,
            result.get("master_trend"),
            result.get("master_bias"),
            result.get("master_risk"),
            result.get("summary"),
            json.dumps(meta, ensure_ascii=False),
        ))


# ============================================================
# 🚀 5. Celery task — MASTER AI orchestrator
# ============================================================
@shared_task(name="backend.ai_agents.score_ai_agent.generate_master_score")
def generate_master_score():
    logger.info("🧠 Start Score-Orchestrator agent...")

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding.")
        return

    try:
        insights = fetch_today_insights(conn)
        numeric = fetch_numeric_scores(conn)
        prompt = build_prompt(insights, numeric)

        # JSON-engine
        result = ask_gpt(prompt)

        store_master_result(conn, result)
        conn.commit()

        logger.info("✅ Master score opgeslagen.")

    except Exception:
        logger.error("❌ Score-Orchestrator fout:")
        logger.error(traceback.format_exc())

    finally:
        conn.close()
