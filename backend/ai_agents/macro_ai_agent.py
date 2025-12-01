import logging
import traceback
import json
from datetime import datetime
from celery import shared_task

from backend.utils.db import get_db_connection
from backend.utils.openai_client import ask_gpt
from backend.utils.scoring_utils import generate_scores_db

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# ======================================================
# 🧠 MACRO AI AGENT – v3 (perfect aligned with MARKET)
# ======================================================

@shared_task(name="backend.ai_agents.macro_ai_agent.generate_macro_insight")
def generate_macro_insight():

    logger.info("🌍 Start Macro AI Agent...")

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding.")
        return

    try:
        # =========================================================
        # 1️⃣ Scoreregels ophalen
        # =========================================================
        with conn.cursor() as cur:
            cur.execute("""
                SELECT indicator, range_min, range_max, score,
                       trend, interpretation, action
                FROM macro_indicator_rules
                ORDER BY indicator ASC, range_min ASC;
            """)
            rule_rows = cur.fetchall()

        rules_by_indicator = {}
        for r in rule_rows:
            indicator, rmin, rmax, score, trend, interp, action = r
            rules_by_indicator.setdefault(indicator, []).append({
                "range_min": float(rmin),
                "range_max": float(rmax),
                "score": int(score),
                "trend": trend,
                "interpretation": interp,
                "action": action,
            })

        logger.info(f"📘 Macro regels geladen ({len(rules_by_indicator)})")

        # =========================================================
        # 2️⃣ Macro-data uit DB ophalen
        # =========================================================
        with conn.cursor() as cur:
            cur.execute("""
                SELECT indicator, value, timestamp
                FROM macro_data
                WHERE date = CURRENT_DATE
                ORDER BY indicator ASC;
            """)
            rows = cur.fetchall()

        if not rows:
            logger.warning("⚠️ Geen macro_data voor vandaag")
            return

        macro_values = {
            indicator: {
                "value": float(value),
                "timestamp": ts.isoformat() if isinstance(ts, datetime) else str(ts)
            }
            for indicator, value, ts in rows
        }

        # =========================================================
        # 3️⃣ Macro-score berekenen via DB-logica
        # =========================================================
        macro_scores = generate_scores_db("macro")
        macro_avg = macro_scores.get("total_score", 0)
        score_items = macro_scores.get("scores", {})

        # Top contributors (max 3)
        top_contributors = sorted(
            score_items.items(),
            key=lambda kv: kv[1].get("score", 0),
            reverse=True
        )[:3]

        top_contributors_pretty = [
            {
                "indicator": name,
                "value": data.get("value"),
                "score": data.get("score"),
                "trend": data.get("trend"),
                "interpretation": data.get("interpretation"),
            }
            for name, data in top_contributors
        ]

        # =========================================================
        # 4️⃣ Bouw prompt payload
        # =========================================================
        data_payload = {
            "macro_values": macro_values,
            "macro_rules": rules_by_indicator,
            "macro_avg_score": macro_avg,
            "macro_top_contributors": top_contributors_pretty,
        }

        prompt_context = f"""
Je bent een professionele macro-econoom gespecialiseerd in Bitcoin.

Analyseer deze macrodata + scoreregels:

{json.dumps(data_payload, ensure_ascii=False, indent=2)}

Geef geldige JSON terug met:
{{
  "trend": "",
  "bias": "",
  "risk": "",
  "summary": "",
  "top_signals": []
}}
"""

        # =========================================================
        # 5️⃣ AI-context via ask_gpt (parsed JSON)
        # =========================================================
        ai_context = ask_gpt(
            prompt_context,
            system_role="Je bent een professionele macro-analist. Antwoord ALTIJD in geldige JSON."
        )

        if not isinstance(ai_context, dict):
            raw = ai_context.get("raw_text", "")[:200] if isinstance(ai_context, dict) else ""
            ai_context = {
                "trend": "",
                "bias": "",
                "risk": "",
                "summary": raw,
                "top_signals": []
            }

        # =========================================================
        # 6️⃣ Opslaan in ai_category_insights
        # =========================================================
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO ai_category_insights
                    (category, avg_score, trend, bias, risk, summary, top_signals)
                VALUES ('macro', %s, %s, %s, %s, %s, %s)
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
                macro_avg,
                ai_context.get("trend"),
                ai_context.get("bias"),
                ai_context.get("risk"),
                ai_context.get("summary"),
                json.dumps(ai_context.get("top_signals", []), ensure_ascii=False),
            ))

        conn.commit()
        logger.info("✅ Macro AI insight opgeslagen")

    except Exception:
        logger.error("❌ Macro Agent FOUT:")
        logger.error(traceback.format_exc())

    finally:
        conn.close()
