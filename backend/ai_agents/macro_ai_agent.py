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
# 🌍 MACRO AI AGENT – FIXED VERSION (MATCHES MARKET)
# ======================================================

@shared_task(name="backend.ai_agents.macro_ai_agent.generate_macro_insight")
def generate_macro_insight():
    """
    Analyseert macro-indicatoren op basis van:
    - macro_data (name, value, trend, interpretation, action, score)
    - macro_indicator_rules (range_min/range_max logica)
    - samengestelde macro-score via generate_scores_db("macro")

    Output:
    - ai_category_insights (samenvatting)
    - ai_reflections (reflecties per indicator)
    """

    logger.info("🌍 Start Macro AI Agent...")

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding.")
        return

    try:
        # =========================================================
        # 1️⃣ Scoreregels uit database ophalen
        # =========================================================
        with conn.cursor() as cur:
            cur.execute("""
                SELECT indicator, range_min, range_max, score, trend, interpretation, action
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
        # 2️⃣ Laatste macro waardes ophalen — FIXED: name instead of indicator
        # =========================================================
        with conn.cursor() as cur:
            cur.execute("""
                SELECT name, value, trend, interpretation, action, score, timestamp
                FROM macro_data
                WHERE timestamp::date = CURRENT_DATE
                ORDER BY timestamp DESC;
            """)
            macro_rows = cur.fetchall()

        if not macro_rows:
            logger.warning("⚠️ Geen macro_data gevonden voor vandaag.")
            return

        macro_items = []
        for name, value, trend, interp, action, score, ts in macro_rows:
            macro_items.append({
                "indicator": name,       # FIXED
                "value": float(value) if value is not None else None,
                "trend": trend,
                "interpretation": interp,
                "action": action,
                "score": float(score) if score is not None else None,
                "timestamp": ts.isoformat()
            })

        # =========================================================
        # 3️⃣ Macro-score vanuit scoringsengine
        # =========================================================
        macro_scores = generate_scores_db("macro")
        macro_avg = macro_scores.get("total_score", 0)
        score_items = macro_scores.get("scores", {})

        # Top contributors (max 3)
        top_contrib = sorted(
            score_items.items(),
            key=lambda kv: kv[1].get("score", 0),
            reverse=True
        )[:3]

        top_contrib_pretty = [
            {
                "indicator": k,
                "value": v.get("value"),
                "score": v.get("score"),
                "trend": v.get("trend"),
                "interpretation": v.get("interpretation"),
            }
            for k, v in top_contrib
        ]

        # =========================================================
        # 4️⃣ AI context prompt
        # =========================================================
        data_payload = {
            "macro_items": macro_items,
            "macro_rules": rules_by_indicator,
            "macro_avg_score": macro_avg,
            "top_contributors": top_contrib_pretty
        }

        prompt_context = f"""
Je bent een macro-economische AI-analist gespecialiseerd in Bitcoin.

Hieronder vind je:
- actuele macro-indicatoren
- scoreregels
- samengestelde macro-score

DATA:
{json.dumps(data_payload, ensure_ascii=False, indent=2)}

Geef geldig JSON:
{{
  "trend": "",
  "bias": "",
  "risk": "",
  "summary": "",
  "top_signals": []
}}
"""

        ai_context = ask_gpt(
            prompt_context,
            system_role="Je bent een professionele macro-analist. Antwoord in geldige JSON."
        )

        if not isinstance(ai_context, dict):
            ai_context = {
                "trend": "",
                "bias": "",
                "risk": "",
                "summary": "",
                "top_signals": []
            }

        # =========================================================
        # 5️⃣ AI reflecties
        # =========================================================
        prompt_reflection = f"""
Je bent dezelfde macro-analist.

Maak een JSON-lijst met reflecties per indicator.

Elk item:
{{
  "indicator": "",
  "ai_score": 0,
  "compliance": 0,
  "comment": "",
  "recommendation": ""
}}

Indicatoren:
{macro_items}
"""

        ai_reflections = ask_gpt(
            prompt_reflection,
            system_role="Je bent een macro-analist. Antwoord in een JSON-lijst."
        )

        if not isinstance(ai_reflections, list):
            ai_reflections = []

        # =========================================================
        # 6️⃣ Opslaan ai_category_insights
        # =========================================================
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO ai_category_insights
                    (category, avg_score, trend, bias, risk, summary, top_signals)
                VALUES ('macro', %s, %s, %s, %s, %s, %s)
                ON CONFLICT (category, date)
                DO UPDATE SET
                    avg_score   = EXCLUDED.avg_score,
                    trend       = EXCLUDED.trend,
                    bias        = EXCLUDED.bias,
                    risk        = EXCLUDED.risk,
                    summary     = EXCLUDED.summary,
                    top_signals = EXCLUDED.top_signals,
                    created_at  = NOW();
            """, (
                macro_avg,
                ai_context.get("trend"),
                ai_context.get("bias"),
                ai_context.get("risk"),
                ai_context.get("summary"),
                json.dumps(ai_context.get("top_signals", []))
            ))

        # =========================================================
        # 7️⃣ Opslaan ai_reflections
        # =========================================================
        for r in ai_reflections:
            indicator = r.get("indicator")
            if not indicator:
                continue

            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO ai_reflections
                        (category, indicator, raw_score, ai_score, compliance, comment, recommendation)
                    VALUES ('macro', %s, NULL, %s, %s, %s, %s)
                    ON CONFLICT (category, indicator, date)
                    DO UPDATE SET
                        ai_score      = EXCLUDED.ai_score,
                        compliance    = EXCLUDED.compliance,
                        comment       = EXCLUDED.comment,
                        recommendation= EXCLUDED.recommendation,
                        timestamp     = NOW();
                """, (
                    indicator,
                    r.get("ai_score"),
                    r.get("compliance"),
                    r.get("comment"),
                    r.get("recommendation")
                ))

        conn.commit()
        logger.info("✅ Macro AI insights + reflecties opgeslagen.")

    except Exception:
        logger.error("❌ Macro Agent FOUT:")
        logger.error(traceback.format_exc())

    finally:
        conn.close()
