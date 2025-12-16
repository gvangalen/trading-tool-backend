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
# 🌍 MACRO AI AGENT — USER-AWARE (STABLE / FIXED)
# ======================================================

@shared_task(name="backend.ai_agents.macro_ai_agent.generate_macro_insight")
def generate_macro_insight(user_id: int):
    """
    Analyseert macro-indicatoren PER USER.

    DB constraints (BELANGRIJK):
    - ai_category_insights UNIQUE (user_id, category, date)
    - ai_reflections UNIQUE (category, user_id, indicator, date)
    """

    if user_id is None:
        raise ValueError("❌ Macro AI Agent vereist een user_id")

    logger.info(f"🌍 Start Macro AI Agent — user_id={user_id}")

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding.")
        return

    try:
        # =========================================================
        # 1️⃣ Macro scoreregels (GLOBAAL)
        # =========================================================
        with conn.cursor() as cur:
            cur.execute("""
                SELECT indicator, range_min, range_max, score, trend, interpretation, action
                FROM macro_indicator_rules
                ORDER BY indicator ASC, range_min ASC;
            """)
            rule_rows = cur.fetchall()

        rules_by_indicator = {}
        for indicator, rmin, rmax, score, trend, interp, action in rule_rows:
            rules_by_indicator.setdefault(indicator, []).append({
                "range_min": float(rmin),
                "range_max": float(rmax),
                "score": int(score),
                "trend": trend,
                "interpretation": interp,
                "action": action,
            })

        logger.info(f"📘 Macro regels geladen ({len(rules_by_indicator)} indicatoren)")

        # =========================================================
        # 2️⃣ Macro data VANDAAG (USER-SPECIFIEK)
        # =========================================================
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    name,
                    value,
                    trend,
                    interpretation,
                    action,
                    score,
                    timestamp
                FROM macro_data
                WHERE user_id = %s
                  AND timestamp::date = CURRENT_DATE
                ORDER BY timestamp DESC;
            """, (user_id,))
            macro_rows = cur.fetchall()

        if not macro_rows:
            logger.warning(f"⚠️ Geen macro_data voor vandaag (user_id={user_id})")
            return

        macro_items = []
        for name, value, trend, interp, action, score, ts in macro_rows:
            macro_items.append({
                "indicator": name,
                "value": float(value) if value is not None else None,
                "trend": trend,
                "interpretation": interp,
                "action": action,
                "score": float(score) if score is not None else None,
                "timestamp": ts.isoformat() if ts else None,
            })

        # =========================================================
        # 3️⃣ Macro-score (USER-AWARE)
        # =========================================================
        macro_scores = generate_scores_db("macro", user_id=user_id)
        macro_avg = macro_scores.get("total_score", 0)
        score_items = macro_scores.get("scores", {})

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
        # 4️⃣ AI CONTEXT
        # =========================================================
        payload = {
            "user_id": user_id,
            "macro_items": macro_items,
            "macro_rules": rules_by_indicator,
            "macro_avg_score": macro_avg,
            "top_contributors": top_contrib_pretty,
        }

        prompt_context = f"""
Je bent een macro-economische analist gespecialiseerd in Bitcoin.

Analyseer de onderstaande macrodata en geef een samenvattend oordeel.

DATA:
{json.dumps(payload, ensure_ascii=False, indent=2)}

ANTWOORD ALLEEN GELDIGE JSON:
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
            system_role="Je bent een professionele macro-analist. Antwoord uitsluitend in geldige JSON."
        )

        if not isinstance(ai_context, dict):
            raise ValueError("❌ Macro AI response is geen geldige JSON")

        # =========================================================
        # 5️⃣ AI REFLECTIES PER INDICATOR
        # =========================================================
        prompt_reflections = f"""
Maak reflecties per macro-indicator.

DATA:
{json.dumps(macro_items, ensure_ascii=False, indent=2)}

ANTWOORD ALS JSON-LIJST:
[
  {{
    "indicator": "",
    "ai_score": 0,
    "compliance": 0,
    "comment": "",
    "recommendation": ""
  }}
]
"""

        ai_reflections = ask_gpt(
            prompt_reflections,
            system_role="Je bent een macro-analist. Antwoord uitsluitend in geldige JSON."
        )

        if not isinstance(ai_reflections, list):
            ai_reflections = []

        # =========================================================
        # 6️⃣ OPSLAAN ai_category_insights
        # UNIQUE (user_id, category, date)
        # =========================================================
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO ai_category_insights
                    (category, user_id, avg_score, trend, bias, risk, summary, top_signals)
                VALUES
                    ('macro', %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (user_id, category, date)
                DO UPDATE SET
                    avg_score   = EXCLUDED.avg_score,
                    trend       = EXCLUDED.trend,
                    bias        = EXCLUDED.bias,
                    risk        = EXCLUDED.risk,
                    summary     = EXCLUDED.summary,
                    top_signals = EXCLUDED.top_signals,
                    created_at  = NOW();
            """, (
                user_id,
                macro_avg,
                ai_context["trend"],
                ai_context["bias"],
                ai_context["risk"],
                ai_context["summary"],
                json.dumps(ai_context.get("top_signals", [])),
            ))

        # =========================================================
        # 7️⃣ OPSLAAN ai_reflections
        # UNIQUE (category, user_id, indicator, date)
        # =========================================================
        for r in ai_reflections:
            indicator = r.get("indicator")
            if not indicator:
                continue

            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO ai_reflections
                        (category, user_id, indicator, raw_score, ai_score, compliance, comment, recommendation)
                    VALUES
                        ('macro', %s, %s, NULL, %s, %s, %s, %s)
                    ON CONFLICT (category, user_id, indicator, date)
                    DO UPDATE SET
                        ai_score       = EXCLUDED.ai_score,
                        compliance     = EXCLUDED.compliance,
                        comment        = EXCLUDED.comment,
                        recommendation = EXCLUDED.recommendation,
                        timestamp      = NOW();
                """, (
                    user_id,
                    indicator,
                    r.get("ai_score"),
                    r.get("compliance"),
                    r.get("comment"),
                    r.get("recommendation"),
                ))

        conn.commit()
        logger.info(f"✅ Macro AI Agent voltooid voor user_id={user_id}")

    except Exception:
        conn.rollback()
        logger.error("❌ Macro AI Agent FOUT")
        logger.error(traceback.format_exc())

    finally:
        conn.close()
