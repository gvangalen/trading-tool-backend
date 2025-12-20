import logging
import traceback
import json

from backend.utils.db import get_db_connection
from backend.utils.openai_client import ask_gpt
from backend.utils.scoring_utils import generate_scores_db

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# ======================================================
# 🌍 MACRO AI AGENT — PURE AI + DB LOGICA
# ======================================================
def run_macro_agent(user_id: int):
    """
    Genereert macro AI insights voor één gebruiker.
    Schrijft zelf:
    - ai_category_insights (macro)
    - ai_reflections (macro)
    """

    if user_id is None:
        raise ValueError("❌ Macro AI Agent vereist een user_id")

    logger.info(f"🌍 [Macro-Agent] Start voor user_id={user_id}")

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding")
        return

    try:
        # =========================================================
        # 1️⃣ Macro scoreregels
        # =========================================================
        with conn.cursor() as cur:
            cur.execute("""
                SELECT indicator, range_min, range_max, score, trend, interpretation, action
                FROM macro_indicator_rules
                ORDER BY indicator, range_min;
            """)
            rows = cur.fetchall()

        rules_by_indicator = {}
        for ind, rmin, rmax, score, trend, interp, action in rows:
            rules_by_indicator.setdefault(ind, []).append({
                "range_min": float(rmin),
                "range_max": float(rmax),
                "score": int(score),
                "trend": trend,
                "interpretation": interp,
                "action": action,
            })

        # =========================================================
        # 2️⃣ Macro data van vandaag (user)
        # =========================================================
        with conn.cursor() as cur:
            cur.execute("""
                SELECT name, value, trend, interpretation, action, score, timestamp
                FROM macro_data
                WHERE user_id = %s
                  AND timestamp::date = CURRENT_DATE
                ORDER BY timestamp DESC;
            """, (user_id,))
            macro_rows = cur.fetchall()

        if not macro_rows:
            logger.info(f"ℹ️ [Macro-Agent] Geen macro_data vandaag (user_id={user_id})")
            return

        macro_items = [
            {
                "indicator": name,
                "value": float(value) if value is not None else None,
                "trend": trend,
                "interpretation": interp,
                "action": action,
                "score": float(score) if score is not None else None,
                "timestamp": ts.isoformat() if ts else None,
            }
            for name, value, trend, interp, action, score, ts in macro_rows
        ]

        # =========================================================
        # 3️⃣ Macro score (DB-gedreven)
        # =========================================================
        macro_scores = generate_scores_db("macro", user_id=user_id)
        macro_avg = macro_scores.get("total_score", 0)

        top_contributors = sorted(
            macro_scores.get("scores", {}).items(),
            key=lambda kv: kv[1].get("score", 0),
            reverse=True
        )[:3]

        top_pretty = [
            {
                "indicator": k,
                "value": v.get("value"),
                "score": v.get("score"),
                "trend": v.get("trend"),
                "interpretation": v.get("interpretation"),
            }
            for k, v in top_contributors
        ]

        # =========================================================
        # 4️⃣ AI SAMENVATTING
        # =========================================================
        payload = {
            "macro_items": macro_items,
            "macro_rules": rules_by_indicator,
            "macro_avg_score": macro_avg,
            "top_contributors": top_pretty,
        }

        prompt = f"""
Je bent een macro-economische analist gespecialiseerd in Bitcoin.

Analyseer de onderstaande macrodata.

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
            prompt,
            system_role="Je bent een professionele macro-analist. Antwoord uitsluitend in geldige JSON."
        )

        if not isinstance(ai_context, dict):
            raise ValueError("❌ Macro AI response geen geldige JSON")

        # =========================================================
        # 5️⃣ AI REFLECTIES PER INDICATOR
        # =========================================================
        reflections_prompt = f"""
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
            reflections_prompt,
            system_role="Je bent een macro-analist. Antwoord uitsluitend in geldige JSON."
        )

        if not isinstance(ai_reflections, list):
            ai_reflections = []

        # =========================================================
        # 6️⃣ Opslaan ai_category_insights
        # =========================================================
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO ai_category_insights
                    (category, user_id, avg_score, trend, bias, risk, summary, top_signals)
                VALUES ('macro', %s, %s, %s, %s, %s, %s, %s)
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
                macro_avg,
                ai_context["trend"],
                ai_context["bias"],
                ai_context["risk"],
                ai_context["summary"],
                json.dumps(ai_context.get("top_signals", [])),
            ))

        # =========================================================
        # 7️⃣ Opslaan ai_reflections
        # =========================================================
        for r in ai_reflections:
            if not r.get("indicator"):
                continue

            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO ai_reflections
                        (category, user_id, indicator, raw_score, ai_score, compliance, comment, recommendation)
                    VALUES ('macro', %s, %s, NULL, %s, %s, %s, %s)
                    ON CONFLICT (category, user_id, indicator, date)
                    DO UPDATE SET
                        ai_score = EXCLUDED.ai_score,
                        compliance = EXCLUDED.compliance,
                        comment = EXCLUDED.comment,
                        recommendation = EXCLUDED.recommendation,
                        timestamp = NOW();
                """, (
                    user_id,
                    r.get("indicator"),
                    r.get("ai_score"),
                    r.get("compliance"),
                    r.get("comment"),
                    r.get("recommendation"),
                ))

        conn.commit()
        logger.info(f"✅ [Macro-Agent] Voltooid voor user_id={user_id}")

    except Exception:
        conn.rollback()
        logger.error("❌ [Macro-Agent] Fout", exc_info=True)

    finally:
        conn.close()
