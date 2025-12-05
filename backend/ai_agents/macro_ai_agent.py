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
# 🌍 MACRO AI AGENT – USER-AWARE
# ======================================================

@shared_task(name="backend.ai_agents.macro_ai_agent.generate_macro_insight")
def generate_macro_insight(user_id: int | None = None):
    """
    Analyseert macro-indicatoren op basis van:
    - macro_data (name, value, trend, interpretation, action, score, user_id)
    - macro_indicator_rules (range_min/range_max logica)
    - samengestelde macro-score via generate_scores_db("macro")

    Output:
    - ai_category_insights (samenvatting)
    - ai_reflections (reflecties per indicator)

    🧠 Als user_id is meegegeven → draait per gebruiker.
    Zonder user_id → valt terug op globale (oude) variant
    die geen user-filter gebruikt (alleen als je tabellen nog geen user_id hebben).
    """

    logger.info(f"🌍 Start Macro AI Agent... user_id={user_id}")

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding.")
        return

    try:
        # =========================================================
        # 1️⃣ Scoreregels uit database (global, geen user-filter)
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
        # 2️⃣ Laatste macro waardes ophalen (user-specifiek indien mogelijk)
        # =========================================================
        with conn.cursor() as cur:
            if user_id is not None:
                # User-specifieke macrodata
                cur.execute("""
                    SELECT name, value, trend, interpretation, action, score, timestamp
                    FROM macro_data
                    WHERE timestamp::date = CURRENT_DATE
                      AND user_id = %s
                    ORDER BY timestamp DESC;
                """, (user_id,))
            else:
                # Backwards compatible: geen user_id kolom / globale data
                cur.execute("""
                    SELECT name, value, trend, interpretation, action, score, timestamp
                    FROM macro_data
                    WHERE timestamp::date = CURRENT_DATE
                    ORDER BY timestamp DESC;
                """)

            macro_rows = cur.fetchall()

        if not macro_rows:
            logger.warning(f"⚠️ Geen macro_data gevonden voor vandaag (user_id={user_id}).")
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
        # 3️⃣ Macro-score vanuit scoringsengine
        #    (nu nog globaal; kan later user-specific gemaakt worden)
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
            "top_contributors": top_contrib_pretty,
            "user_id": user_id,
        }

        prompt_context = f"""
Je bent een macro-economische AI-analist gespecialiseerd in Bitcoin.

Hieronder vind je:
- actuele macro-indicatoren
- scoreregels
- samengestelde macro-score
- user_id (voor welke gebruiker deze analyse geldt, indien aanwezig)

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
{json.dumps(macro_items, ensure_ascii=False, indent=2)}
"""

        ai_reflections = ask_gpt(
            prompt_reflection,
            system_role="Je bent een macro-analist. Antwoord in een JSON-lijst."
        )

        if not isinstance(ai_reflections, list):
            ai_reflections = []

        # =========================================================
        # 6️⃣ Opslaan ai_category_insights (user-specifiek)
        # =========================================================
        with conn.cursor() as cur:
            if user_id is not None:
                # Nieuwe variant: met user_id + unieke key (category, user_id, date)
                cur.execute("""
                    INSERT INTO ai_category_insights
                        (category, user_id, avg_score, trend, bias, risk, summary, top_signals)
                    VALUES ('macro', %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (category, user_id, date)
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
                    ai_context.get("trend"),
                    ai_context.get("bias"),
                    ai_context.get("risk"),
                    ai_context.get("summary"),
                    json.dumps(ai_context.get("top_signals", [])),
                ))
            else:
                # Backwards compat: oude schema zonder user_id
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
                    json.dumps(ai_context.get("top_signals", [])),
                ))

        # =========================================================
        # 7️⃣ Opslaan ai_reflections (user-specifiek)
        # =========================================================
        for r in ai_reflections:
            indicator = r.get("indicator")
            if not indicator:
                continue

            with conn.cursor() as cur:
                if user_id is not None:
                    cur.execute("""
                        INSERT INTO ai_reflections
                            (category, user_id, indicator, raw_score, ai_score, compliance, comment, recommendation)
                        VALUES ('macro', %s, %s, NULL, %s, %s, %s, %s)
                        ON CONFLICT (category, user_id, indicator, date)
                        DO UPDATE SET
                            ai_score      = EXCLUDED.ai_score,
                            compliance    = EXCLUDED.compliance,
                            comment       = EXCLUDED.comment,
                            recommendation= EXCLUDED.recommendation,
                            timestamp     = NOW();
                    """, (
                        user_id,
                        indicator,
                        r.get("ai_score"),
                        r.get("compliance"),
                        r.get("comment"),
                        r.get("recommendation"),
                    ))
                else:
                    # Oude schema: geen user_id kolom
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
                        r.get("recommendation"),
                    ))

        conn.commit()
        logger.info(f"✅ Macro AI insights + reflecties opgeslagen (user_id={user_id}).")

    except Exception:
        logger.error("❌ Macro Agent FOUT:")
        logger.error(traceback.format_exc())

    finally:
        conn.close()
