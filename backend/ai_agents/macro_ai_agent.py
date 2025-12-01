import logging
import traceback
import json
from datetime import datetime
from celery import shared_task

from backend.utils.db import get_db_connection
from backend.utils.openai_client import ask_gpt

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# ======================================================
# 🌍 MACRO AI AGENT — volledig gefixt volgens DB structuur
# ======================================================
@shared_task(name="backend.ai_agents.macro_ai_agent.generate_macro_insight")
def generate_macro_insight():
    logger.info("🌍 Start Macro AI Agent (V2, FIXED)...")

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding.")
        return

    try:
        # =====================================================
        # 1️⃣ Macro scoreregels ophalen (range_min / range_max)
        # =====================================================
        with conn.cursor() as cur:
            cur.execute("""
                SELECT indicator, range_min, range_max, score, interpretation, action
                FROM macro_indicator_rules
                ORDER BY indicator ASC, score ASC;
            """)
            rule_rows = cur.fetchall()

        rules_by_indicator = {}
        for indicator, rmin, rmax, score, interp, action in rule_rows:
            rules_by_indicator.setdefault(indicator, []).append({
                "range_min": float(rmin),
                "range_max": float(rmax),
                "score": int(score),
                "interpretation": interp,
                "action": action,
            })

        logger.info(f"📘 Macro-regels geladen voor {len(rules_by_indicator)} indicatoren.")


        # =====================================================
        # 2️⃣ Macro-data ophalen (FIXED → timestamp::date)
        # =====================================================
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    name,       -- indicator name!
                    value,
                    score,
                    interpretation,
                    action,
                    trend
                FROM macro_data
                WHERE timestamp::date = CURRENT_DATE
                ORDER BY name ASC;
            """)
            rows = cur.fetchall()

        if not rows:
            logger.warning("⚠️ Geen macro_data gevonden voor vandaag (timestamp::date).")
            return

        combined = []
        for (name, value, score, interp, action, trend) in rows:
            combined.append({
                "indicator": name,
                "value": float(value),
                "score": float(score),
                "interpretation": interp,
                "action": action,
                "trend": trend,
                "rules": rules_by_indicator.get(name, []),
            })

        # prompt tekst
        data_text = "\n".join([
            f"{c['indicator']}: value={c['value']}, score={c['score']}, trend={c['trend']}, "
            f"interpretation={c['interpretation']}, rules={json.dumps(c['rules'], ensure_ascii=False)}"
            for c in combined
        ])


        # =====================================================
        # 3️⃣ AI Macro-interpretatie
        # =====================================================
        prompt_context = f"""
Je bent een macro-economische analyse-AI gespecialiseerd in Bitcoin.

Hieronder staan de actuele macro-indicatoren + scoreregels:

{data_text}

Geef ALLEEN geldige JSON:
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
            system_role="Je bent een professionele macro-analist. Antwoord ALTIJD in geldige JSON."
        )

        if not isinstance(ai_context, dict):
            logger.warning("⚠️ Geen dict → fallback.")
            ai_context = {
                "trend": None,
                "bias": None,
                "risk": None,
                "summary": str(ai_context)[:300],
                "top_signals": [],
            }


        # =====================================================
        # 4️⃣ AI Reflecties per indicator
        # =====================================================
        prompt_reflection = f"""
Genereer een JSON-lijst. Per item:

{{
  "indicator": "",
  "ai_score": 0,
  "compliance": 0,
  "comment": "",
  "recommendation": ""
}}

Indicatoren:
{data_text}
        """

        ai_ref = ask_gpt(
            prompt_reflection,
            system_role="Je bent een professionele macro-analist. Antwoord in JSON-lijst."
        )

        if not isinstance(ai_ref, list):
            logger.warning("⚠️ Reflecties geen lijst → fallback lege lijst.")
            ai_ref = []


        # =====================================================
        # 5️⃣ Opslaan macro category insight
        # =====================================================
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO ai_category_insights
                    (category, avg_score, trend, bias, risk, summary, top_signals)
                VALUES ('macro', NULL, %s, %s, %s, %s, %s)
                ON CONFLICT (category, date)
                DO UPDATE SET
                    trend = EXCLUDED.trend,
                    bias  = EXCLUDED.bias,
                    risk  = EXCLUDED.risk,
                    summary = EXCLUDED.summary,
                    top_signals = EXCLUDED.top_signals,
                    created_at = NOW();
            """, (
                ai_context.get("trend"),
                ai_context.get("bias"),
                ai_context.get("risk"),
                ai_context.get("summary"),
                json.dumps(ai_context.get("top_signals", [])),
            ))


        # =====================================================
        # 6️⃣ Opslaan individuele reflecties
        # =====================================================
        for r in ai_ref:
            ind = r.get("indicator")
            if not ind:
                continue

            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO ai_reflections (
                        category, indicator, raw_score, ai_score, compliance, comment, recommendation
                    )
                    VALUES ('macro', %s, NULL, %s, %s, %s, %s)
                    ON CONFLICT (category, indicator, date)
                    DO UPDATE SET
                        ai_score = EXCLUDED.ai_score,
                        compliance = EXCLUDED.compliance,
                        comment = EXCLUDED.comment,
                        recommendation = EXCLUDED.recommendation,
                        timestamp = NOW();
                """, (
                    ind,
                    r.get("ai_score"),
                    r.get("compliance"),
                    r.get("comment"),
                    r.get("recommendation"),
                ))

        conn.commit()
        logger.info("✅ Macro AI insights + reflecties opgeslagen.")

    except Exception:
        logger.error("❌ Macro Agent FOUT:")
        logger.error(traceback.format_exc())

    finally:
        conn.close()
