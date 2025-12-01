import logging
import traceback
import json
from datetime import datetime
from celery import shared_task

from backend.utils.db import get_db_connection
from backend.utils.openai_client import ask_gpt

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# =====================================================================
# 📊 TECHNICAL AI AGENT — FINAL FIXED VERSION (Matches Macro & Market)
# =====================================================================
@shared_task(name="backend.ai_agents.technical_ai_agent.generate_technical_insight")
def generate_technical_insight():
    logger.info("📊 Start Technical AI Agent (FINAL FIX)...")

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding.")
        return

    try:
        # ------------------------------------------------------
        # 1️⃣ Scoreregels ophalen
        # ------------------------------------------------------
        with conn.cursor() as cur:
            cur.execute("""
                SELECT indicator, range_min, range_max, score, interpretation, action
                FROM technical_indicator_rules
                ORDER BY indicator ASC, score ASC
            """)
            rule_rows = cur.fetchall()

        rules_by_indicator = {}
        for indicator, rmin, rmax, score, interp, action in rule_rows:
            rules_by_indicator.setdefault(indicator, []).append({
                "range_min": float(rmin),
                "range_max": float(rmax),
                "score": int(score),
                "interpretation": interp,
                "action": action
            })

        logger.info(f"📘 Scoreregels geladen voor {len(rules_by_indicator)} indicatoren.")


        # ------------------------------------------------------
        # 2️⃣ Technische dagwaarden ophalen
        # ------------------------------------------------------
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    indicator,
                    value,
                    score,
                    advies,
                    uitleg
                FROM technical_indicators
                WHERE timestamp::date = CURRENT_DATE
                ORDER BY indicator ASC;
            """)
            rows = cur.fetchall()

        if not rows:
            logger.warning("⚠️ Geen technical_indicators voor vandaag — AI agent slaat over.")
            return

        combined = []
        score_values = []

        for (name, value, score, advies, uitleg) in rows:
            score_float = float(score)
            combined.append({
                "indicator": name,
                "value": float(value),
                "score": score_float,
                "advies": advies or "",
                "uitleg": uitleg or "",
                "rules": rules_by_indicator.get(name, [])
            })
            score_values.append(score_float)

        # Bereken het gemiddelde (voor avg_score)
        avg_score = round(sum(score_values) / len(score_values), 2)

        # Bouw AI prompt
        data_text = "\n".join([
            f"{c['indicator']}: value={c['value']}, score={c['score']}, advies={c['advies']}, rules={json.dumps(c['rules'], ensure_ascii=False)}"
            for c in combined
        ])


        # ------------------------------------------------------
        # 3️⃣ AI hoofd-samenvatting
        # ------------------------------------------------------
        prompt_context = f"""
Je bent een technische analyse AI gespecialiseerd in Bitcoin.
Hier zijn alle technische indicatoren en scoreregels:

{data_text}

Geef geldige JSON terug:
{{
  "trend": "",
  "bias": "",
  "momentum": "",
  "summary": "",
  "top_signals": []
}}
        """

        ai_context = ask_gpt(
            prompt_context,
            system_role="Je bent een professionele technische analyse expert. Antwoord ALTIJD in geldige JSON."
        )

        if not isinstance(ai_context, dict):
            ai_context = {
                "trend": "",
                "bias": "",
                "momentum": "",
                "summary": str(ai_context)[:200],
                "top_signals": []
            }

        logger.info(f"🧠 AI-context technical: {ai_context}")


        # ------------------------------------------------------
        # 4️⃣ AI reflecties (per indicator)
        # ------------------------------------------------------
        prompt_reflection = f"""
Maak een JSON-lijst met reflecties per indicator:

Elk item:
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

        ai_reflections = ask_gpt(
            prompt_reflection,
            system_role="Je bent een technische analyse expert. Antwoord in een JSON-lijst."
        )

        if not isinstance(ai_reflections, list):
            ai_reflections = []


        # ------------------------------------------------------
        # 5️⃣ Opslaan in ai_category_insights (BELANGRIJK!)
        # ------------------------------------------------------
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO ai_category_insights
                    (category, avg_score, trend, bias, risk, summary, top_signals)
                VALUES
                    ('technical', %s, %s, %s, %s, %s, %s)
                ON CONFLICT (category, date)
                DO UPDATE SET
                    avg_score  = EXCLUDED.avg_score,
                    trend      = EXCLUDED.trend,
                    bias       = EXCLUDED.bias,
                    summary    = EXCLUDED.summary,
                    top_signals= EXCLUDED.top_signals,
                    created_at = NOW();
            """, (
                avg_score,
                ai_context.get("trend") or "",
                ai_context.get("bias") or "",
                None,  # risk wordt NIET gebruikt voor technical
                ai_context.get("summary") or "",
                json.dumps(ai_context.get("top_signals", [])),
            ))

        logger.info("💾 Technical category-insight opgeslagen.")


        # ------------------------------------------------------
        # 6️⃣ Reflecties opslaan
        # ------------------------------------------------------
        for r in ai_reflections:
            ind = r.get("indicator")
            if not ind:
                continue

            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO ai_reflections
                        (category, indicator, raw_score, ai_score, compliance, comment, recommendation)
                    VALUES
                        ('technical', %s, NULL, %s, %s, %s, %s)
                    ON CONFLICT (category, indicator, date)
                    DO UPDATE SET
                        ai_score      = EXCLUDED.ai_score,
                        compliance    = EXCLUDED.compliance,
                        comment       = EXCLUDED.comment,
                        recommendation= EXCLUDED.recommendation,
                        timestamp     = NOW();
                """, (
                    ind,
                    r.get("ai_score"),
                    r.get("compliance"),
                    r.get("comment"),
                    r.get("recommendation")
                ))

        conn.commit()
        logger.info("✅ Technical AI Agent COMPLETED.")

    except Exception:
        logger.error("❌ Fout in Technical AI Agent:")
        logger.error(traceback.format_exc())

    finally:
        conn.close()
