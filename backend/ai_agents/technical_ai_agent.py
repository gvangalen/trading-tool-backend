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
# 📊 TECHNICAL AI AGENT — FIXED FOR REAL DB STRUCTURE
# ======================================================
@shared_task(name="backend.ai_agents.technical_ai_agent.generate_technical_insight")
def generate_technical_insight():
    logger.info("📊 Start Technical AI Agent (FIXED)...")

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding.")
        return

    try:
        # ------------------------------------------------------
        # 1️⃣ Scoreregels ophalen (range_min / range_max)
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

        logger.info(f"📘 Regels geladen voor {len(rules_by_indicator)} technische indicatoren.")


        # ------------------------------------------------------
        # 2️⃣ Technische indicatoren ophalen (FIXED → timestamp::date)
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
            logger.warning("⚠️ Geen technical_indicators gevonden voor vandaag.")
            return

        combined = []
        for (name, value, score, advies, uitleg) in rows:
            combined.append({
                "indicator": name,
                "value": float(value),
                "score": float(score),
                "advies": advies,
                "uitleg": uitleg,
                "rules": rules_by_indicator.get(name, [])
            })

        # Prompttekst opbouwen
        data_text = "\n".join([
            f"{c['indicator']}: value={c['value']}, score={c['score']}, advies={c['advies']}, "
            f"uitleg={c['uitleg']}, rules={json.dumps(c['rules'], ensure_ascii=False)}"
            for c in combined
        ])


        # ------------------------------------------------------
        # 3️⃣ AI Context genereren
        # ------------------------------------------------------
        prompt_context = f"""
Je bent een technische analyse AI gespecialiseerd in Bitcoin.

Hieronder de technische indicatoren + scoreregels:

{data_text}

Geef ALLEEN geldige JSON terug:
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
            system_role="Je bent een professionele crypto technical analyst. Antwoord ALTIJD in geldige JSON."
        )

        if not isinstance(ai_context, dict):
            ai_context = {
                "trend": None,
                "bias": None,
                "momentum": None,
                "summary": str(ai_context)[:200],
                "top_signals": []
            }


        # ------------------------------------------------------
        # 4️⃣ AI Reflecties genereren
        # ------------------------------------------------------
        prompt_reflection = f"""
Maak een JSON-lijst met per indicator:

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
            system_role="Je bent een technische analyse expert. Antwoord in geldige JSON-lijst."
        )

        if not isinstance(ai_reflections, list):
            ai_reflections = []

        logger.info(f"🧠 Technical AI context: {ai_context}")
        logger.info(f"🪞 Reflecties gegenereerd: {len(ai_reflections)}")


        # ------------------------------------------------------
        # 5️⃣ Opslaan categorie-samenvatting (ai_category_insights)
        # ------------------------------------------------------
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO ai_category_insights
                    (category, avg_score, trend, bias, risk, summary, top_signals)
                VALUES ('technical', NULL, %s, %s, NULL, %s, %s)
                ON CONFLICT (category, date)
                DO UPDATE SET
                    trend   = EXCLUDED.trend,
                    bias    = EXCLUDED.bias,
                    summary = EXCLUDED.summary,
                    top_signals = EXCLUDED.top_signals,
                    created_at = NOW();
            """, (
                ai_context.get("trend"),
                ai_context.get("bias"),
                ai_context.get("summary"),
                json.dumps(ai_context.get("top_signals", [])),
            ))


        # ------------------------------------------------------
        # 6️⃣ Reflecties opslaan
        # ------------------------------------------------------
        for r in ai_reflections:
            ind = r.get("indicator")
            if not ind:
                continue

            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO ai_reflections (
                        category, indicator, raw_score, ai_score, compliance, comment, recommendation
                    )
                    VALUES ('technical', %s, NULL, %s, %s, %s, %s)
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
        logger.info("✅ Technical AI Agent voltooid.")

    except Exception:
        logger.error("❌ Fout in Technical AI Agent:")
        logger.error(traceback.format_exc())

    finally:
        conn.close()
