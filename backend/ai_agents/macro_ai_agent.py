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
# 🌍 MACRO AI AGENT – met regelcontext + reflectielaag
# ======================================================

@shared_task(name="backend.ai_agents.macro_ai_agent.generate_macro_insight")
def generate_macro_insight():
    """
    Analyseert macro-indicatoren met hun scoreregels, 
    genereert AI-interpretatie én reflectie per indicator.

    ⚙️ AI krijgt nu zowel de data van vandaag als de scoreregels uit de database.
    """
    logger.info("🌍 Start Macro AI Agent...")

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding.")
        return

    try:
        # === 1️⃣ Regels ophalen per indicator ===
        with conn.cursor() as cur:
            cur.execute("""
                SELECT indicator, rule_range, score, interpretation, action
                FROM macro_indicator_rules
                ORDER BY indicator ASC, score ASC;
            """)
            rule_rows = cur.fetchall()

        rules_by_indicator = {}
        for r in rule_rows:
            indicator, rule_range, score, interpretation, action = r
            rules_by_indicator.setdefault(indicator, []).append({
                "range": rule_range,
                "score": score,
                "interpretation": interpretation,
                "action": action
            })

        logger.info(f"📘 Regels geladen voor {len(rules_by_indicator)} macro-indicatoren.")

        # === 2️⃣ Laatste macrodata ophalen ===
        with conn.cursor() as cur:
            cur.execute("""
                SELECT indicator, value, score, advies, uitleg
                FROM macro_data
                WHERE date = CURRENT_DATE
                ORDER BY indicator ASC;
            """)
            data_rows = cur.fetchall()

        if not data_rows:
            logger.warning("⚠️ Geen macro_data gevonden voor vandaag.")
            return

        # === 3️⃣ Combineer regels + data ===
        combined_info = []
        for d in data_rows:
            indicator = d[0]
            combined_info.append({
                "indicator": indicator,
                "value": d[1],
                "score": d[2],
                "advies": d[3],
                "uitleg": d[4],
                "rules": rules_by_indicator.get(indicator, [])
            })

        # Tekstuele samenvatting voor AI
        data_text = "\n".join([
            f"{c['indicator']} → waarde: {c['value']}, score: {c['score']}, advies: {c['advies']}, regels: {json.dumps(c['rules'], ensure_ascii=False)}"
            for c in combined_info
        ])

        # === 4️⃣ Contextuele interpretatie (trend/bias/risk) ===
        prompt_context = f"""
        Je bent een macro-economische analyse-AI gespecialiseerd in Bitcoin.

        Hieronder staan de actuele macro-indicatoren en hun scoreregels:
        {data_text}

        Geef je antwoord als JSON met:
        - trend: bullish, bearish of neutraal
        - bias: risk-on, risk-off of gemengd
        - risk: laag, gemiddeld of hoog
        - summary: max 2 zinnen met interpretatie van de macro-situatie
        - top_signals: lijst van opvallende indicatoren die de richting bepalen
        """

        ai_response_context = ask_gpt(prompt_context)
        try:
            ai_context = json.loads(ai_response_context)
        except Exception:
            logger.warning("⚠️ AI-context kon niet als JSON worden geparsed.")
            ai_context = {"summary": ai_response_context[:200]}

        # === 5️⃣ Reflectie per indicator ===
        prompt_reflection = f"""
        Beoordeel per macro-indicator in onderstaande lijst:
        {data_text}

        Voor elke indicator:
        - ai_score (0-100): jouw herbeoordeling van de betrouwbaarheid vandaag
        - compliance (0-100): volgt de gebruiker zijn eigen regel of niet?
        - comment: korte reflectie op basis van waarde en regels
        - recommendation: 1 zin met suggestie of advies

        Geef als JSON-lijst, bv:
        [
          {{
            "indicator": "DXY",
            "ai_score": 65,
            "compliance": 90,
            "comment": "DXY stijgt licht, beperkt risico",
            "recommendation": "Let op als DXY > 107"
          }},
          ...
        ]
        """

        ai_response_reflection = ask_gpt(prompt_reflection)
        try:
            ai_reflections = json.loads(ai_response_reflection)
            if not isinstance(ai_reflections, list):
                ai_reflections = []
        except Exception:
            logger.warning("⚠️ Reflectie kon niet als JSON worden geparsed.")
            ai_reflections = []

        logger.info(f"🧠 AI macro interpretatie: {ai_context}")
        logger.info(f"🪞 AI macro reflecties: {len(ai_reflections)} items")

        # === 6️⃣ Opslaan interpretatie (samenvatting) ===
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO ai_category_insights (category, avg_score, trend, bias, risk, summary, top_signals)
                VALUES ('macro', NULL, %s, %s, %s, %s, %s)
                ON CONFLICT (category, date) DO UPDATE SET
                    trend = EXCLUDED.trend,
                    bias = EXCLUDED.bias,
                    risk = EXCLUDED.risk,
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

        # === 7️⃣ Opslaan individuele reflecties ===
        for r in ai_reflections:
            indicator = r.get("indicator")
            ai_score = r.get("ai_score")
            compliance = r.get("compliance")
            comment = r.get("comment")
            recommendation = r.get("recommendation")

            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO ai_reflections (category, indicator, raw_score, ai_score, compliance, comment, recommendation)
                    VALUES ('macro', %s, NULL, %s, %s, %s, %s)
                    ON CONFLICT (category, indicator, date)
                    DO UPDATE SET
                        ai_score = EXCLUDED.ai_score,
                        compliance = EXCLUDED.compliance,
                        comment = EXCLUDED.comment,
                        recommendation = EXCLUDED.recommendation,
                        timestamp = NOW();
                """, (indicator, ai_score, compliance, comment, recommendation))

        conn.commit()
        logger.info("✅ Macro AI insights + reflecties succesvol opgeslagen.")

    except Exception:
        logger.error("❌ Fout in Macro AI Agent:")
        logger.error(traceback.format_exc())

    finally:
        conn.close()
