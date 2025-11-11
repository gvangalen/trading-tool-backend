import logging
import traceback
from datetime import datetime
from celery import shared_task
from backend.utils.db import get_db_connection
from backend.utils.openai_client import ask_gpt
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# ======================================================
# 🌍 MACRO AI AGENT
# ======================================================

@shared_task(name="backend.ai_agents.macro_ai_agent.generate_macro_insight")
def generate_macro_insight():
    """Analyseert macro-indicatoren en genereert AI-interpretatie."""
    logger.info("🌍 Start Macro AI Agent...")

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding.")
        return

    try:
        # Laatste macrodata ophalen
        with conn.cursor() as cur:
            cur.execute("""
                SELECT indicator, value, score, advies
                FROM macro_data
                WHERE date = CURRENT_DATE
                ORDER BY indicator ASC;
            """)
            rows = cur.fetchall()

        if not rows:
            logger.warning("⚠️ Geen macro_data gevonden voor vandaag.")
            return

        # Format macrodata voor AI
        data_text = "\n".join([
            f"{r[0]} → waarde: {r[1]}, score: {r[2]}, advies: {r[3]}"
            for r in rows
        ])

        prompt = f"""
        Je bent een macro-economische analyse-AI gespecialiseerd in Bitcoin.

        Hieronder staan de actuele macro-indicatoren:
        {data_text}

        Geef je antwoord als JSON met:
        - trend: bullish, bearish of neutraal
        - bias: risk-on, risk-off of gemengd
        - risk: laag, gemiddeld of hoog
        - summary: max 2 zinnen met interpretatie van de macro-situatie
        - top_signals: lijst van opvallende indicatoren die de richting bepalen
        """

        ai_response = ask_gpt(prompt)

        if not ai_response:
            logger.warning("⚠️ Geen AI-response ontvangen.")
            return

        logger.info(f"🧠 AI macro interpretatie ontvangen: {ai_response}")

        # Resultaat opslaan
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
                ai_response.get("trend"),
                ai_response.get("bias"),
                ai_response.get("risk"),
                ai_response.get("summary"),
                ai_response.get("top_signals"),
            ))

        conn.commit()
        logger.info("✅ Macro AI Insight opgeslagen in ai_category_insights.")

    except Exception:
        logger.error("❌ Fout in Macro AI Agent:")
        logger.error(traceback.format_exc())

    finally:
        conn.close()
