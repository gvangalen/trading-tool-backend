import logging
import traceback
from datetime import datetime
from celery import shared_task
from backend.utils.db import get_db_connection
from backend.utils.openai_client import ask_gpt

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# ======================================================
# 📊 TECHNICAL AI AGENT
# ======================================================

@shared_task(name="backend.ai_agents.technical_ai_agent.generate_technical_insight")
def generate_technical_insight():
    """Analyseert technische indicatoren en genereert AI-interpretatie."""
    logger.info("📊 Start Technical AI Agent...")

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding.")
        return

    try:
        # Laatste technische data ophalen (dagelijks)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT indicator, value, score, advies
                FROM technical_data_day
                WHERE date = CURRENT_DATE
                ORDER BY indicator ASC;
            """)
            rows = cur.fetchall()

        if not rows:
            logger.warning("⚠️ Geen technical_data_day gevonden voor vandaag.")
            return

        # Format data voor AI prompt
        data_text = "\n".join([
            f"{r[0]} → waarde: {r[1]}, score: {r[2]}, advies: {r[3]}"
            for r in rows
        ])

        prompt = f"""
        Je bent een technische analyse AI gespecialiseerd in Bitcoin.

        Hieronder staan de actuele technische indicatoren:
        {data_text}

        Analyseer dit en geef je output als JSON met:
        - trend: bullish, bearish of neutraal
        - momentum: sterk, matig of zwak
        - structuur: breakout, consolidatie of range
        - risk: laag, gemiddeld of hoog
        - summary: max 2 zinnen met interpretatie van de technische situatie
        - top_signals: lijst van de 2-4 belangrijkste indicatoren die deze analyse bepalen
        """

        ai_response = ask_gpt(prompt)

        if not ai_response:
            logger.warning("⚠️ Geen AI-response ontvangen.")
            return

        logger.info(f"🧠 AI technische interpretatie ontvangen: {ai_response}")

        # Resultaat opslaan
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO ai_category_insights (category, avg_score, trend, bias, risk, summary, top_signals)
                VALUES ('technical', NULL, %s, NULL, %s, %s, %s)
                ON CONFLICT (category, date) DO UPDATE SET
                    trend = EXCLUDED.trend,
                    risk = EXCLUDED.risk,
                    summary = EXCLUDED.summary,
                    top_signals = EXCLUDED.top_signals,
                    created_at = NOW();
            """, (
                ai_response.get("trend"),
                ai_response.get("risk"),
                ai_response.get("summary"),
                ai_response.get("top_signals"),
            ))

        conn.commit()
        logger.info("✅ Technical AI Insight opgeslagen in ai_category_insights.")

    except Exception:
        logger.error("❌ Fout in Technical AI Agent:")
        logger.error(traceback.format_exc())

    finally:
        conn.close()
