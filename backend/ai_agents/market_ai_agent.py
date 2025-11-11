import logging
import traceback
from datetime import datetime
from celery import shared_task
from backend.utils.db import get_db_connection
from backend.utils.openai_client import ask_gpt

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# ======================================================
# 🪙 MARKET AI AGENT
# ======================================================

@shared_task(name="backend.ai_agents.market_ai_agent.generate_market_insight")
def generate_market_insight():
    """Analyseert marktdata (prijs, volume, momentum) en genereert AI-interpretatie."""
    logger.info("🪙 Start Market AI Agent...")

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding.")
        return

    try:
        with conn.cursor() as cur:
            # Laatste 7 dagen uit market_data_7d
            cur.execute("""
                SELECT date, open, close, high, low, volume, change
                FROM market_data_7d
                ORDER BY date DESC
                LIMIT 7;
            """)
            rows = cur.fetchall()

        if not rows:
            logger.warning("⚠️ Geen market_data_7d gevonden.")
            return

        # Data formatteren voor prompt
        data_text = "\n".join([
            f"{r[0]} | O:{r[1]} H:{r[2]} L:{r[3]} C:{r[4]} Δ:{r[6]}% Vol:{r[5]/1e9:.2f}B"
            for r in rows
        ])

        prompt = f"""
        Je bent een professionele Bitcoin-marktanalyse-AI.

        Hier zijn de laatste 7 dagen marktdata:
        {data_text}

        Geef een korte analyse in JSON:
        - trend: bullish, bearish of neutraal
        - bias: opwaarts, neerwaarts of consolidatie
        - risk: laag, gemiddeld of hoog
        - summary: max 2 zinnen
        - top_signals: opsomming van opvallende patronen (zoals 'sterk volume bij daling', 'lange wicks', etc.)
        """

        ai_response = ask_gpt(prompt)

        if not ai_response:
            logger.warning("⚠️ Geen AI-response ontvangen.")
            return

        logger.info(f"🧠 AI interpretatie ontvangen: {ai_response}")

        # Resultaat opslaan
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO ai_category_insights (category, avg_score, trend, bias, risk, summary, top_signals)
                VALUES ('market', NULL, %s, %s, %s, %s, %s)
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
        logger.info("✅ Market AI Insight opgeslagen in ai_category_insights.")

    except Exception:
        logger.error("❌ Fout in Market AI Agent:")
        logger.error(traceback.format_exc())

    finally:
        conn.close()
