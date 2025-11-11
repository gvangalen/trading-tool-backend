import logging
import traceback
from datetime import datetime
from celery import shared_task
from backend.utils.db import get_db_connection
from backend.ai_utils.openai_client import ask_gpt

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# ======================================================
# 🧮 SCORE AI AGENT
# ======================================================

@shared_task(name="backend.ai_agents.score_ai_agent.generate_master_score")
def generate_master_score():
    """Combineert macro-, market- en technical-insights tot één AI Master Score."""
    logger.info("🧮 Start Score AI Agent (Master Score)...")

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding.")
        return

    try:
        # 1️⃣ Haal alle category insights op van vandaag
        with conn.cursor() as cur:
            cur.execute("""
                SELECT category, trend, bias, risk, summary
                FROM ai_category_insights
                WHERE date = CURRENT_DATE
                ORDER BY category;
            """)
            rows = cur.fetchall()

        if not rows:
            logger.warning("⚠️ Geen AI category insights gevonden voor vandaag.")
            return

        # 2️⃣ Format data voor de AI
        category_text = "\n".join([
            f"[{r[0].upper()}] trend={r[1]}, bias={r[2]}, risk={r[3]} → {r[4]}"
            for r in rows
        ])

        prompt = f"""
        Jij bent een trading-analist-AI.  
        Hieronder vind je de huidige analyses van drie agents:
        {category_text}

        Analyseer dit als één samenhangend geheel en geef je antwoord in JSON:
        - master_trend: bullish / bearish / neutraal
        - master_bias: risk-on / risk-off / gemengd
        - master_risk: laag / gemiddeld / hoog
        - master_score: getal tussen 0-100 dat het algemene marktsentiment samenvat
        - summary: max 3 zinnen met jouw overkoepelende interpretatie
        - outlook: korte verwachting (bijv. 'kans op herstel', 'waarschuwing voor zwakte', etc.)
        """

        ai_response = ask_gpt(prompt)

        if not ai_response:
            logger.warning("⚠️ Geen AI-response ontvangen.")
            return

        logger.info(f"🧠 AI Master Score ontvangen: {ai_response}")

        # 3️⃣ Sla het op in ai_category_insights (category='score')
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO ai_category_insights 
                    (category, avg_score, trend, bias, risk, summary, top_signals)
                VALUES ('score', %s, %s, %s, %s, %s, %s)
                ON CONFLICT (category, date) DO UPDATE SET
                    avg_score = EXCLUDED.avg_score,
                    trend = EXCLUDED.trend,
                    bias = EXCLUDED.bias,
                    risk = EXCLUDED.risk,
                    summary = EXCLUDED.summary,
                    top_signals = EXCLUDED.top_signals,
                    created_at = NOW();
            """, (
                ai_response.get("master_score"),
                ai_response.get("master_trend"),
                ai_response.get("master_bias"),
                ai_response.get("master_risk"),
                ai_response.get("summary"),
                ai_response.get("outlook"),
            ))

        conn.commit()
        logger.info("✅ AI Master Score opgeslagen in ai_category_insights (category='score').")

    except Exception:
        logger.error("❌ Fout in Score AI Agent:")
        logger.error(traceback.format_exc())

    finally:
        conn.close()
