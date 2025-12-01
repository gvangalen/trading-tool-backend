import logging
import json
from datetime import datetime
from celery import shared_task

from backend.utils.db import get_db_connection
from backend.utils.openai_client import ask_gpt  # JSON-engine

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# ===================================================================
# 📡 Laad AI-insights (macro/market/technical/setup/strategy zelf)
# ===================================================================
def load_ai_insights():
    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen database in load_ai_insights")
        return {}

    insights = {}

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT category, avg_score, trend, bias, risk, summary, top_signals,
                       date, created_at
                FROM ai_category_insights
                WHERE date >= (CURRENT_DATE - INTERVAL '1 day')
                ORDER BY date DESC, created_at DESC;
            """)
            rows = cur.fetchall()

        # → Pak de meest recente per categorie
        for cat, avg, trend, bias, risk, summary, ts, d, created_at in rows:
            if cat in insights:
                continue
            insights[cat] = {
                "score": float(avg) if avg is not None else None,
                "trend": trend,
                "bias": bias,
                "risk": risk,
                "summary": summary,
                "top_signals": ts,
                "date": d.isoformat() if hasattr(d, "isoformat") else str(d),
                "created_at": created_at.isoformat() if isinstance(created_at, datetime) else None,
            }

        return insights

    except Exception as e:
        logger.error(f"❌ load_ai_insights fout: {e}", exc_info=True)
        return {}

    finally:
        conn.close()



# ===================================================================
# 🎯 AI — Analyseer bestaande strategieën
# ===================================================================
def analyze_strategies(strategies: list, ai_context: dict):
    """
    AI analyseert ALLEEN — maakt GEEN nieuwe strategieën.

    AI-output structuur:
    {
      "avg_score": 68,
      "trend": "Bullish",
      "bias": "Kansen",
      "risk": "Gemiddeld",
      "summary": "Korte analyse",
      "top_signals": [
          {"name": "...", "entry": "..."},
          ...
      ]
    }
    """
    prompt = f"""
Je bent een professionele trading-analist.

Analyseer onderstaande strategieën. 
MAAK GEEN NIEUWE STRATEGIEËN.

Output alleen geldige JSON.

---

📌 Context uit andere AI-agents:
{json.dumps(ai_context, indent=2)}

---

📌 Bestaande strategieën:
{json.dumps(strategies, indent=2)}

---

🎯 Maak een analyse met dit JSON formaat:

{{
  "avg_score": 0-100,
  "trend": "Bullish | Bearish | Neutraal",
  "bias": "Kansen | Afwachten | Risico",
  "risk": "Laag | Gemiddeld | Hoog",
  "summary": "Korte tekst over de strategieën van vandaag",
  "top_signals": [
    {{"name": "", "reason": ""}}
  ]
}}

BELANGRIJK:
- Gebruik alleen JSON.
- GEEN strategieën genereren.
"""
    response = ask_gpt(prompt, system_role="Je bent een crypto-strategie analist en gebruikt ONLY JSON.")

    if not isinstance(response, dict):
        logger.error("❌ Ongeldige JSON in analyse")
        return None

    return response



# ===================================================================
# 🕒 CELERY TASK — Strategy ANALYSE OPSLAAN
# ===================================================================
@shared_task(name="backend.ai_agents.strategy_ai_agent.analyze_strategy_ai")
def analyze_strategy_ai():
    logger.info("🧠 Start Strategy ANALYSE AI Agent...")

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Database fout.")
        return

    try:
        # -----------------------------------------------------------------
        # 1️⃣ BESTAANDE STRATEGIEËN OPHALEN (NIET AANPASSEN!)
        # -----------------------------------------------------------------
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, setup_id, entry, target, stop_loss, explanation, risk_profile, created_at
                FROM strategies
                ORDER BY created_at DESC;
            """)
            rows = cur.fetchall()

        if not rows:
            logger.warning("⚠️ Geen strategieën gevonden.")
            return

        strategies = []
        for (sid, setup_id, entry, target, sl, expl, risk, created_at) in rows:
            strategies.append({
                "strategy_id": sid,
                "setup_id": setup_id,
                "entry": entry,
                "targets": target.split(",") if target else [],
                "stop_loss": sl,
                "risk_reward": risk,
                "explanation": expl,
                "created_at": created_at.isoformat() if created_at else None
            })

        # -----------------------------------------------------------------
        # 2️⃣ LAAT AI STRATEGIEËN ANALYSEREN
        # -----------------------------------------------------------------
        ai_context = load_ai_insights()
        analysis = analyze_strategies(strategies, ai_context)

        if not analysis:
            logger.error("❌ Geen geldige strategy-analyse ontvangen")
            return

        # -----------------------------------------------------------------
        # 3️⃣ OPSLAAN ALS AI-INSIGHT (category = 'strategy')
        # -----------------------------------------------------------------
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO ai_category_insights
                    (category, avg_score, trend, bias, risk, summary, top_signals)
                VALUES ('strategy', %s, %s, %s, %s, %s, %s)
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
                analysis.get("avg_score"),
                analysis.get("trend"),
                analysis.get("bias"),
                analysis.get("risk"),
                analysis.get("summary"),
                json.dumps(analysis.get("top_signals", [])),
            ))

        conn.commit()

        logger.info("📊 Strategy AI-analyse opgeslagen.")

    except Exception as e:
        logger.error(f"❌ Strategy Analyse AI fout: {e}", exc_info=True)

    finally:
        conn.close()

    logger.info("✅ Strategy ANALYSE AI Agent voltooid.")
