import logging
import json
from datetime import datetime
from celery import shared_task

from backend.utils.db import get_db_connection
from backend.utils.openai_client import ask_gpt  # JSON-engine

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# ===================================================================
# 📡 Laad AI-insights (macro/market/technical/setup/strategy)
# ===================================================================
def load_ai_insights(user_id: int | None):
    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen database in load_ai_insights")
        return {}

    insights = {}

    try:
        with conn.cursor() as cur:
            if user_id:
                cur.execute("""
                    SELECT category, avg_score, trend, bias, risk, summary, top_signals,
                           date, created_at
                    FROM ai_category_insights
                    WHERE user_id = %s
                      AND date >= (CURRENT_DATE - INTERVAL '1 day')
                    ORDER BY date DESC, created_at DESC;
                """, (user_id,))
            else:
                # backwards compatible
                cur.execute("""
                    SELECT category, avg_score, trend, bias, risk, summary, top_signals,
                           date, created_at
                    FROM ai_category_insights
                    WHERE date >= (CURRENT_DATE - INTERVAL '1 day')
                    ORDER BY date DESC, created_at DESC;
                """)

            rows = cur.fetchall()

        # → Pak per categorie de nieuwste entry
        for cat, avg, trend, bias, risk, summary, top_signals, d, created_at in rows:
            if cat in insights:
                continue
            insights[cat] = {
                "score": float(avg) if avg is not None else None,
                "trend": trend,
                "bias": bias,
                "risk": risk,
                "summary": summary,
                "top_signals": top_signals,
                "date": d.isoformat() if hasattr(d, "isoformat") else str(d),
                "created_at": created_at.isoformat() if hasattr(created_at, "isoformat") else None,
            }

        return insights

    except Exception as e:
        logger.error(f"❌ load_ai_insights fout: {e}", exc_info=True)
        return {}

    finally:
        conn.close()


# ===================================================================
# 🎯 AI analyseert bestaande strategieën
# ===================================================================
def analyze_strategies(strategies: list, ai_context: dict):
    """
    AI analyseert ALLEEN — maakt GEEN nieuwe strategieën.
    """
    prompt = f"""
Je bent een professionele trading-analist.

Analyseer deze strategieën. 
MAAK GEEN NIEUWE STRATEGIEËN.

Context:
{json.dumps(ai_context, indent=2)}

Strategieën:
{json.dumps(strategies, indent=2)}

---

JSON format:

{{
  "avg_score": 0-100,
  "trend": "Bullish | Bearish | Neutraal",
  "bias": "Kansen | Afwachten | Risico",
  "risk": "Laag | Gemiddeld | Hoog",
  "summary": "Korte tekst",
  "top_signals": [
    {{"name": "", "reason": ""}}
  ]
}}
"""
    response = ask_gpt(
        prompt,
        system_role="Je bent een crypto-strategie analist. ALLEEN geldige JSON."
    )

    if not isinstance(response, dict):
        logger.error("❌ Ongeldige JSON in analyse")
        return None

    return response


# ===================================================================
# 🕒 CELERY TASK — Strategy AI ANALYSE (USER-AWARE!!)
# ===================================================================
@shared_task(name="backend.ai_agents.strategy_ai_agent.analyze_strategy_ai")
def analyze_strategy_ai(user_id: int | None = None):
    logger.info(f"🧠 Start Strategy ANALYSE AI Agent (user_id={user_id})")

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Database fout.")
        return

    try:
        # -----------------------------------------------------------------
        # 1️⃣ STRATEGIEËN OPHALEN
        # -----------------------------------------------------------------
        with conn.cursor() as cur:
            if user_id:
                cur.execute("""
                    SELECT id, setup_id, entry, target, stop_loss, explanation,
                           risk_profile, created_at
                    FROM strategies
                    WHERE user_id = %s
                    ORDER BY created_at DESC;
                """, (user_id,))
            else:
                cur.execute("""
                    SELECT id, setup_id, entry, target, stop_loss, explanation,
                           risk_profile, created_at
                    FROM strategies
                    ORDER BY created_at DESC;
                """)

            rows = cur.fetchall()

        if not rows:
            logger.warning(f"⚠️ Geen strategieën gevonden (user_id={user_id})")
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
                "created_at": created_at.isoformat() if created_at else None,
            })

        # -----------------------------------------------------------------
        # 2️⃣ AI CONTEXT LADEN
        # -----------------------------------------------------------------
        ai_context = load_ai_insights(user_id=user_id)

        # -----------------------------------------------------------------
        # 3️⃣ ANALYSE
        # -----------------------------------------------------------------
        analysis = analyze_strategies(strategies, ai_context)

        if not analysis:
            logger.error("❌ Geen geldige strategy-analyse ontvangen")
            return

        # -----------------------------------------------------------------
        # 4️⃣ OPSLAAN
        # -----------------------------------------------------------------
        with conn.cursor() as cur:
            if user_id:
                cur.execute("""
                    INSERT INTO ai_category_insights
                        (category, user_id, avg_score, trend, bias, risk, summary, top_signals)
                    VALUES ('strategy', %s, %s, %s, %s, %s, %s, %s)
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
                    analysis.get("avg_score"),
                    analysis.get("trend"),
                    analysis.get("bias"),
                    analysis.get("risk"),
                    analysis.get("summary"),
                    json.dumps(analysis.get("top_signals", [])),
                ))
            else:
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
        logger.info(f"📊 Strategy AI-analyse opgeslagen voor user_id={user_id}")

    except Exception as e:
        logger.error(f"❌ Strategy Analyse AI fout: {e}", exc_info=True)

    finally:
        conn.close()

    logger.info("✅ Strategy ANALYSE AI Agent voltooid.")


# ===================================================================
# 🚀 AI STRATEGY GENERATION (WRAPPER – BELANGRIJKE FIX)
# ===================================================================
def generate_strategy_from_setup(setup: dict, user_id: int):
    """
    Wrapper-functie zodat Celery import NIET faalt.
    Wordt gebruikt door backend.celery_task.strategy_task
    """

    from backend.ai_agents.strategy_generator import generate_strategy

    logger.info(f"⚙️ Generate strategy from setup {setup.get('id')} (user_id={user_id})")

    return generate_strategy(
        setup=setup,
        user_id=user_id
    )
