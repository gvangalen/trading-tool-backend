import logging
import json
from datetime import datetime
from celery import shared_task

from backend.utils.db import get_db_connection
from backend.utils.openai_client import ask_gpt  # JSON-engine

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# ===================================================================
# 📡 Laad AI-insights (macro / market / technical / setup / strategy)
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
                    SELECT category, avg_score, trend, bias, risk, summary,
                           top_signals, date, created_at
                    FROM ai_category_insights
                    WHERE user_id = %s
                    ORDER BY date DESC, created_at DESC;
                """, (user_id,))
            else:
                cur.execute("""
                    SELECT category, avg_score, trend, bias, risk, summary,
                           top_signals, date, created_at
                    FROM ai_category_insights
                    ORDER BY date DESC, created_at DESC;
                """)

            rows = cur.fetchall()

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
                "date": d.isoformat() if d else None,
                "created_at": created_at.isoformat() if created_at else None,
            }

        return insights

    except Exception as e:
        logger.error(f"❌ load_ai_insights fout: {e}", exc_info=True)
        return {}

    finally:
        conn.close()


# ===================================================================
# 🎯 AI analyseert bestaande strategieën (GEEN generatie)
# ===================================================================
def analyze_strategies(strategies: list, ai_context: dict):
    prompt = f"""
Je bent een professionele trading-analist.

Analyseer deze strategieën.
MAAK GEEN NIEUWE STRATEGIEËN.

Context:
{json.dumps(ai_context, indent=2)}

Strategieën:
{json.dumps(strategies, indent=2)}

Geef ALLEEN geldige JSON terug.

JSON format:
{{
  "avg_score": 0-100,
  "trend": "Bullish | Bearish | Neutraal",
  "bias": "Kansen | Afwachten | Risico",
  "risk": "Laag | Gemiddeld | Hoog",
  "summary": "Korte samenvatting",
  "top_signals": [
    {{ "name": "", "reason": "" }}
  ]
}}
"""

    response = ask_gpt(
        prompt,
        system_role="Je bent een crypto-strategie analist. Alleen geldige JSON."
    )

    if not isinstance(response, dict):
        logger.error("❌ Ongeldige JSON in analyze_strategies")
        return None

    return response


# ===================================================================
# 🕒 CELERY — STRATEGY ANALYSE (DASHBOARD / AGENT)
# ===================================================================
@shared_task(name="backend.ai_agents.strategy_ai_agent.analyze_strategy_ai")
def analyze_strategy_ai(user_id: int | None = None):
    logger.info(f"🧠 Start Strategy ANALYSE AI (user_id={user_id})")

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Database fout")
        return

    try:
        with conn.cursor() as cur:
            if user_id:
                cur.execute("""
                    SELECT
                        id,
                        setup_id,
                        entry,
                        target,
                        stop_loss,
                        risk_profile,
                        explanation,
                        data,
                        created_at
                    FROM strategies
                    WHERE user_id = %s
                    ORDER BY created_at DESC;
                """, (user_id,))
            else:
                cur.execute("""
                    SELECT
                        id,
                        setup_id,
                        entry,
                        target,
                        stop_loss,
                        risk_profile,
                        explanation,
                        data,
                        created_at
                    FROM strategies
                    ORDER BY created_at DESC;
                """)

            rows = cur.fetchall()

        if not rows:
            logger.warning("⚠️ Geen strategieën gevonden")
            return

        strategies = []
        for (
            sid,
            setup_id,
            entry,
            target,
            stop_loss,
            risk_profile,
            explanation,
            data,
            created_at
        ) in rows:
            strategies.append({
                "strategy_id": sid,
                "setup_id": setup_id,
                "entry": entry,
                "target": target,
                "targets": data.get("targets") if isinstance(data, dict) else None,
                "stop_loss": stop_loss,
                "risk_profile": risk_profile,
                "explanation": explanation,
                "created_at": created_at.isoformat() if created_at else None,
            })

        ai_context = load_ai_insights(user_id)
        analysis = analyze_strategies(strategies, ai_context)

        if not analysis:
            logger.error("❌ Geen geldige AI analyse")
            return

        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO ai_category_insights
                    (category, user_id, avg_score, trend, bias, risk, summary, top_signals)
                VALUES ('strategy', %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (category, user_id, date)
                DO UPDATE SET
                    avg_score = EXCLUDED.avg_score,
                    trend = EXCLUDED.trend,
                    bias = EXCLUDED.bias,
                    risk = EXCLUDED.risk,
                    summary = EXCLUDED.summary,
                    top_signals = EXCLUDED.top_signals,
                    created_at = NOW();
            """, (
                user_id,
                analysis.get("avg_score"),
                analysis.get("trend"),
                analysis.get("bias"),
                analysis.get("risk"),
                analysis.get("summary"),
                json.dumps(analysis.get("top_signals", [])),
            ))

        conn.commit()
        logger.info("✅ Strategy analyse opgeslagen")

    except Exception as e:
        logger.error(f"❌ Strategy analyse fout: {e}", exc_info=True)

    finally:
        conn.close()


# ===================================================================
# 🚀 AI STRATEGY GENERATION (BESTAAND — NIET AAN GEZETEN)
# ===================================================================
def generate_strategy_from_setup(setup: dict, user_id: int):
    logger.info(f"⚙️ AI strategy generatie | setup={setup.get('id')} user={user_id}")

    prompt = f"""
Je bent een professionele crypto trader.

Genereer een CONCRETE tradingstrategie op basis van deze setup.

Setup:
{json.dumps(setup, indent=2)}

Vereisten:
- entry (string of range)
- targets (lijst)
- stop_loss
- risk_reward
- explanation (kort & concreet)

Geef ALLEEN geldige JSON terug.

JSON format:
{{
  "entry": "",
  "targets": [],
  "stop_loss": "",
  "risk_reward": "",
  "explanation": ""
}}
"""

    result = ask_gpt(
        prompt,
        system_role="Je bent een professionele trading AI. Alleen geldige JSON."
    )

    if not isinstance(result, dict):
        raise ValueError("AI strategy generatie gaf geen geldige JSON")

    return result
