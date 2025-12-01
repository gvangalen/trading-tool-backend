import logging
import json
from datetime import datetime
from celery import shared_task

from backend.utils.db import get_db_connection
from backend.utils.openai_client import ask_gpt  # JSON-engine

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# ===================================================================
# ⛑️ Fallback strategie
# ===================================================================
def fallback_strategy(reason: str):
    return {
        "entry": "n.v.t.",
        "targets": [],
        "stop_loss": "n.v.t.",
        "risk_reward": "?",
        "explanation": f"AI-output kon niet worden geïnterpreteerd ({reason})"
    }


# ===================================================================
# 📡 AI insights ophalen
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
                SELECT category, avg_score, trend, bias, risk, summary
                FROM ai_category_insights
                WHERE date = CURRENT_DATE
            """)
            rows = cur.fetchall()

        for cat, avg_score, trend, bias, risk, summary in rows:
            insights[cat] = {
                "score": avg_score,
                "trend": trend,
                "bias": bias,
                "risk": risk,
                "summary": summary,
            }

        logger.info(f"📊 Loaded AI insights: {list(insights.keys())}")
        return insights

    except Exception as e:
        logger.error(f"❌ load_ai_insights fout: {e}", exc_info=True)
        return {}

    finally:
        conn.close()


# ===================================================================
# 🎯 Strategie genereren
# ===================================================================
def generate_strategy_from_setup(setup_dict: dict):

    if not isinstance(setup_dict, dict):
        return fallback_strategy("Ongeldige setup")

    setup = setup_dict
    setup_name = setup.get("name", "Onbekende setup")
    symbol = setup.get("symbol", "BTC")
    timeframe = setup.get("timeframe", "1D")

    logger.info(f"📄 Strategy Agent verwerkt setup: {setup_name} ({symbol} – {timeframe})")

    ai = load_ai_insights()

    master    = ai.get("master", {}) or {}
    macro_ai  = ai.get("macro", {}) or {}
    tech_ai   = ai.get("technical", {}) or {}
    market_ai = ai.get("market", {}) or {}

    # -------------------------------------------------------
    # AI Prompt
    # -------------------------------------------------------
    prompt = f"""
Je bent een professionele swingtrader.

Genereer een tradingstrategie uitsluitend in geldige JSON.

JSON structuur:
{{
  "entry": "range",
  "targets": ["t1","t2","t3"],
  "stop_loss": "prijs",
  "risk_reward": "1:3",
  "explanation": "1–3 korte zinnen"
}}

BELANGRIJK:
- Alleen pure JSON
"""

    response = ask_gpt(
        prompt,
        system_role="Je bent een professionele crypto trader. Antwoord altijd in geldige JSON."
    )

    if not isinstance(response, dict):
        logger.error("❌ AI gaf geen geldige JSON terug.")
        return fallback_strategy("Invalid JSON")

    entry       = response.get("entry", "n.v.t.")
    targets     = response.get("targets", [])
    stop_loss   = response.get("stop_loss", "n.v.t.")
    rr          = response.get("risk_reward", "?")
    explanation = response.get("explanation", "Geen uitleg")

    if isinstance(targets, str):
        targets = [t.strip() for t in targets.split(",") if t.strip()]

    strategy = {
        "entry": entry,
        "targets": targets,
        "stop_loss": stop_loss,
        "risk_reward": rr,
        "explanation": explanation,
    }

    logger.info(f"✅ Strategy resultaat: {json.dumps(strategy, ensure_ascii=False)[:200]}")
    return strategy


# ===================================================================
# 🕒 CELERY TASK — Strategy opslaan in strategies tabel (GEFIXT)
# ===================================================================
@shared_task(name="backend.ai_agents.strategy_ai_agent.generate_strategy_ai")
def generate_strategy_ai():
    logger.info("🧠 Start Strategy AI Agent...")

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Database fout.")
        return

    try:
        # ---------------------------
        # 1️⃣ Alle setups ophalen
        # ---------------------------
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, name, symbol, timeframe, trend
                FROM setups
                WHERE symbol = 'BTC'
            """)
            rows = cur.fetchall()

        setups = []
        for sid, name, symbol, timeframe, trend in rows:
            setups.append({
                "setup_id": sid,
                "name": name,
                "symbol": symbol,
                "timeframe": timeframe,
                "trend": trend,
                "indicators": []
            })

        if not setups:
            logger.warning("⚠️ Geen BTC setups.")
            return

        # ---------------------------
        # 2️⃣ Strategie per setup
        # ---------------------------
        with conn.cursor() as cur:
            for s in setups:
                strat = generate_strategy_from_setup(s)

                # Opslaan in echte kolommen
                cur.execute("""
                    INSERT INTO strategies
                        (setup_id, entry, target, stop_loss, explanation, risk_profile, strategy_type, data, created_at)
                    VALUES
                        (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                """, (
                    s["setup_id"],
                    strat["entry"],
                    ",".join(strat["targets"]),        # target → text
                    strat["stop_loss"],
                    strat["explanation"],
                    strat["risk_reward"],              # risk_profile kolom
                    "ai",
                    json.dumps(strat),                 # volledige JSON in data kolom
                ))

        conn.commit()
        logger.info("✅ Strategy AI Agent voltooid.")

    except Exception as e:
        logger.error(f"❌ Strategy AI fout: {e}", exc_info=True)

    finally:
        conn.close()
