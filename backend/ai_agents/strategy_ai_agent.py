import logging
import traceback
import json
from datetime import datetime

from backend.utils.db import get_db_connection
from backend.utils.openai_client import ask_gpt, ask_gpt_text

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# ============================================================
# 🤖 STRATEGY AI AGENT (universele architectuur)
# ============================================================
def generate_strategy_from_setup(setup_dict: dict):
    """
    Genereert een complete strategie (entry, targets, SL, uitleg)
    op basis van:
    - De gekozen setup
    - AI macro/market/technical insights
    - De AI Master Score
    """

    # -------------------------------------------------------
    # 1️⃣ Setup goed formatteren
    # -------------------------------------------------------
    if not isinstance(setup_dict, dict):
        logger.error("❌ Ongeldige setup meegegeven aan Strategy Agent.")
        return fallback_strategy("Setup niet leesbaar.")

    setup = setup_dict
    setup_name = setup.get("name", "Onbekende setup")
    symbol = setup.get("symbol", "BTC")
    timeframe = setup.get("timeframe", "1D")
    trend = setup.get("trend", "?")
    indicators = ", ".join(setup.get("indicators", [])) if isinstance(setup.get("indicators"), list) else "Geen"

    logger.info(f"📄 Strategy Agent verwerkt setup: {setup_name}")

    # -------------------------------------------------------
    # 2️⃣ AI Insights laden (macro, market, technical + master)
    # -------------------------------------------------------
    ai = load_ai_insights()
    master = ai.get("score", {})
    macro_ai = ai.get("macro", {})
    technical_ai = ai.get("technical", {})
    market_ai = ai.get("market", {})

    # -------------------------------------------------------
    # 3️⃣ Prompt opbouwen (universele style)
    # -------------------------------------------------------
    prompt = f"""
Je bent een professionele crypto trader en AI-strategie analist.

We hebben een setup en AI-insights. Bouw een strategie die ALTIJD in geldige JSON staat.

====================
📌 SETUP INFO
====================
Naam: {setup_name}
Asset: {symbol}
Timeframe: {timeframe}
Trend: {trend}
Indicatoren: {indicators}

====================
📊 AI MASTER SCORE
====================
Score: {master.get("score")}
Trend: {master.get("trend")}
Bias: {master.get("bias")}
Risico: {master.get("risk")}
Samenvatting: {master.get("summary")}

====================
🔎 AI INSIGHTS
====================
Macro: score={macro_ai.get("score")}, trend={macro_ai.get("trend")}, bias={macro_ai.get("bias")}
Market: score={market_ai.get("score")}, trend={market_ai.get("trend")}
Technical: score={technical_ai.get("score")}, trend={technical_ai.get("trend")}

====================
🎯 DOEL
====================
Maak een swingtrade strategie met:
- entry (1 niveau)
- targets (3 niveaus)
- stop_loss (1 niveau)
- risk_reward (ratio)
- explanation (kort, helder, NL)

De strategie moet in lijn liggen met:
1) AI Master Score
2) Macro / Technical / Market bias
3) Trend van de setup

ANTWOORD IN PURE JSON.
"""

    # -------------------------------------------------------
    # 4️⃣ GPT Request (universele helper)
    # -------------------------------------------------------
    ai_json = ask_gpt_text(prompt)

    try:
        parsed = json.loads(ai_json)
        if not isinstance(parsed, dict):
            raise ValueError("AI gaf geen dict terug.")
        return parsed

    except Exception as e:
        logger.error(f"❌ Strategy JSON parse fout: {e}")
        return fallback_strategy(ai_json)


# ------------------------------------------------------------
# 🔃 Meerdere strategieën genereren
# ------------------------------------------------------------
def generate_strategy_advice(setups: list):
    strategies = []

    if not isinstance(setups, list):
        logger.error("❌ Ongeldige setups-lijst.")
        return strategies

    for setup in setups:
        result = generate_strategy_from_setup(setup)
        strategies.append({
            "setup": setup.get("name"),
            "symbol": setup.get("symbol", "BTC"),
            "timeframe": setup.get("timeframe", "1D"),
            "strategy": result
        })

    return strategies


# ------------------------------------------------------------
# ⛑️ Fallback strategie als AI faalt
# ------------------------------------------------------------
def fallback_strategy(reason: str):
    return {
        "entry": "n.v.t.",
        "targets": [],
        "stop_loss": "n.v.t.",
        "risk_reward": "?",
        "explanation": f"AI-output kon niet worden geïnterpreteerd ({reason[:200]})"
    }


# ------------------------------------------------------------
# 📡 AI Insights ophalen uit DB (universele stijl)
# ------------------------------------------------------------
def load_ai_insights():
    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding bij load_ai_insights.")
        return {}

    insights = {}

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT category, avg_score, trend, bias, risk, summary
                FROM ai_category_insights
                WHERE date = CURRENT_DATE;
            """)
            rows = cur.fetchall()

        for r in rows:
            insights[r[0]] = {
                "score": r[1],
                "trend": r[2],
                "bias": r[3],
                "risk": r[4],
                "summary": r[5],
            }

        return insights

    except Exception as e:
        logger.error(f"❌ Fout bij load_ai_insights: {e}")
        return {}

    finally:
        conn.close()
