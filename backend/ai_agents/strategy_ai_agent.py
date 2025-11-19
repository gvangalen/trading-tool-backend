import logging
import json
from datetime import datetime

from backend.utils.db import get_db_connection
from backend.utils.openai_client import ask_gpt  # ⬅️ JSON helper gebruiken

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

    raw_indicators = setup.get("indicators")
    if isinstance(raw_indicators, list):
        indicators_str = ", ".join(raw_indicators)
    elif isinstance(raw_indicators, str):
        indicators_str = raw_indicators
    else:
        indicators_str = "Geen"

    logger.info(f"📄 Strategy Agent verwerkt setup: {setup_name} ({symbol} – {timeframe})")

    # -------------------------------------------------------
    # 2️⃣ AI Insights laden (macro, market, technical + master)
    # -------------------------------------------------------
    ai = load_ai_insights()

    master = ai.get("score", {}) or ai.get("master", {}) or {}
    macro_ai = ai.get("macro", {}) or {}
    technical_ai = ai.get("technical", {}) or {}
    market_ai = ai.get("market", {}) or {}

    # -------------------------------------------------------
    # 3️⃣ Prompt opbouwen (universele style)
    # -------------------------------------------------------
    prompt = f"""
Je bent een professionele crypto trader en AI-strategie analist.

We hebben een setup en AI-insights. Bouw een **swingtrade strategie** die ALTIJD in geldige JSON staat.

====================
📌 SETUP INFO
====================
Naam: {setup_name}
Asset: {symbol}
Timeframe: {timeframe}
Trend: {trend}
Indicatoren: {indicators_str}

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
Maak een SWINGTRADE strategie die de richting van de trend volgt.

Structuur JSON-output:
{{
  "entry": "getal of range als string, bijv. '95000' of '95000-97000'",
  "targets": ["target1", "target2", "target3"],
  "stop_loss": "niveau, bijv. '91000'",
  "risk_reward": "ratio als string, bijv. '3R' of '1:3'",
  "explanation": "korte NL uitleg (2-4 zinnen) waarom deze strategie logisch is."
}}

BELANGRIJK:
- Gebruik **ALLEEN** deze velden.
- Geen extra tekst, geen uitleg buiten JSON.
- Antwoord in **pure JSON**.
"""

    # -------------------------------------------------------
    # 4️⃣ GPT Request via ask_gpt (JSON helper)
    # -------------------------------------------------------
    response = ask_gpt(
        prompt,
        system_role=(
            "Je bent een professionele crypto trader. "
            "Antwoord ALTIJD in geldige JSON, zonder extra tekst, markdown of uitleg."
        ),
    )

    # Als er een error-key in zit, fallback
    if not isinstance(response, dict):
        logger.error(f"❌ Strategy Agent kreeg geen dict terug: {response}")
        return fallback_strategy("AI gaf geen dict terug.")

    if "error" in response and not any(k in response for k in ("entry", "targets", "stop_loss")):
        logger.error(f"❌ Strategy Agent error: {response.get('error')}")
        return fallback_strategy(response.get("error", "Onbekende AI-fout"))

    # -------------------------------------------------------
    # 5️⃣ Normaliseer resultaat (altijd dezelfde velden)
    # -------------------------------------------------------
    entry = response.get("entry")
    targets = response.get("targets")
    stop_loss = response.get("stop_loss")
    rr = response.get("risk_reward") or response.get("rr") or response.get("rr_ratio")
    explanation = response.get("explanation") or response.get("summary")

    # Targets altijd lijst
    if isinstance(targets, str):
        # bv "95000, 98000, 100000"
        targets = [t.strip() for t in targets.split(",") if t.strip()]
    elif not isinstance(targets, list):
        targets = []

    # Defaults
    if not entry:
        entry = "n.v.t."
    if not stop_loss:
        stop_loss = "n.v.t."
    if not rr:
        rr = "?"
    if not explanation:
        explanation = "AI-strategie gegenereerd, maar zonder uitgebreide uitleg."

    strategy = {
        "entry": entry,
        "targets": targets,
        "stop_loss": stop_loss,
        "risk_reward": rr,
        "explanation": explanation,
    }

    logger.info(f"✅ Strategy Agent resultaat: {json.dumps(strategy, ensure_ascii=False)[:200]}")
    return strategy


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
        "explanation": f"AI-output kon niet worden geïnterpreteerd ({str(reason)[:200]})"
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
            category, avg_score, trend, bias, risk, summary = r
            insights[category] = {
                "score": avg_score,
                "trend": trend,
                "bias": bias,
                "risk": risk,
                "summary": summary,
            }

        logger.info(f"📊 AI Insights geladen voor categories: {list(insights.keys())}")
        return insights

    except Exception as e:
        logger.error(f"❌ Fout bij load_ai_insights: {e}")
        return {}

    finally:
        conn.close()
