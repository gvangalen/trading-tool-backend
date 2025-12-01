import logging
import json
from datetime import datetime
from celery import shared_task

from backend.utils.db import get_db_connection
from backend.utils.openai_client import ask_gpt  # JSON-engine

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# ===================================================================
# ⛑️ Fallback strategie als AI-output niet correct is
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
# 📡 Laden van AI insights uit ai_category_insights (huidige dag)
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
# 🎯 Strategie genereren op basis van setup + AI insights
# ===================================================================
def generate_strategy_from_setup(setup_dict: dict):
    """
    Genereert een strategie (entry, targets, SL, uitleg)
    gebaseerd op:
    - Setup structuur
    - Macro / Technical / Market AI insights
    - Master score (bias / trend / risico)
    """

    if not isinstance(setup_dict, dict):
        logger.error("❌ Ongeldige setup meegegeven aan Strategy Agent.")
        return fallback_strategy("Setup niet leesbaar.")

    setup = setup_dict
    setup_name = setup.get("name", "Onbekende setup")
    symbol = setup.get("symbol", "BTC")
    timeframe = setup.get("timeframe", "1D")
    trend = setup.get("trend", "?")
    indicators = setup.get("indicators") or []

    if isinstance(indicators, str):
        indicators_str = indicators
    else:
        indicators_str = ", ".join(indicators)

    logger.info(f"📄 Strategy Agent verwerkt setup: {setup_name} ({symbol} – {timeframe})")

    # -------------------------------------------------------
    # AI-insights laden
    # -------------------------------------------------------
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

====================
📌 SETUP INFO
====================
Naam: {setup_name}
Asset: {symbol}
Timeframe: {timeframe}
Trend: {trend}
Indicatoren: {indicators_str}

====================
📊 MASTER SCORE
====================
Score: {master.get("score")}
Trend: {master.get("trend")}
Bias: {master.get("bias")}
Risico: {master.get("risk")}
Samenvatting: {master.get("summary")}

====================
🔎 CATEGORIE INSIGHTS
====================
Macro: score={macro_ai.get("score")}, trend={macro_ai.get("trend")}, bias={macro_ai.get("bias")}
Market: score={market_ai.get("score")}, trend={market_ai.get("trend")}
Technical: score={tech_ai.get("score")}, trend={tech_ai.get("trend")}

====================
🎯 DOEL
====================
Genereer een SWINGTRADE strategie die trend-gebaseerd is.

JSON structuur:
{{
  "entry": "prijs of range als string",
  "targets": ["t1", "t2", "t3"],
  "stop_loss": "prijs",
  "risk_reward": "1:3",
  "explanation": "max 3 zinnen"
}}

BELANGRIJK:
- Antwoord ALLEEN in geldige JSON
- Geen tekst buiten JSON
"""

    response = ask_gpt(
        prompt,
        system_role="Je bent een professionele crypto trader. Antwoord ALTIJD in pure JSON."
    )

    if not isinstance(response, dict):
        logger.error(f"❌ Geen geldige JSON: {response}")
        return fallback_strategy("Geen geldige JSON")

    entry       = response.get("entry", "n.v.t.")
    targets     = response.get("targets", [])
    stop_loss   = response.get("stop_loss", "n.v.t.")
    rr          = response.get("risk_reward", "?")
    explanation = response.get("explanation", "AI-strategie gegenereerd.")

    if isinstance(targets, str):
        targets = [x.strip() for x in targets.split(",")]

    strategy = {
        "entry": entry,
        "targets": targets,
        "stop_loss": stop_loss,
        "risk_reward": rr,
        "explanation": explanation,
    }

    logger.info(f"✅ Strategy resultaat: {json.dumps(strategy, ensure_ascii=False)[:250]}")
    return strategy


# ===================================================================
# 🧠 Bulk generator (voor Celery)
# ===================================================================
def generate_strategy_advice(setups: list):
    strategies = []
    if not isinstance(setups, list):
        return strategies

    for setup in setups:
        strat = generate_strategy_from_setup(setup)
        strategies.append({
            "setup": setup.get("name"),
            "symbol": setup.get("symbol", "BTC"),
            "timeframe": setup.get("timeframe", "1D"),
            "strategy": strat
        })

    return strategies


# ===================================================================
# 🕒 CELERY TASK — AI-strategieën genereren en opslaan
# ===================================================================
@shared_task(name="backend.ai_agents.strategy_ai_agent.generate_strategy_ai")
def generate_strategy_ai():
    """
    Pakt alle BTC setups → Genereert AI strategie → Slaat op in DB
    """
    logger.info("🧠 Start Strategy AI Agent...")

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Database fout.")
        return

    try:
        # --------------------------------------
        # 1️⃣ Alle setups ophalen
        # --------------------------------------
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
                "indicators": []  # setup-form definieert indicators optioneel
            })

        if not setups:
            logger.warning("⚠️ Geen BTC setups.")
            return

        # --------------------------------------
        # 2️⃣ Strategieën genereren
        # --------------------------------------
        strategies = []
        for s in setups:
            strat = generate_strategy_from_setup(s)
            strategies.append((s["setup_id"], strat))

        # --------------------------------------
        # 3️⃣ Opslaan
        # --------------------------------------
        with conn.cursor() as cur:
            for setup_id, strat in strategies:
                cur.execute("""
                    INSERT INTO strategies
                        (setup_id, date, entry, targets, stop_loss, rr, explanation)
                    VALUES
                        (%s, CURRENT_DATE, %s, %s, %s, %s, %s)
                    ON CONFLICT (setup_id, date) DO UPDATE SET
                        entry       = EXCLUDED.entry,
                        targets     = EXCLUDED.targets,
                        stop_loss   = EXCLUDED.stop_loss,
                        rr          = EXCLUDED.rr,
                        explanation = EXCLUDED.explanation,
                        created_at  = NOW()
                """, (
                    setup_id,
                    strat["entry"],
                    json.dumps(strat["targets"]),
                    strat["stop_loss"],
                    strat["risk_reward"],
                    strat["explanation"]
                ))

        conn.commit()
        logger.info("✅ Strategy AI Agent voltooid.")

    except Exception as e:
        logger.error(f"❌ Strategy AI fout: {e}", exc_info=True)

    finally:
        conn.close()
