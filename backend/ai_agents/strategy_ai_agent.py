import logging
import json
import os
from dotenv import load_dotenv
from openai import OpenAI, OpenAIError

from backend.utils.db import get_db_connection

# -----------------------------------------------------
# 🔧 Setup
# -----------------------------------------------------
load_dotenv()
DEFAULT_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# =====================================================
# 🧰 Helper: veilige JSON/dict conversie
# =====================================================
def ensure_dict(obj, context=""):
    if isinstance(obj, dict):
        return obj
    if isinstance(obj, str):
        try:
            parsed = json.loads(obj)
            if isinstance(parsed, dict):
                return parsed
            logger.warning(f"⚠️ {context}: JSON geladen maar is geen dict.")
            return {}
        except json.JSONDecodeError:
            logger.error(f"❌ {context}: Kan string niet parsen als JSON:\n{obj}")
            return {}
    logger.warning(f"⚠️ {context}: Ongeldig type ({type(obj)}), verwacht dict of str.")
    return {}


# =====================================================
# 📡 Haal AI-insights op (macro/market/technical/score)
# =====================================================
def load_ai_insights():
    conn = get_db_connection()
    if not conn:
        logger.warning("❌ Geen DB-verbinding voor AI insights.")
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
                cat = r[0]
                insights[cat] = {
                    "score": float(r[1] or 0),
                    "trend": r[2],
                    "bias": r[3],
                    "risk": r[4],
                    "summary": r[5],
                }

        return insights

    except Exception as e:
        logger.error(f"❌ Fout bij ophalen ai_insights: {e}")
        return {}
    finally:
        conn.close()


# =====================================================
# 🧠 Strategie genereren via AI (NIEUWE AI-ARCHITECTUUR)
# =====================================================
def generate_strategy_from_setup(setup: dict | str, model: str = DEFAULT_MODEL) -> dict:
    setup = ensure_dict(setup, context="generate_strategy_from_setup")

    # ---------------------------------------
    # 🔥 1. AI-insights laden
    # ---------------------------------------
    ai_insights = load_ai_insights()
    master = ai_insights.get("score", {})

    setup_name = setup.get("name", "Onbekende setup")
    timeframe = setup.get("timeframe", "1D")
    symbol = setup.get("symbol", "BTC")
    trend = setup.get("trend", "?")
    indicators = (
        ", ".join(setup.get("indicators", []))
        if isinstance(setup.get("indicators"), list)
        else "Geen"
    )

    macro_ai = ai_insights.get("macro", {})
    technical_ai = ai_insights.get("technical", {})
    market_ai = ai_insights.get("market", {})

    # ---------------------------------------
    # 🧠 2. Verbeterde prompt
    # ---------------------------------------
    prompt = f"""
Je bent een professionele crypto trader en AI-strategie analist.

Genereer een strategie op basis van:

=== SETUP INFO ===
Naam: {setup_name}
Asset: {symbol}
Timeframe: {timeframe}
Trend: {trend}
Indicatoren: {indicators}

=== AI MASTER SCORE ===
Score: {master.get("score")}
Trend: {master.get("trend")}
Bias: {master.get("bias")}
Risico: {master.get("risk")}
Samenvatting: {master.get("summary")}

=== AI FACTOR INSIGHTS ===
Macro: score={macro_ai.get("score")}, trend={macro_ai.get("trend")}, bias={macro_ai.get("bias")}
Market: score={market_ai.get("score")}, trend={market_ai.get("trend")}
Technical: score={technical_ai.get("score")}, trend={technical_ai.get("trend")}

Gebruik ALLES hierboven.

🎯 DOEL:
Geef een swingtrade-strategie die:
- In lijn ligt met AI Master Score (bias & trend)
- De sterke/zwakke factoren benadrukt
- Duidelijk uitlegt *waarom*

Antwoord ALTIJD in geldige JSON met deze velden:
entry, targets, stop_loss, risk_reward, explanation
"""

    logger.info(f"🧠 Strategie-prompt voor '{setup_name}' gegenereerd.")

    # ---------------------------------------
    # 🧠 3. OpenAI request
    # ---------------------------------------
    try:
        response = client.chat.completions.create(
            model=model,
            messages=[
                {
                    "role": "system",
                    "content": "Je bent een ervaren crypto trader. Antwoord ALTIJD in geldige JSON."
                },
                {"role": "user", "content": prompt},
            ],
            temperature=0.55,
        )

        raw = response.choices[0].message.content
        logger.info(f"AI strategy output: {raw[:180]}...")

        result = json.loads(raw)
        if not isinstance(result, dict):
            raise ValueError("AI gaf geen JSON-dict terug")

        return result

    except Exception as e:
        logger.error(f"❌ Fout bij strategie-generatie: {e}")
        return {
            "entry": "n.v.t.",
            "targets": [],
            "stop_loss": "n.v.t.",
            "risk_reward": "?",
            "explanation": "AI-output kon niet worden geïnterpreteerd."
        }


# =====================================================
# 📊 Strategie-advies voor meerdere setups
# =====================================================
def generate_strategy_advice(setups):
    """
    AI-strategieën genereren voor ALLE actieve setups
    (scores worden uit AI-insights gehaald, niet meer meegegeven).
    """
    strategies = []

    if not isinstance(setups, list):
        logger.error("❌ Invalid setups list.")
        return strategies

    for setup in setups:
        setup = ensure_dict(setup, context="generate_strategy_advice")
        strategy = generate_strategy_from_setup(setup)

        strategies.append({
            "setup_name": setup.get("name"),
            "symbol": setup.get("symbol", "BTC"),
            "timeframe": setup.get("timeframe", "1D"),
            "trend": setup.get("trend", "?"),
            "strategy": strategy,
        })

    return strategies
