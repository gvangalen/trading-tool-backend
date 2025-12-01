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
# 📡 AI insights ophalen (FIXED DATE FILTER)
# ===================================================================
def load_ai_insights():
    """
    Haalt recente AI-insights op uit ai_category_insights.

    🔧 Belangrijke fix:
    - NIET meer strikt WHERE date = CURRENT_DATE
    - Wel: laatste ~24–30 uur pakken, zodat timezone / runtimes geen probleem zijn
    """

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen database in load_ai_insights")
        return {}

    insights = {}

    try:
        with conn.cursor() as cur:
            # Pak alles van de afgelopen ~30 uur voor alle categorieën
            cur.execute("""
                SELECT category, avg_score, trend, bias, risk, summary, date, created_at
                FROM ai_category_insights
                WHERE date >= (CURRENT_DATE - INTERVAL '1 day')
                ORDER BY date DESC, created_at DESC;
            """)
            rows = cur.fetchall()

        if not rows:
            logger.warning("⚠️ load_ai_insights: geen recente ai_category_insights gevonden.")
            return {}

        for (cat, avg_score, trend, bias, risk, summary, d, created_at) in rows:
            # Alleen de eerste (meest recente) per categorie bewaren
            if cat in insights:
                continue

            insights[cat] = {
                "score": float(avg_score) if avg_score is not None else None,
                "trend": trend,
                "bias": bias,
                "risk": risk,
                "summary": summary,
                "date": d.isoformat() if hasattr(d, "isoformat") else str(d),
                "created_at": created_at.isoformat() if isinstance(created_at, datetime) else None,
            }

        logger.info(f"📊 Loaded AI insights voor categorieën: {list(insights.keys())}")
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

    # 1️⃣ Laad andere AI inzichten (macro / market / technical / setup / master / strategy)
    ai = load_ai_insights()

    # Eventueel later in de prompt gebruiken:
    # macro = ai.get("macro")
    # market = ai.get("market")
    # technical = ai.get("technical")
    # ...

    # 2️⃣ Prompt (nu nog basic; kan later uitgebreid worden met ai-context)
    prompt = f"""
Je bent een professionele swingtrader.

Genereer een tradingstrategie voor deze setup:
- Naam: {setup_name}
- Symbool: {symbol}
- Timeframe: {timeframe}

Gebruik ALLEEN geldige JSON:

{{
  "entry": "",
  "targets": ["t1","t2","t3"],
  "stop_loss": "",
  "risk_reward": "1:3",
  "explanation": "1–3 korte zinnen"
}}

BELANGRIJK:
- Alleen JSON als output.
- Geen tekst erbuiten.
"""

    response = ask_gpt(
        prompt,
        system_role="Je bent een professionele crypto trader. Antwoord ALTIJD in geldige JSON."
    )

    if not isinstance(response, dict):
        return fallback_strategy("Invalid JSON output")

    entry       = response.get("entry", "n.v.t.")
    targets     = response.get("targets", [])
    stop_loss   = response.get("stop_loss", "n.v.t.")
    rr          = response.get("risk_reward", "?")
    explanation = response.get("explanation", "Geen uitleg")

    if isinstance(targets, str):
        targets = [t.strip() for t in targets.split(",") if t.strip()]

    strat = {
        "entry": entry,
        "targets": targets,
        "stop_loss": stop_loss,
        "risk_reward": rr,
        "explanation": explanation,
    }

    logger.info(f"✅ Strategy resultaat: {json.dumps(strat)[:200]}")
    return strat


# ===================================================================
# 🕒 CELERY TASK — Strategy opslaan + AI SUMMARY OPSLAAN
# ===================================================================
@shared_task(name="backend.ai_agents.strategy_ai_agent.generate_strategy_ai")
def generate_strategy_ai():
    logger.info("🧠 Start Strategy AI Agent...")

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Database fout.")
        return

    try:
        # -----------------------------------------------------------------
        # 1️⃣ SETUPS ophalen
        # -----------------------------------------------------------------
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, name, symbol, timeframe, trend
                FROM setups
                WHERE symbol = 'BTC'
            """)
            rows = cur.fetchall()

        setups = [
            {
                "setup_id": sid,
                "name": name,
                "symbol": symbol,
                "timeframe": timeframe,
                "trend": trend,
            }
            for sid, name, symbol, timeframe, trend in rows
        ]

        if not setups:
            logger.warning("⚠️ Geen BTC setups.")
            return

        generated_strategies = []

        # -----------------------------------------------------------------
        # 2️⃣ GENEREREN PER SETUP
        # -----------------------------------------------------------------
        with conn.cursor() as cur:
            for s in setups:
                strat = generate_strategy_from_setup(s)

                generated_strategies.append({
                    "setup_id": s["setup_id"],
                    "name": s["name"],
                    "match_quality": 75,  # placeholder; AI kan dit later bepalen
                    "risk_reward": strat["risk_reward"],
                    "entry": strat["entry"],
                })

                cur.execute("""
                    INSERT INTO strategies
                        (setup_id, entry, target, stop_loss, explanation, risk_profile, strategy_type, data, created_at)
                    VALUES
                        (%s, %s, %s, %s, %s, %s, 'ai', %s, NOW())
                """, (
                    s["setup_id"],
                    strat["entry"],
                    ",".join(strat["targets"]),
                    strat["stop_loss"],
                    strat["explanation"],
                    strat["risk_reward"],
                    json.dumps(strat),
                ))

        conn.commit()
        logger.info("💾 Strategieën opgeslagen.")

        # -----------------------------------------------------------------
        # 3️⃣ AI SUMMARY OPSLAAN IN ai_category_insights (category = 'strategy')
        # -----------------------------------------------------------------
        if generated_strategies:
            # super simpele "score" uit risk_reward, placeholder
            avg_risk = sum(
                1 if s["risk_reward"] == "1:3" else 0
                for s in generated_strategies
            ) / len(generated_strategies)

            avg_score = round(50 + (avg_risk * 50), 2)
            trend = "Positief" if avg_score >= 60 else "Neutraal"
            bias = "Kansen" if avg_score >= 50 else "Afwachten"
            risk = "Gemiddeld" if avg_score >= 50 else "Hoog"

            summary = (
                f"Vandaag zijn {len(generated_strategies)} strategieën gegenereerd. "
                f"Gemiddelde strategy-score is {avg_score}/100. "
                f"Beste setup was '{generated_strategies[0]['name']}'."
            )

            top_signals = [
                {
                    "name": s["name"],
                    "risk_reward": s["risk_reward"],
                    "entry": s["entry"],
                }
                for s in generated_strategies[:3]
            ]

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
                    avg_score,
                    trend,
                    bias,
                    risk,
                    summary,
                    json.dumps(top_signals),
                ))

            conn.commit()
            logger.info("📊 Strategy AI-insight opgeslagen in ai_category_insights.")

        logger.info("✅ Strategy AI Agent volledig voltooid.")

    except Exception as e:
        logger.error(f"❌ Strategy AI fout: {e}", exc_info=True)

    finally:
        conn.close()
