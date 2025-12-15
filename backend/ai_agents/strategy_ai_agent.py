import logging
import json
from celery import shared_task

from backend.utils.db import get_db_connection
from backend.utils.openai_client import ask_gpt

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# ===================================================================
# 🎯 AI analyseert BESTAANDE strategieën (GEEN generatie)
# → Gedraagt zich IDENTIEK aan setup AI-uitleg
# ===================================================================
def analyze_strategies(strategies: list):
    prompt = f"""
Je bent een professionele trading-analist.

Analyseer deze bestaande tradingstrategie.
MAAK GEEN NIEUWE STRATEGIEËN.

Strategie:
{json.dumps(strategies, indent=2)}

Geef ALLEEN geldige JSON terug.

JSON format:
{{
  "comment": "Korte samenvatting van de kwaliteit",
  "recommendation": "Concreet en praktisch advies"
}}
"""

    response = ask_gpt(
        prompt,
        system_role="Je bent een crypto-strategie analist. Alleen geldige JSON."
    )

    if not isinstance(response, dict):
        logger.error("❌ Ongeldige JSON van AI")
        return None

    return response


# ===================================================================
# 🕒 CELERY TASK — STRATEGY AI ANALYSE
# 👉 POST /api/strategies/analyze/{strategy_id}
# ===================================================================
@shared_task(name="backend.ai_agents.strategy_ai_agent.analyze_strategy_ai")
def analyze_strategy_ai(strategy_id: int, user_id: int):
    logger.info(
        f"🧠 Start strategy AI analyse | strategy_id={strategy_id} user_id={user_id}"
    )

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen database connectie")
        return

    try:
        # ---------------------------------------------------
        # 1️⃣ Haal strategie op
        # ---------------------------------------------------
        with conn.cursor() as cur:
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
                WHERE id = %s AND user_id = %s
            """, (strategy_id, user_id))

            row = cur.fetchone()

        if not row:
            logger.error("❌ Strategie niet gevonden")
            return

        (
            sid,
            setup_id,
            entry,
            target,
            stop_loss,
            risk_profile,
            explanation,
            data,
            created_at
        ) = row

        strategy_payload = [{
            "strategy_id": sid,
            "setup_id": setup_id,
            "entry": entry,
            "target": target,
            "targets": data.get("targets") if isinstance(data, dict) else None,
            "stop_loss": stop_loss,
            "risk_profile": risk_profile,
            "explanation": explanation,
            "created_at": created_at.isoformat() if created_at else None,
        }]

        # ---------------------------------------------------
        # 2️⃣ AI analyse
        # ---------------------------------------------------
        analysis = analyze_strategies(strategy_payload)
        if not analysis:
            logger.error("❌ AI analyse mislukt")
            return

        ai_text = analysis.get("recommendation")
        if not ai_text:
            logger.error("❌ Geen AI recommendation ontvangen")
            return

        # ---------------------------------------------------
        # 3️⃣ Opslaan IN strategy.data (zoals setup)
        # ---------------------------------------------------
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE strategies
                SET data = jsonb_set(
                    COALESCE(data, '{}'::jsonb),
                    '{ai_explanation}',
                    to_jsonb(%s::text),
                    true
                )
                WHERE id = %s AND user_id = %s
            """, (
                ai_text,
                strategy_id,
                user_id
            ))

        conn.commit()
        logger.info("✅ Strategy AI uitleg opgeslagen in strategies.data.ai_explanation")

    except Exception as e:
        logger.error(f"❌ Strategy AI analyse fout: {e}", exc_info=True)

    finally:
        conn.close()


# ===================================================================
# 🚀 BESTAANDE STRATEGY GENERATION — ONGEWIJZIGD
# ===================================================================
def generate_strategy_from_setup(setup: dict, user_id: int):
    logger.info(
        f"⚙️ AI strategy generatie | setup={setup.get('id')} user={user_id}"
    )

    prompt = f"""
Je bent een professionele crypto trader.

Genereer een CONCRETE tradingstrategie op basis van deze setup.

Setup:
{json.dumps(setup, indent=2)}

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
