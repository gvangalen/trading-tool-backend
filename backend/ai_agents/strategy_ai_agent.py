import logging
import json
from datetime import date

from backend.utils.db import get_db_connection
from backend.utils.openai_client import ask_gpt

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# ===================================================================
# 🎯 AI analyseert BESTAANDE strategieën (GEEN generatie)
# → Gedraagt zich IDENTIEK aan setup AI-uitleg
# ===================================================================
def analyze_strategies(strategies: list) -> dict | None:
    """
    Analyseert bestaande strategieën.
    Maakt GEEN nieuwe strategie.
    """

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
        logger.error("❌ Ongeldige JSON van AI bij strategy-analyse")
        return None

    return response


# ===================================================================
# 🧠 STRATEGY ADJUSTMENT — NIVAU 2 (VANDAAG-VERSIE)
# → Past strategy aan obv MARKTCONTEXT
# → Overschrijft NIETS
# ===================================================================
def adjust_strategy_for_today(
    base_strategy: dict,
    setup: dict,
    market_context: dict,
) -> dict | None:
    """
    Past een bestaande strategy subtiel aan voor vandaag.
    Setup blijft gelijk.
    """

    logger.info(
        f"🟡 Strategy adjustment voor setup={setup.get('id')} | date={date.today()}"
    )

    prompt = f"""
Je bent een professionele crypto trader.

Je krijgt:
1. Een BESTAANDE tradingstrategie
2. De huidige setup (blijft gelijk)
3. De actuele marktcontext

Je taak:
- PAS de strategie SUBTIEL aan voor vandaag
- GEEN nieuwe strategie maken
- Entry mag gelijk blijven of licht verfijnd
- Targets mogen verschuiven
- Stop-loss mag aangescherpt of verruimd worden

Bestaande strategy:
{json.dumps(base_strategy, indent=2)}

Setup:
{json.dumps(setup, indent=2)}

Marktcontext (vandaag):
{json.dumps(market_context, indent=2)}

Geef ALLEEN geldige JSON terug.

JSON format:
{{
  "entry": "",
  "targets": [],
  "stop_loss": "",
  "adjustment_reason": "",
  "confidence_score": 0,
  "changes": {{
    "entry": "unchanged | refined",
    "targets": "raised | lowered | unchanged",
    "stop_loss": "tightened | loosened | unchanged"
  }}
}}
"""

    result = ask_gpt(
        prompt,
        system_role="Je bent een professionele trading AI. Alleen geldige JSON."
    )

    if not isinstance(result, dict):
        logger.error("❌ Ongeldige JSON van AI bij strategy-adjustment")
        return None

    return result


# ===================================================================
# 🕒 CELERY TASK — STRATEGY AI ANALYSE (BESTAAND)
# 👉 POST /api/strategies/analyze/{strategy_id}
# ===================================================================
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
            return

        ai_text = analysis.get("recommendation")
        if not ai_text:
            return

        # ---------------------------------------------------
        # 3️⃣ Opslaan IN strategies.data.ai_explanation
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
        logger.info("✅ Strategy AI uitleg opgeslagen")

    except Exception as e:
        logger.error("❌ Strategy AI analyse fout", exc_info=True)

    finally:
        conn.close()


# ===================================================================
# 🚀 BESTAANDE STRATEGY GENERATION — ONGEWIJZIGD
# ===================================================================
def generate_strategy_from_setup(setup: dict, user_id: int) -> dict:
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
