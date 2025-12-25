import logging
import json
from datetime import date
from typing import Dict, List, Optional, Any

from backend.utils.openai_client import ask_gpt
from backend.utils.db import get_db_connection

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# ===================================================================
# 🎯 AI analyseert BESTAANDE strategieën (GEEN generatie)
# ===================================================================

def analyze_strategies(strategies: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    Analyseert bestaande strategieën.
    Maakt GEEN nieuwe strategie.
    """

    prompt = f"""
Je bent een professionele trading-analist.

Analyseer deze bestaande tradingstrategie.
MAAK GEEN NIEUWE STRATEGIEËN.

Strategieën:
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

    if not {"comment", "recommendation"}.issubset(response.keys()):
        logger.error("❌ Strategy-analyse mist verplichte velden")
        return None

    return response


# ===================================================================
# 🟡 STRATEGY ADJUSTMENT — DAGELIJKSE BIJSTELLING
# ===================================================================

def adjust_strategy_for_today(
    base_strategy: Dict[str, Any],
    setup: Dict[str, Any],
    market_context: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """
    Past een bestaande strategy subtiel aan voor vandaag.
    Setup blijft gelijk.

    BELANGRIJK:
    - DCA heeft GEEN entry-trigger
    - Entry bij DCA = referentieprijs (context)
    """

    logger.info(
        f"🟡 Strategy adjustment | setup={setup.get('id')} | date={date.today()}"
    )

    strategy_type = (setup.get("strategy_type") or "").lower()

    prompt = f"""
Je bent een professionele crypto trader.

Je krijgt:
1. Een BESTAANDE tradingstrategie
2. De huidige setup (blijft gelijk)
3. De actuele marktcontext

BELANGRIJKE REGELS:
- Maak GEEN nieuwe strategie
- Houd setup gelijk
- Pas alleen details subtiel aan

SPECIFIEK VOOR DCA:
- DCA heeft GEEN vaste entry-trigger
- Gebruik "entry" als REFERENTIEPRIJS (bijv. huidige marktprijs)
- Entry is GEEN koop-signaal
- Geef altijd een korte uitleg waarom dit een referentie is

VOOR ANDERE STRATEGIEËN:
- Entry is een actieprijs
- Entry mag licht verfijnd worden

Bestaande strategy:
{json.dumps(base_strategy, indent=2)}

Setup:
{json.dumps(setup, indent=2)}

Marktcontext (vandaag):
{json.dumps(market_context, indent=2)}

Geef ALLEEN geldige JSON terug.

JSON format:
{{
  "entry": null | number | string,
  "entry_type": "reference" | "action",
  "targets": [],
  "stop_loss": null | number | string,
  "adjustment_reason": "",
  "confidence_score": 0,
  "changes": {{
    "entry": "unchanged | refined | reference",
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

    required_keys = {"entry", "targets", "stop_loss", "changes", "entry_type"}
    if not required_keys.issubset(result.keys()):
        logger.error("❌ Strategy-adjustment mist verplichte velden")
        return None

    score = result.get("confidence_score")
    if not isinstance(score, (int, float)) or not (0 <= score <= 100):
        result["confidence_score"] = 50

    if strategy_type == "dca":
        result["entry_type"] = "reference"
        if result.get("entry") in ("", None):
            result["changes"]["entry"] = "reference"
    else:
        result["entry_type"] = "action"

    return result


# ===================================================================
# 🚀 INITIËLE STRATEGY GENERATIE (SETUP → STRATEGY)
# ⚠️ Wordt alleen gebruikt door strategy_task (NIET door analyse)
# ===================================================================

def generate_strategy_from_setup(setup: Dict[str, Any]) -> Dict[str, Any]:
    """
    Genereert een initiële tradingstrategie op basis van een setup.
    """

    logger.info(f"⚙️ AI strategy generatie | setup={setup.get('id')}")

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
        raise ValueError("❌ AI strategy generatie gaf geen geldige JSON")

    required_keys = {"entry", "targets", "stop_loss"}
    if not required_keys.issubset(result.keys()):
        raise ValueError("❌ Strategy mist verplichte velden")

    return result


# ===================================================================
# 💾 OPSLAAN AI-UITLEG IN STRATEGY.DATA
# ===================================================================

def save_ai_explanation_to_strategy(
    strategy_id: int,
    ai_result: dict,
):
    explanation = (
        f"{ai_result.get('comment', '')}\n\n"
        f"{ai_result.get('recommendation', '')}"
    ).strip()

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE strategies
                SET data = jsonb_set(
                    data,
                    '{ai_explanation}',
                    %s::jsonb,
                    true
                )
                WHERE id = %s
            """, (
                json.dumps(explanation),
                strategy_id,
            ))
            conn.commit()
    finally:
        conn.close()


# ===================================================================
# 🧠 ORCHESTRATOR — ANALYSE → OPSLAG (DIT ONTBRAK)
# ===================================================================

def analyze_and_store_strategy(
    strategy_id: int,
    strategies: List[Dict[str, Any]],
):
    """
    1️⃣ Analyseert bestaande strategie(ën)
    2️⃣ Slaat AI-uitleg op in strategies.data.ai_explanation
    """

    logger.info(f"🧠 Strategy AI analyse gestart | strategy_id={strategy_id}")

    ai_result = analyze_strategies(strategies)

    if not ai_result:
        logger.error("❌ Geen AI-resultaat bij strategy-analyse")
        return None

    save_ai_explanation_to_strategy(
        strategy_id=strategy_id,
        ai_result=ai_result,
    )

    logger.info(f"✅ AI-uitleg opgeslagen voor strategy_id={strategy_id}")
    return ai_result
