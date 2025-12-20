import logging
import json
from datetime import date
from typing import Dict, List, Optional, Any

from backend.utils.openai_client import ask_gpt

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
    """

    logger.info(
        f"🟡 Strategy adjustment | setup={setup.get('id')} | date={date.today()}"
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

    # ✅ Verplichte velden check
    required_keys = {"entry", "targets", "stop_loss", "changes"}
    if not required_keys.issubset(result.keys()):
        logger.error("❌ Strategy-adjustment mist verplichte velden")
        return None

    # ✅ Confidence score afdwingen
    score = result.get("confidence_score")
    if not isinstance(score, (int, float)) or not (0 <= score <= 100):
        result["confidence_score"] = 50

    return result


# ===================================================================
# 🚀 INITIËLE STRATEGY GENERATIE (SETUP → STRATEGY)
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
