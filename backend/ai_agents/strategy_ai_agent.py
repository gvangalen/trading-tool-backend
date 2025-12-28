import logging
import json
from datetime import date
from typing import Dict, List, Optional, Any

from backend.utils.db import get_db_connection
from backend.utils.openai_client import ask_gpt
from backend.ai_core.system_prompt_builder import build_system_prompt

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# ===================================================================
# 🎯 AI — ANALYSE VAN BESTAANDE STRATEGIEËN (GEEN GENERATIE)
# ===================================================================

def analyze_strategies(strategies: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    Analyseert bestaande strategieën.
    ❌ Maakt GEEN nieuwe strategie
    """

    TASK = """
Analyseer bestaande tradingstrategieën.

Regels:
- Maak GEEN nieuwe strategie
- Beoordeel kwaliteit, duidelijkheid en discipline
- Benoem risico’s en verbeterpunten
- Geen voorspellingen
- Geen nieuwe levels of entries

Doel:
- Korte evaluatie
- Praktische aanbeveling
"""

    system_prompt = build_system_prompt(
        agent="strategy",
        task=TASK
    )

    prompt = f"""
BESTAANDE STRATEGIEËN:
{json.dumps(strategies, ensure_ascii=False, indent=2)}

ANTWOORD ALLEEN GELDIGE JSON:
{{
  "comment": "",
  "recommendation": ""
}}
"""

    response = ask_gpt(prompt, system_role=system_prompt)

    if not isinstance(response, dict):
        logger.error("❌ Ongeldige JSON van AI bij strategy-analyse")
        return None

    if not {"comment", "recommendation"}.issubset(response.keys()):
        logger.error("❌ Strategy-analyse mist verplichte velden")
        return None

    return response


# ===================================================================
# 🟡 DAGELIJKSE STRATEGY-AANPASSING (DETAILS, GEEN NIEUWE STRATEGIE)
# ===================================================================

def adjust_strategy_for_today(
    base_strategy: Dict[str, Any],
    setup: Dict[str, Any],
    market_context: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """
    Past een bestaande strategie subtiel aan voor vandaag.
    Setup blijft ongewijzigd.
    """

    logger.info(
        f"🟡 Strategy adjustment | setup={setup.get('id')} | date={date.today()}"
    )

    strategy_type = (setup.get("strategy_type") or "").lower()

    TASK = """
Pas een BESTAANDE strategie licht aan op basis van actuele marktcontext.

Regels:
- Maak GEEN nieuwe strategie
- Houd setup ongewijzigd
- Wijzig alleen details (entry / targets / stop)
- Geen nieuwe concepten introduceren

Specifiek:
- DCA:
  - Entry = referentieprijs
  - GEEN trigger
  - Benoem expliciet dat het geen signaal is
- Andere strategieën:
  - Entry = actieprijs
  - Kleine verfijning toegestaan
"""

    system_prompt = build_system_prompt(
        agent="strategy",
        task=TASK
    )

    prompt = f"""
BESTAANDE STRATEGIE:
{json.dumps(base_strategy, ensure_ascii=False, indent=2)}

SETUP:
{json.dumps(setup, ensure_ascii=False, indent=2)}

MARKTCONTEXT VANDAAG:
{json.dumps(market_context, ensure_ascii=False, indent=2)}

ANTWOORD ALLEEN GELDIGE JSON:
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

    result = ask_gpt(prompt, system_role=system_prompt)

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

    # 🔒 DCA-fix afdwingen
    if strategy_type == "dca":
        result["entry_type"] = "reference"
        if result.get("entry") in ("", None):
            result["changes"]["entry"] = "reference"
    else:
        result["entry_type"] = "action"

    return result


# ===================================================================
# 🚀 INITIËLE STRATEGY GENERATIE (SETUP → STRATEGY)
# ⚠️ Alleen hier mag AI iets "maken"
# ===================================================================

def generate_strategy_from_setup(setup: Dict[str, Any]) -> Dict[str, Any]:
    """
    Genereert een initiële tradingstrategie op basis van een setup.
    """

    logger.info(f"⚙️ AI strategy generatie | setup={setup.get('id')}")

    TASK = """
Genereer een CONCRETE tradingstrategie op basis van een setup.

Regels:
- Wees concreet
- Geen educatie
- Geen hype
- Geen voorspellingen
- Focus op uitvoerbaarheid
"""

    system_prompt = build_system_prompt(
        agent="strategy",
        task=TASK
    )

    prompt = f"""
SETUP:
{json.dumps(setup, ensure_ascii=False, indent=2)}

ANTWOORD ALLEEN GELDIGE JSON:
{{
  "entry": "",
  "targets": [],
  "stop_loss": "",
  "risk_reward": "",
  "explanation": ""
}}
"""

    result = ask_gpt(prompt, system_role=system_prompt)

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
# 🧠 ORCHESTRATOR — ANALYSE → OPSLAG
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
