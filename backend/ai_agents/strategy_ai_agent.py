import logging
import json
from datetime import date
from typing import Dict, List, Optional, Any

from backend.utils.db import get_db_connection
from backend.utils.openai_client import ask_gpt
from backend.ai_core.system_prompt_builder import build_system_prompt
from backend.ai_core.agent_context import build_agent_context

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# ===================================================================
# 🎯 AI — ANALYSE VAN BESTAANDE STRATEGIEËN (GEEN GENERATIE)
# ===================================================================

def analyze_strategies(
    *,
    user_id: int,
    strategies: List[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    """
    Analyseert bestaande strategieën.
    ❌ Maakt GEEN nieuwe strategie
    ✔ Gebruikt historisch besliscontext
    """

    agent_context = build_agent_context(
        user_id=user_id,
        category="strategy",
        current_score=None,
        current_items=strategies,
        lookback_days=3,  # strategie = trager geheugen
    )

    TASK = """
Je bent een senior trading coach.

Je krijgt:
- huidige strategieën
- eerdere AI-analyse(s) van strategiebeslissingen

Beoordeel:
- consistentie in besluitvorming
- discipline (entries, stops, targets)
- of aanpassingen logisch voortbouwen op eerdere keuzes

GEEN:
- nieuwe strategieën
- nieuwe levels
- marktvoorspellingen
- scoreberekeningen

OUTPUT — ALLEEN GELDIGE JSON:
{
  "comment": "",
  "recommendation": ""
}

REGELS:
- comment: 2–3 zinnen, evaluatief
- recommendation: concreet, uitvoerbaar, niet adviserend
"""

    system_prompt = build_system_prompt(
        agent="strategy",
        task=TASK
    )

    payload = {
        "context": agent_context,
        "strategies": strategies,
    }

    response = ask_gpt(
        prompt=json.dumps(payload, ensure_ascii=False, indent=2),
        system_role=system_prompt
    )

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
    *,
    user_id: int,
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

    agent_context = build_agent_context(
        user_id=user_id,
        category="strategy",
        current_score=None,
        current_items=[base_strategy],
        lookback_days=3,
    )

    TASK = """
Je past een BESTAANDE tradingstrategie licht aan.

Je krijgt:
- huidige strategie
- setup (onveranderlijk)
- marktcontext van vandaag
- context van eerdere strategy-aanpassingen

Gebruik expliciet:
- of deze aanpassing consistent is met eerdere beslissingen
- of dit een voortzetting, verzwakking of correctie is
- risico-discipline (stop/targets)

REGELS:
- Maak GEEN nieuwe strategie
- Introduceer GEEN nieuwe concepten
- Houd setup ongewijzigd

DCA-SPECIFIEK:
- Entry = referentieprijs
- Geen triggers
- Benoem expliciet dat dit geen signaal is

OUTPUT — ALLEEN GELDIGE JSON:
{
  "entry": null | number | string,
  "entry_type": "reference" | "action",
  "targets": [],
  "stop_loss": null | number | string,
  "adjustment_reason": "",
  "confidence_score": 0,
  "changes": {
    "entry": "unchanged | refined | reference",
    "targets": "raised | lowered | unchanged",
    "stop_loss": "tightened | loosened | unchanged"
  }
}
"""

    system_prompt = build_system_prompt(
        agent="strategy",
        task=TASK
    )

    payload = {
        "context": agent_context,
        "base_strategy": base_strategy,
        "setup": setup,
        "market_context": market_context,
    }

    result = ask_gpt(
        prompt=json.dumps(payload, ensure_ascii=False, indent=2),
        system_role=system_prompt
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

    # 🔒 DCA-regels afdwingen
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
    *,
    user_id: int,
    strategy_id: int,
    strategies: List[Dict[str, Any]],
):
    """
    1️⃣ Analyseert bestaande strategie(ën)
    2️⃣ Slaat AI-uitleg op in strategies.data.ai_explanation
    """

    logger.info(f"🧠 Strategy AI analyse gestart | strategy_id={strategy_id}")

    ai_result = analyze_strategies(
        user_id=user_id,
        strategies=strategies
    )

    if not ai_result:
        logger.error("❌ Geen AI-resultaat bij strategy-analyse")
        return None

    save_ai_explanation_to_strategy(
        strategy_id=strategy_id,
        ai_result=ai_result,
    )

    logger.info(f"✅ AI-uitleg opgeslagen voor strategy_id={strategy_id}")
    return ai_result
