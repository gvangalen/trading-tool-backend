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
    Analyseert BESTAANDE strategieën vanuit execution-perspectief.

    ❌ GEEN nieuwe strategie
    ❌ GEEN nieuwe setup
    ❌ GEEN marktvoorspellingen

    ✔ Optimaliseert execution-details
    ✔ Gebruikt historisch besliscontext
    ✔ Voorkomt herhaling van eerdere fouten
    """

    # ======================================================
    # 🧠 HISTORISCHE STRATEGY CONTEXT
    # ======================================================
    agent_context = build_agent_context(
        user_id=user_id,
        category="strategy",
        current_score=None,
        current_items=strategies,
        lookback_days=3,  # strategie = trager geheugen
    )

    # ======================================================
    # 🎯 STRATEGY EXECUTION TASK
    # ======================================================
    TASK = """
Je bent een senior execution strategist.

Doel:
- Optimaliseer een BESTAANDE tradingstrategie voor de huidige marktcontext.
- De kernstrategie blijft ongewijzigd.

Je krijgt:
- huidige strategieën
- historische AI-inzichten over eerdere strategiebeslissingen
- execution-patronen uit recente dagen

Regels:
- GEEN nieuwe strategie
- GEEN nieuwe setup
- GEEN nieuwe entries, targets of levels
- GEEN marktvoorspellingen
- GEEN scoreberekeningen

Je mag:
- execution-logica aanscherpen
- inconsistenties benoemen
- aangeven waar discipline ontbreekt
- aangeven of huidige aanpassingen logisch voortbouwen op eerdere keuzes

OUTPUT — ALLEEN GELDIGE JSON:
{
  "comment": "",
  "recommendation": ""
}

REGELS:
- comment:
  - 2–3 zinnen
  - evaluatief
  - gericht op consistentie & discipline

- recommendation:
  - concreet en uitvoerbaar
  - procesmatig (timing, volgorde, rust, bevestiging)
  - GEEN trade-actie
  - GEEN marktadvies
"""

    system_prompt = build_system_prompt(
        agent="strategy",
        task=TASK
    )

    # ======================================================
    # 📦 AI PAYLOAD
    # ======================================================
    payload = {
        "context": agent_context,
        "strategies": strategies,
    }

    response = ask_gpt(
        prompt=json.dumps(payload, ensure_ascii=False, indent=2),
        system_role=system_prompt
    )

    # ======================================================
    # 🧪 VALIDATIE
    # ======================================================
    if not isinstance(response, dict):
        logger.error("❌ Ongeldige JSON van AI bij strategy-execution-analyse")
        return None

    if not {"comment", "recommendation"}.issubset(response.keys()):
        logger.error("❌ Strategy-execution-analyse mist verplichte velden")
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

    BELANGRIJK:
    - Deze functie MOET elke dag een geldige snapshot opleveren
    - Snapshot is required voor bot-agent om te mogen traden
    """

    logger.info(
        f"🟡 Strategy adjustment | setup={setup.get('id')} | date={date.today()}"
    )

    strategy_type = (setup.get("strategy_type") or "").lower()

    # ======================================================
    # 🧠 Historische context
    # ======================================================
    agent_context = build_agent_context(
        user_id=user_id,
        category="strategy",
        current_score=None,
        current_items=[base_strategy],
        lookback_days=3,
    )

    # ======================================================
    # 🎯 AI TASK
    # ======================================================
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

    # ======================================================
    # 🤖 AI CALL
    # ======================================================
    result = ask_gpt(
        prompt=json.dumps(payload, ensure_ascii=False, indent=2),
        system_role=system_prompt
    )

    if not isinstance(result, dict):
        logger.error("❌ Ongeldige JSON van AI bij strategy-adjustment")
        return None

    # ======================================================
    # 🧪 VALIDATIE
    # ======================================================
    required_keys = {"entry", "targets", "stop_loss", "changes", "entry_type"}
    if not required_keys.issubset(result.keys()):
        logger.error("❌ Strategy-adjustment mist verplichte velden")
        return None

    # ======================================================
    # 🔢 Confidence normaliseren
    # ======================================================
    score = result.get("confidence_score")
    if not isinstance(score, (int, float)) or not (0 <= score <= 100):
        result["confidence_score"] = 50

    # ======================================================
    # 🔒 DCA-regels afdwingen
    # ======================================================
    if strategy_type == "dca":
        result["entry_type"] = "reference"
        if result.get("entry") in ("", None):
            result["changes"]["entry"] = "reference"
    else:
        result["entry_type"] = "action"

    # ======================================================
    # 🔒 SNAPSHOT CONTRACT (CRUCIAAL)
    # → voorkomt no_snapshot bij bot-agent
    # ======================================================
    result.setdefault("entry", None)
    result.setdefault("targets", [])
    result.setdefault("stop_loss", None)
    result.setdefault("confidence_score", 0)
    result.setdefault(
        "adjustment_reason",
        "No explicit execution plan for today"
    )

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
# 🧠 ORCHESTRATOR — ANALYSE → SNAPSHOT → OPSLAG
# ===================================================================

def analyze_and_store_strategy(
    *,
    user_id: int,
    strategy_id: int,
    strategies: List[Dict[str, Any]],
    base_strategy: Dict[str, Any],
    setup: Dict[str, Any],
    market_context: Dict[str, Any],
):
    """
    DAGELIJKSE STRATEGY RUN

    1️⃣ Analyse (AI reflection)
    2️⃣ Strategy adjustment voor vandaag
    3️⃣ 🔥 Snapshot opslaan (VERPLICHT)
    """

    today = date.today()
    logger.info(f"🧠 Strategy AI dagrun | strategy_id={strategy_id} | {today}")

    # 1️⃣ Analyse (coach / reflectie)
    ai_result = analyze_strategies(
        user_id=user_id,
        strategies=strategies
    )

    if ai_result:
        save_ai_explanation_to_strategy(
            strategy_id=strategy_id,
            ai_result=ai_result,
        )

    # 2️⃣ DAGELIJKSE STRATEGY SNAPSHOT (ALTijd)
    snapshot = adjust_strategy_for_today(
        user_id=user_id,
        base_strategy=base_strategy,
        setup=setup,
        market_context=market_context,
    )

    if not snapshot:
        # ⛑️ FAILSAFE — lege snapshot
        snapshot = {
            "entry": None,
            "targets": [],
            "stop_loss": None,
            "confidence_score": 0,
            "adjustment_reason": "Strategy agent failed to produce snapshot",
        }

    # 3️⃣ 🔥 OPSLAAN — DIT IS WAAR DE BOT OP DRAAIT
    persist_active_strategy_snapshot(
        user_id=user_id,
        strategy_id=strategy_id,
        snapshot_date=today,
        snapshot=snapshot,
    )

    logger.info(f"✅ Active strategy snapshot opgeslagen | strategy_id={strategy_id}")

    return {
        "analysis": ai_result,
        "snapshot": snapshot,
    }


# ===================================================================
# 💾 ACTIVE STRATEGY SNAPSHOT (VERPLICHT VOOR BOT EXECUTION)
# ===================================================================

def persist_active_strategy_snapshot(
    *,
    user_id: int,
    strategy_id: int,
    snapshot_date: date,
    snapshot: Dict[str, Any],
):
    """
    Slaat DAGELIJKS het execution-plan op voor bots.

    Contract:
    - EXACT 1 snapshot per strategy per dag
    - Mag lege velden bevatten
    - Bot-agent is hier volledig afhankelijk van
    """

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO active_strategy_snapshot (
                    user_id,
                    strategy_id,
                    snapshot_date,
                    entry,
                    targets,
                    stop_loss,
                    confidence_score,
                    adjustment_reason,
                    created_at,
                    updated_at
                )
                VALUES (
                    %s,%s,%s,
                    %s,%s,%s,
                    %s,%s,
                    NOW(), NOW()
                )
                ON CONFLICT (user_id, strategy_id, snapshot_date)
                DO UPDATE SET
                    entry              = EXCLUDED.entry,
                    targets            = EXCLUDED.targets,
                    stop_loss          = EXCLUDED.stop_loss,
                    confidence_score   = EXCLUDED.confidence_score,
                    adjustment_reason  = EXCLUDED.adjustment_reason,
                    updated_at         = NOW()
                """,
                (
                    user_id,
                    strategy_id,
                    snapshot_date,
                    snapshot.get("entry"),
                    json.dumps(snapshot.get("targets") or []),
                    snapshot.get("stop_loss"),
                    float(snapshot.get("confidence_score") or 0),
                    snapshot.get("adjustment_reason", ""),
                ),
            )
            conn.commit()
    finally:
        conn.close()
