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

def _get_latest_market_price(symbol: str = "BTC") -> float:
    conn = get_db_connection()
    if not conn:
        raise RuntimeError("Geen databaseverbinding")

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT price
                FROM market_data
                WHERE symbol = %s
                  AND price IS NOT NULL
                ORDER BY timestamp DESC
                LIMIT 1
                """,
                (symbol,),
            )
            row = cur.fetchone()

        if not row or row[0] is None:
            raise RuntimeError(f"Geen market price gevonden voor {symbol}")

        return float(row[0])

    finally:
        conn.close()


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

    logger.info(
        f"🟡 Strategy adjustment | setup={setup.get('id')} | date={date.today()}"
    )

    # 🔥 NIEUW MODEL
    setup_type = (setup.get("setup_type") or "").lower()
    is_dca = setup_type == "dca"
    is_trade = setup_type == "trade"

    # ======================================================
    # CONTEXT
    # ======================================================
    agent_context = build_agent_context(
        user_id=user_id,
        category="strategy",
        current_score=None,
        current_items=[base_strategy],
        lookback_days=3,
    )

    # ======================================================
    # AI TASK
    # ======================================================
    TASK = """
Je past een BESTAANDE tradingstrategie licht aan.

BELANGRIJK:
- GEEN score
- GEEN trade beslissing
- GEEN marktanalyse

Je mag:
- execution verbeteren
- discipline verbeteren
- consistentie bewaken

OUTPUT JSON:
{
  "entry": null,
  "entry_type": "reference",
  "targets": [],
  "stop_loss": null,
  "adjustment_reason": "",
  "changes": {
    "entry": "",
    "targets": "",
    "stop_loss": ""
  }
}
"""

    system_prompt = build_system_prompt(agent="strategy", task=TASK)

    payload = {
        "context": agent_context,
        "base_strategy": base_strategy,
        "setup": setup,
        "market_context": market_context,
    }

    result = ask_gpt(
        prompt=json.dumps(payload, ensure_ascii=False, indent=2),
        system_role=system_prompt,
    )

    if not isinstance(result, dict):
        return None

    required = {"entry", "targets", "stop_loss", "changes", "entry_type"}
    if not required.issubset(result.keys()):
        return None

    # 🔥 DCA FIX
    if is_dca:
        result["entry_type"] = "reference"
        result["entry"] = None
        result["stop_loss"] = None
        result["targets"] = []

    result.setdefault("adjustment_reason", "Execution unchanged")

    return result

# ======================================================
# Helper functie voor strategy score
# ======================================================
def fetch_strategy_score_for_today(
    *,
    conn,
    user_id: int,
) -> Optional[float]:
    """
    Strategy execution score komt UIT score agent (daily_scores).
    Strategy agent mag deze NOOIT zelf berekenen.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT strategy_score
            FROM daily_scores
            WHERE user_id = %s
              AND report_date = CURRENT_DATE
            LIMIT 1;
            """,
            (user_id,),
        )
        row = cur.fetchone()

    if not row or row[0] is None:
        return None

    try:
        return float(row[0])
    except Exception:
        return None


# ===================================================================
# 🚀 INITIËLE STRATEGY GENERATIE (SETUP → STRATEGY)
# ⚠️ Alleen hier mag AI iets "maken"
# ===================================================================
def generate_strategy_from_setup(setup: Dict[str, Any]) -> Dict[str, Any]:

    logger.info(f"⚙️ AI strategy generatie | setup={setup.get('id')}")

    setup_type = (setup.get("setup_type") or "").lower()
    symbol = setup.get("symbol", "BTC")

    # 🔥 DCA = GEEN LEVELS MAAR WEL BASE_AMOUNT VERPLICHT
    if setup_type == "dca":
        return {
            "entry": None,
            "targets": [],
            "stop_loss": None,
            "risk_reward": None,
            "base_amount": 50,  # of uit setup halen later
            "explanation": "DCA strategie — vaste accumulatie zonder vaste entry levels"
        }

    # -------------------------------------------------
    # MARKET PRICE
    # -------------------------------------------------
    try:
        live_price = float(_get_latest_market_price(symbol))
    except Exception as e:
        logger.error("❌ Geen market price beschikbaar: %s", e)
        raise

    # -------------------------------------------------
    # AI TASK
    # -------------------------------------------------
    TASK = """
Genereer trading levels.

Regels:
- entry rond current_price
- stop_loss onder entry
- targets boven entry

Output JSON:
{
  "entry": number,
  "targets": [number, number, number],
  "stop_loss": number,
  "risk_reward": number,
  "base_amount": number,
  "explanation": ""
}
"""

    system_prompt = build_system_prompt(agent="strategy", task=TASK)

    payload = {
        "setup": setup,
        "market_context": {
            "symbol": symbol,
            "current_price": live_price
        }
    }

    result = ask_gpt(
        prompt=json.dumps(payload, ensure_ascii=False, indent=2),
        system_role=system_prompt
    )

    # 🔥 HARD VALIDATIE (DIT IS WAT JE WILT)
    if not isinstance(result, dict):
        logger.error("❌ AI gaf geen dict terug: %s", result)
        raise RuntimeError("AI response invalid")

    def to_float(v):
        try:
            return float(v)
        except:
            return None

    entry = to_float(result.get("entry"))
    stop = to_float(result.get("stop_loss"))
    base_amount = to_float(result.get("base_amount"))

    targets = []
    for t in result.get("targets", []):
        tv = to_float(t)
        if tv is not None:
            targets.append(tv)

    # 🔥 GEEN FALLBACK → HARD FAIL + LOG
    if entry is None:
        logger.error("❌ Strategy mist entry: %s", result)
        raise RuntimeError("Strategy invalid: entry missing")

    if stop is None:
        logger.error("❌ Strategy mist stop_loss: %s", result)
        raise RuntimeError("Strategy invalid: stop_loss missing")

    if not targets:
        logger.error("❌ Strategy mist targets: %s", result)
        raise RuntimeError("Strategy invalid: targets missing")

    if base_amount is None:
        logger.error("❌ Strategy mist base_amount: %s", result)
        raise RuntimeError("Strategy invalid: base_amount missing")

    return {
        "entry": entry,
        "stop_loss": stop,
        "targets": targets,
        "risk_reward": result.get("risk_reward"),
        "base_amount": base_amount,
        "explanation": result.get("explanation", "")
    }

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
    today = date.today()

    conn = get_db_connection()
    if not conn:
        raise RuntimeError("Geen DB")

    try:
        setup_type = (setup.get("setup_type") or "").lower()

        # 🔥 FIX → DCA hoeft geen levels
        if setup_type == "dca":
            has_entry = True
            has_stop = True
            has_targets = True
        else:
            has_entry = base_strategy.get("entry") is not None
            has_stop = base_strategy.get("stop_loss") is not None
            has_targets = bool(base_strategy.get("targets"))

        # --------------------------------------------------
        # BOOTSTRAP
        # --------------------------------------------------
        if not has_entry or not has_stop or not has_targets:

            generated = generate_strategy_from_setup(setup)

            base_strategy["entry"] = generated.get("entry")
            base_strategy["stop_loss"] = generated.get("stop_loss")
            base_strategy["targets"] = generated.get("targets")

            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE strategies
                    SET
                        entry = %s,
                        stop_loss = %s,
                        targets = %s
                    WHERE id = %s
                """, (
                    base_strategy["entry"],
                    base_strategy["stop_loss"],
                    base_strategy["targets"],
                    strategy_id,
                ))

            conn.commit()

        # --------------------------------------------------
        # AI ANALYSE
        # --------------------------------------------------
        ai_result = analyze_strategies(
            user_id=user_id,
            strategies=strategies,
        )

        if ai_result:
            save_ai_explanation_to_strategy(
                strategy_id=strategy_id,
                ai_result=ai_result,
            )

        # --------------------------------------------------
        # ADJUSTMENT
        # --------------------------------------------------
        snapshot = adjust_strategy_for_today(
            user_id=user_id,
            base_strategy=base_strategy,
            setup=setup,
            market_context=market_context,
        ) or {}

        # 🔥 fallback naar base
        if snapshot.get("entry") is None:
            snapshot["entry"] = base_strategy.get("entry")

        if snapshot.get("stop_loss") is None:
            snapshot["stop_loss"] = base_strategy.get("stop_loss")

        if not snapshot.get("targets"):
            snapshot["targets"] = base_strategy.get("targets") or []

        # --------------------------------------------------
        # SCORE
        # --------------------------------------------------
        score = fetch_strategy_score_for_today(
            conn=conn,
            user_id=user_id,
        ) or 0.0

        snapshot["confidence_score"] = score

        # --------------------------------------------------
        # SAVE SNAPSHOT
        # --------------------------------------------------
        persist_active_strategy_snapshot(
            user_id=user_id,
            strategy_id=strategy_id,
            setup_id=setup["id"],
            snapshot_date=today,
            snapshot=snapshot,
        )

        return {
            "analysis": ai_result,
            "snapshot": snapshot,
        }

    finally:
        conn.close()

# ===================================================================
# 💾 ACTIVE STRATEGY SNAPSHOT (VERPLICHT VOOR BOT EXECUTION)
# ===================================================================
def persist_active_strategy_snapshot(
    *,
    user_id: int,
    strategy_id: int,
    setup_id: int,
    snapshot_date: date,
    snapshot: Dict[str, Any],
):
    """
    Slaat dagelijks het execution-plan op voor bots.

    Contract:
    - EXACT 1 snapshot per setup per dag
    - setup_id is verplicht (DB constraint)
    - targets wordt opgeslagen als TEXT
    """

    entry = snapshot.get("entry")
    stop_loss = snapshot.get("stop_loss")
    targets = snapshot.get("targets") or []

    try:
        entry = float(entry) if entry is not None else None
    except Exception:
        entry = None

    try:
        stop_loss = float(stop_loss) if stop_loss is not None else None
    except Exception:
        stop_loss = None

    clean_targets = []
    for t in targets:
        try:
            clean_targets.append(float(t))
        except Exception:
            continue

    targets_text = ",".join(str(t) for t in clean_targets) if clean_targets else None

    confidence_score = snapshot.get("confidence_score")
    try:
        confidence_score = float(confidence_score) if confidence_score is not None else 0.0
    except Exception:
        confidence_score = 0.0

    adjustment_reason = snapshot.get("adjustment_reason") or ""

    conn = get_db_connection()
    if not conn:
        raise RuntimeError("Geen databaseverbinding")

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO active_strategy_snapshot (
                    user_id,
                    setup_id,
                    strategy_id,
                    snapshot_date,
                    entry,
                    targets,
                    stop_loss,
                    adjustment_reason,
                    confidence_score,
                    created_at
                )
                VALUES (
                    %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    NOW()
                )
                ON CONFLICT (user_id, setup_id, snapshot_date)
                DO UPDATE SET
                    strategy_id        = EXCLUDED.strategy_id,
                    entry              = EXCLUDED.entry,
                    targets            = EXCLUDED.targets,
                    stop_loss          = EXCLUDED.stop_loss,
                    adjustment_reason  = EXCLUDED.adjustment_reason,
                    confidence_score   = EXCLUDED.confidence_score,
                    created_at         = NOW()
                """,
                (
                    user_id,
                    setup_id,
                    strategy_id,
                    snapshot_date,
                    entry,
                    targets_text,
                    stop_loss,
                    adjustment_reason,
                    confidence_score,
                ),
            )
        conn.commit()

        logger.info(
            "✅ Active strategy snapshot opgeslagen | user=%s setup=%s strategy=%s entry=%s stop=%s targets=%s",
            user_id,
            setup_id,
            strategy_id,
            entry,
            stop_loss,
            targets_text,
        )

    finally:
        conn.close()
