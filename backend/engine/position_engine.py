from typing import Dict, Any
from backend.engine.decision_engine import decide_amount


def calculate_position(
    *,
    setup: Dict[str, Any],
    scores: Dict[str, float],
    regime_memory: Dict[str, Any] | None,
    transition_risk: float | None,
) -> Dict[str, Any]:

    decision = decide_amount(
        setup=setup,
        scores=scores,
        regime_memory=regime_memory,
        transition_risk=transition_risk,
    )

    base_amount = float(decision.get("base_amount") or 0.0)
    final_amount = float(decision.get("final_amount") or 0.0)

    position_size = 0.0
    if base_amount > 0:
        position_size = round(final_amount / base_amount, 2)

    return {
        "base_amount": base_amount,
        "final_amount": final_amount,
        "position_size": position_size,
        "exposure_multiplier": decision.get("exposure_multiplier"),
        "raw": decision,
    }
