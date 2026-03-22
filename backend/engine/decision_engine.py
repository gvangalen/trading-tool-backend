from typing import Dict, Any

from backend.engine.curve_engine import calculate_position_size
from backend.engine.exposure_engine import (
    compute_exposure_multiplier,
    apply_exposure_to_amount,
)


class DecisionEngineError(Exception):
    pass


def decide_amount(
    setup: Dict[str, Any],
    scores: Dict[str, float],
    regime_memory: Dict[str, Any] | None = None,
    transition_risk: float | None = None,
) -> Dict[str, Any]:
    """
    Master allocation decision.

    Flow:
        1) Base sizing via curve_engine
        2) Setup conviction adjustment
        3) Exposure adjustment via exposure_engine
        4) Return structured result

    Decision engine rekent NOOIT zelf (alleen orkestratie).
    """

    if not setup:
        raise DecisionEngineError("Setup ontbreekt")

    base_amount = setup.get("base_amount")
    execution_mode = setup.get("execution_mode", "fixed")

    if not isinstance(base_amount, (int, float)) or base_amount <= 0:
        raise DecisionEngineError("Ongeldig base_amount")

    # =================================================
    # 1️⃣ Base Position Size
    # =================================================

    if execution_mode == "fixed":

        sized_amount = float(base_amount)

    elif execution_mode == "custom":

        curve = setup.get("decision_curve")
        if not curve:
            raise DecisionEngineError("Custom mode vereist decision_curve")

        input_key = curve.get("input", "market_score")
        score_value = scores.get(input_key)

        if not isinstance(score_value, (int, float)):
            raise DecisionEngineError(
                f"Score '{input_key}' ontbreekt of ongeldig"
            )

        sized_amount = calculate_position_size(
            base_amount=base_amount,
            curve=curve,
            score=score_value,
        )

    else:
        raise DecisionEngineError(
            f"Onbekende execution_mode: {execution_mode}"
        )

    # =================================================
    # 1.5️⃣ Setup Influence (Conviction Layer)
    # =================================================

    setup_score = scores.get("setup_score", scores.get("setup", 10))

    if isinstance(setup_score, (int, float)):
        if setup_score < 40:
            sized_amount *= 0.5
            setup_reason = "Weak setup → reduced size"
        elif setup_score > 70:
            sized_amount *= 1.2
            setup_reason = "Strong setup → increased size"
        else:
            setup_reason = "Neutral setup"
    else:
        setup_reason = "No setup score"

    # 🔥 FIX: clamp sized_amount (voorkomt extremes)
    sized_amount = max(0.0, min(float(sized_amount), float(base_amount) * 2))

    # =================================================
    # 2️⃣ Exposure Layer (Regime Risk Control)
    # =================================================

    exposure_data = compute_exposure_multiplier(
        regime_memory=regime_memory,
        transition_risk=transition_risk,
    )

    # 🔥 FIX: veilige multiplier
    multiplier = exposure_data.get("multiplier", 1.0)

    if not isinstance(multiplier, (int, float)):
        multiplier = 1.0

    # HARD SAFETY RANGE
    multiplier = max(0.0, min(multiplier, 2.0))

    final_amount = apply_exposure_to_amount(
        amount=sized_amount,
        exposure_multiplier=multiplier,
    )

    # 🔥 FIX: afronden
    final_amount = round(float(final_amount), 2)

    sized_amount = round(float(sized_amount), 2)

    # =================================================
    # 3️⃣ Structured Output (VERY IMPORTANT)
    # =================================================

    return {
        "base_amount": round(float(base_amount), 2),
        "sized_amount": sized_amount,
        "exposure_multiplier": multiplier,
        "final_amount": final_amount,
        "risk_mode": exposure_data.get("risk_mode"),
        "exposure_reason": exposure_data.get("reason"),
        "exposure_components": exposure_data.get("components"),
        "setup_reason": setup_reason,
    }
