from typing import Dict

from backend.engine.curve_engine import calculate_position_size


class DecisionEngineError(Exception):
    pass


def decide_amount(
    setup: Dict,
    scores: Dict[str, float],
) -> float:
    """
    Delegates sizing naar de position sizing engine.
    Decision engine mag NOOIT zelf rekenen.
    """

    if not setup:
        raise DecisionEngineError("Setup ontbreekt")

    base_amount = setup.get("base_amount")
    execution_mode = setup.get("execution_mode", "fixed")

    if not isinstance(base_amount, (int, float)) or base_amount <= 0:
        raise DecisionEngineError("Ongeldig base_amount")

    # -------------------------------------------------
    # FIXED MODE
    # -------------------------------------------------
    if execution_mode == "fixed":
        return round(float(base_amount), 2)

    # -------------------------------------------------
    # CUSTOM MODE
    # -------------------------------------------------
    if execution_mode != "custom":
        raise DecisionEngineError(f"Onbekende execution_mode: {execution_mode}")

    curve = setup.get("decision_curve")
    if not curve:
        raise DecisionEngineError("Custom mode vereist decision_curve")

    input_key = curve.get("input", "market_score")
    score_value = scores.get(input_key)

    if not isinstance(score_value, (int, float)):
        raise DecisionEngineError(f"Score '{input_key}' ontbreekt of ongeldig")

    # 🔥 SINGLE SOURCE OF TRUTH
    return calculate_position_size(
        base_amount=base_amount,
        curve=curve,
        score=score_value,
    )
