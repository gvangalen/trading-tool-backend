from typing import Dict, List, Optional


class DecisionEngineError(Exception):
    pass


def evaluate_curve(curve: Dict, x_value: float) -> float:
    """
    Lineaire interpolatie op basis van decision curve.

    Curve format:
    {
        "input": "market_score",
        "points": [
            {"x": 20, "y": 1.5},
            {"x": 40, "y": 1.2},
            {"x": 60, "y": 1.0},
            {"x": 80, "y": 0.5}
        ]
    }

    Returns:
        multiplier (float)
    """

    if not curve or "points" not in curve:
        raise DecisionEngineError("Decision curve ontbreekt of is ongeldig")

    points: List[Dict[str, float]] = sorted(
        curve["points"], key=lambda p: p["x"]
    )

    # Onder minimum
    if x_value <= points[0]["x"]:
        return float(points[0]["y"])

    # Boven maximum
    if x_value >= points[-1]["x"]:
        return float(points[-1]["y"])

    # Interpolatie
    for i in range(len(points) - 1):
        left = points[i]
        right = points[i + 1]

        if left["x"] <= x_value <= right["x"]:
            x0, y0 = left["x"], left["y"]
            x1, y1 = right["x"], right["y"]

            ratio = (x_value - x0) / (x1 - x0)
            return round(y0 + ratio * (y1 - y0), 4)

    # Fallback (zou nooit mogen gebeuren)
    return float(points[-1]["y"])


def decide_amount(
    setup: Dict,
    scores: Dict[str, float],
) -> float:
    """
    Bepaalt het investeringsbedrag voor een setup.

    Vereist setup-velden:
    - execution_mode: "fixed" | "custom"
    - base_amount: float
    - decision_curve (alleen bij custom)

    Vereist scores:
    - market_score (0–100)

    Returns:
        bedrag (float)
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

    multiplier = evaluate_curve(curve, score_value)

    amount = base_amount * multiplier
    return round(max(amount, 0.0), 2)
