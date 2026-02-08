# backend/engine/decision_validator.py

from typing import Dict, List


class DecisionCurveError(Exception):
    pass


def validate_decision_curve(curve: Dict) -> None:
    """
    Valideert decision_curve JSON.

    Vereist formaat:
    {
        "input": "market_score",
        "points": [
            {"x": 0, "y": 1.5},
            {"x": 50, "y": 1.0},
            {"x": 100, "y": 0.0}
        ]
    }
    """

    if not isinstance(curve, dict):
        raise DecisionCurveError("Decision curve moet een object zijn")

    if "points" not in curve or not isinstance(curve["points"], list):
        raise DecisionCurveError("Decision curve vereist 'points' lijst")

    if len(curve["points"]) < 2:
        raise DecisionCurveError("Decision curve vereist minimaal 2 punten")

    prev_x = None

    for p in curve["points"]:
        if not isinstance(p, dict):
            raise DecisionCurveError("Curve point moet object zijn")

        if "x" not in p or "y" not in p:
            raise DecisionCurveError("Elk point vereist x en y")

        x = p["x"]
        y = p["y"]

        if not isinstance(x, (int, float)):
            raise DecisionCurveError("x moet numeriek zijn")

        if not isinstance(y, (int, float)):
            raise DecisionCurveError("y moet numeriek zijn")

        if x < 0 or x > 100:
            raise DecisionCurveError("x moet tussen 0 en 100 liggen")

        if y < 0:
            raise DecisionCurveError("y mag niet negatief zijn")

        if prev_x is not None and x <= prev_x:
            raise DecisionCurveError("x-waarden moeten oplopend zijn")

        prev_x = x
