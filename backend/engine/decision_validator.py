from typing import Dict, List


class DecisionCurveError(Exception):
    pass


MIN_MULTIPLIER = 0.05
MAX_MULTIPLIER = 3.0


def validate_decision_curve(curve: Dict) -> None:
    """
    Valideert decision_curve JSON.

    Institutionele safeguards:
    - score coverage (0–100)
    - multiplier caps
    - oplopende x
    """

    if not isinstance(curve, dict):
        raise DecisionCurveError("Decision curve moet een object zijn")

    if "points" not in curve or not isinstance(curve["points"], list):
        raise DecisionCurveError("Decision curve vereist 'points' lijst")

    if len(curve["points"]) < 2:
        raise DecisionCurveError("Decision curve vereist minimaal 2 punten")

    points: List[Dict] = curve["points"]

    prev_x = None

    for p in points:
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

        if not 0 <= x <= 100:
            raise DecisionCurveError("x moet tussen 0 en 100 liggen")

        if not MIN_MULTIPLIER <= y <= MAX_MULTIPLIER:
            raise DecisionCurveError(
                f"Multiplier moet tussen {MIN_MULTIPLIER} en {MAX_MULTIPLIER} liggen"
            )

        if prev_x is not None and x <= prev_x:
            raise DecisionCurveError("x-waarden moeten strikt oplopend zijn")

        prev_x = x

    # 🔥 Coverage checks
    if points[0]["x"] != 0:
        raise DecisionCurveError("Curve moet beginnen bij x=0")

    if points[-1]["x"] != 100:
        raise DecisionCurveError("Curve moet eindigen bij x=100")
