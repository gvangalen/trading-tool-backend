from typing import Dict, List


class DecisionCurveError(Exception):
    pass


MIN_MULTIPLIER = 0.05
MAX_MULTIPLIER = 3.0


VALID_INPUTS = {
    "market_score",
    "technical_score",
    "macro_score",
    "setup_score",
}


def _safe_float(value):
    try:
        return float(value)
    except Exception:
        raise DecisionCurveError(f"Ongeldige numerieke waarde: {value}")


def validate_decision_curve(curve: Dict) -> None:
    """
    Valideert decision_curve JSON.

    Institutionele safeguards:
    - score coverage (0–100)
    - multiplier caps
    - oplopende x
    - geldige input field
    """

    if not isinstance(curve, dict):
        raise DecisionCurveError("Decision curve moet een object zijn")

    # -------------------------------------------------
    # Input field
    # -------------------------------------------------

    input_key = curve.get("input")

    if not input_key:
        raise DecisionCurveError("Decision curve vereist 'input'")

    if input_key not in VALID_INPUTS:
        raise DecisionCurveError(
            f"Input '{input_key}' ongeldig. Geldige inputs: {VALID_INPUTS}"
        )

    # -------------------------------------------------
    # Points validation
    # -------------------------------------------------

    points_raw = curve.get("points")

    if not isinstance(points_raw, list):
        raise DecisionCurveError("Decision curve vereist 'points' lijst")

    if len(points_raw) < 2:
        raise DecisionCurveError("Decision curve vereist minimaal 2 punten")

    points: List[Dict] = []

    for i, p in enumerate(points_raw):

        if not isinstance(p, dict):
            raise DecisionCurveError(f"Point {i} moet object zijn")

        if "x" not in p or "y" not in p:
            raise DecisionCurveError(f"Point {i} vereist x en y")

        x = _safe_float(p["x"])
        y = _safe_float(p["y"])

        if not 0 <= x <= 100:
            raise DecisionCurveError(
                f"x moet tussen 0 en 100 liggen (point {i})"
            )

        if not MIN_MULTIPLIER <= y <= MAX_MULTIPLIER:
            raise DecisionCurveError(
                f"Multiplier moet tussen {MIN_MULTIPLIER} en {MAX_MULTIPLIER} liggen"
            )

        points.append({"x": x, "y": y})

    # -------------------------------------------------
    # Sort points
    # -------------------------------------------------

    points.sort(key=lambda p: p["x"])

    prev_x = None

    for p in points:

        x = p["x"]

        if prev_x is not None and x <= prev_x:
            raise DecisionCurveError("x-waarden moeten strikt oplopend zijn")

        prev_x = x

    # -------------------------------------------------
    # Coverage checks
    # -------------------------------------------------

    if points[0]["x"] != 0:
        raise DecisionCurveError("Curve moet beginnen bij x=0")

    if points[-1]["x"] != 100:
        raise DecisionCurveError("Curve moet eindigen bij x=100")
