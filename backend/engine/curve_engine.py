from typing import Dict, List


class CurveEngineError(Exception):
    pass


def evaluate_curve(curve: Dict, x_value: float) -> float:
    """
    Lineaire interpolatie op basis van curve.

    curve = {
        "input": "rsi",
        "points": [
            {"x": 20, "y": 90},
            {"x": 30, "y": 70},
            {"x": 50, "y": 50},
            {"x": 70, "y": 30},
            {"x": 80, "y": 10}
        ]
    }
    """

    if not curve or "points" not in curve:
        raise CurveEngineError("Curve ontbreekt of is ongeldig")

    points: List[Dict[str, float]] = sorted(
        curve["points"], key=lambda p: p["x"]
    )

    if not points:
        raise CurveEngineError("Curve bevat geen punten")

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

    return float(points[-1]["y"])
