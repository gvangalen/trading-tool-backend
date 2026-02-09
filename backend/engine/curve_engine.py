from typing import Dict, List


class CurveEngineError(Exception):
    pass


# =====================================================
# 📈 Curve evaluation (multiplier lookup)
# =====================================================
def evaluate_curve(curve: Dict, x_value: float) -> float:
    """
    Lineaire interpolatie op basis van curve.

    Verwacht multiplier values (bijv 0.5 → 2.0)

    curve = {
        "input": "score",
        "points": [
            {"x": 20, "y": 1.5},
            {"x": 40, "y": 1.2},
            {"x": 60, "y": 1.0},
            {"x": 80, "y": 0.5}
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

    try:
        x_value = float(x_value)
    except Exception:
        raise CurveEngineError("x_value is geen getal")

    # -------------------------------------------------
    # Onder minimum → clamp
    # -------------------------------------------------
    if x_value <= points[0]["x"]:
        return float(points[0]["y"])

    # -------------------------------------------------
    # Boven maximum → clamp
    # -------------------------------------------------
    if x_value >= points[-1]["x"]:
        return float(points[-1]["y"])

    # -------------------------------------------------
    # Interpolatie
    # -------------------------------------------------
    for i in range(len(points) - 1):
        left = points[i]
        right = points[i + 1]

        if left["x"] <= x_value <= right["x"]:
            x0, y0 = left["x"], left["y"]
            x1, y1 = right["x"], right["y"]

            # 🔥 protect against divide-by-zero
            if x1 == x0:
                return float(y0)

            ratio = (x_value - x0) / (x1 - x0)
            interpolated = y0 + ratio * (y1 - y0)

            return round(float(interpolated), 4)

    return float(points[-1]["y"])


# =====================================================
# 💰 POSITION SIZING ENGINE
# =====================================================
def calculate_position_size(
    base_amount: float,
    curve: Dict,
    score: float,
    *,
    min_multiplier: float = 0.1,
    max_multiplier: float = 3.0,
) -> float:
    """
    Single source of truth voor trade sizing.

    size = base_amount * multiplier

    Veiligheden:
    - curve failure → fallback multiplier = 1
    - multiplier clamp
    - negatieve sizing onmogelijk
    - bot kan NOOIT crashen
    """

    # -------------------------------------------------
    # Validate base amount
    # -------------------------------------------------
    try:
        base_amount = float(base_amount)
    except Exception:
        return 0.0

    if base_amount <= 0:
        return 0.0

    # -------------------------------------------------
    # Resolve multiplier
    # -------------------------------------------------
    multiplier = 1.0

    try:
        multiplier = evaluate_curve(curve, score)
    except CurveEngineError:
        multiplier = 1.0
    except Exception:
        multiplier = 1.0

    # -------------------------------------------------
    # Clamp multiplier (🔥 VERY IMPORTANT)
    # voorkomt user leverage mistakes
    # -------------------------------------------------
    try:
        multiplier = float(multiplier)
    except Exception:
        multiplier = 1.0

    multiplier = max(min_multiplier, min(multiplier, max_multiplier))

    # -------------------------------------------------
    # Final size
    # -------------------------------------------------
    try:
        position_size = base_amount * multiplier

        if position_size <= 0:
            return 0.0

        return round(position_size, 2)

    except Exception:
        return 0.0
