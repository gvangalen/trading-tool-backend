from typing import Dict, List, Optional


class CurveEngineError(Exception):
    pass


# =====================================================
# Helpers
# =====================================================

def _safe_float(value, fallback: Optional[float] = None) -> Optional[float]:
    try:
        if value is None:
            return fallback
        return float(value)
    except Exception:
        return fallback


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

    points_raw = curve.get("points")

    if not isinstance(points_raw, list) or len(points_raw) == 0:
        raise CurveEngineError("Curve bevat geen punten")

    points: List[Dict[str, float]] = []

    # -------------------------------------------------
    # Validate curve points
    # -------------------------------------------------
    for p in points_raw:
        x = _safe_float(p.get("x"))
        y = _safe_float(p.get("y"))

        if x is None or y is None:
            continue

        points.append({"x": x, "y": y})

    if not points:
        raise CurveEngineError("Geen geldige curve punten")

    points = sorted(points, key=lambda p: p["x"])

    x_value = _safe_float(x_value)

    if x_value is None:
        raise CurveEngineError("x_value ongeldig")

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
    base_amount = _safe_float(base_amount)

    if base_amount is None or base_amount <= 0:
        return 0.0

    # -------------------------------------------------
    # Validate score
    # -------------------------------------------------
    score = _safe_float(score)

    if score is None:
        score = 50.0

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

    multiplier = _safe_float(multiplier, 1.0)

    # -------------------------------------------------
    # Clamp multiplier
    # voorkomt user leverage mistakes
    # -------------------------------------------------
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
