from typing import Dict, List
from backend.engine.curve_engine import evaluate_curve


MIN_SCORE = 10.0
MAX_SCORE = 100.0


def calculate_score(
    indicator_values: Dict[str, float],
    curves: List[Dict],
) -> float:
    """
    Robust score engine.

    - Fail-safe
    - Clamped
    - Weighted-ready
    """

    scores = []
    weights = []

    for curve_row in curves:
        curve = curve_row.get("curve")
        input_key = curve.get("input")

        if input_key not in indicator_values:
            continue

        x = indicator_values[input_key]

        try:
            y = evaluate_curve(curve, x)
        except Exception:
            continue  # fail soft

        # Clamp score
        y = max(MIN_SCORE, min(float(y), MAX_SCORE))

        weight = float(curve_row.get("weight", 1.0))

        scores.append(y * weight)
        weights.append(weight)

    if not scores or not weights:
        return MIN_SCORE

    weighted_score = sum(scores) / sum(weights)

    return round(
        max(MIN_SCORE, min(weighted_score, MAX_SCORE)),
        2,
    )
