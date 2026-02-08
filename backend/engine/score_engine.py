from typing import Dict, List
from backend.engine.curve_engine import evaluate_curve


def calculate_score(
    indicator_values: Dict[str, float],
    curves: List[Dict],
) -> float:
    """
    indicator_values:
        {
          "rsi": 42,
          "trend_strength": 65,
          "volume_delta": 1.2
        }

    curves:
        lijst met curve records (uit DB)
    """

    scores = []

    for curve_row in curves:
        curve = curve_row["curve"]
        input_key = curve.get("input")

        if input_key not in indicator_values:
            continue

        x = indicator_values[input_key]
        y = evaluate_curve(curve, x)

        scores.append(float(y))

    if not scores:
        return 0.0

    # voorlopig: simpel gemiddelde
    return round(sum(scores) / len(scores), 2)
