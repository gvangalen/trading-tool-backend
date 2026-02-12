from typing import Dict, List, Optional

from backend.engine.curve_engine import evaluate_curve

# OPTIONAL (sterk aanbevolen)
try:
    from backend.engine.regime_weight_engine import apply_regime_weights
except Exception:
    apply_regime_weights = None


MIN_SCORE = 10.0
MAX_SCORE = 100.0

# voorkomt hysterische regime flips
MAX_SCORE_VELOCITY = 18.0

# voorkomt indicator dominance
MAX_WEIGHT_PER_FACTOR = 3.0


class ScoreEngineError(Exception):
    pass


# =====================================================
# Velocity Clamp
# =====================================================

def clamp_score_velocity(
    new_score: float,
    prev_score: Optional[float],
    max_move: float = MAX_SCORE_VELOCITY,
) -> float:
    """
    Voorkomt regime hysterie.
    Grote dagelijkse score jumps zijn meestal noise.
    """

    if prev_score is None:
        return new_score

    upper = prev_score + max_move
    lower = prev_score - max_move

    return max(lower, min(new_score, upper))


# =====================================================
# Main Score Engine
# =====================================================

def calculate_score(
    indicator_values: Dict[str, float],
    curves: List[Dict],
    *,
    prev_score: Optional[float] = None,
    regime_label: Optional[str] = None,
) -> float:
    """
    Institutional-grade score engine.

    Features:
    - Fail soft
    - Weight drift protection
    - Dominance caps
    - Velocity clamp
    - Regime-aware weighting (optional hook)
    """

    if not curves:
        return MIN_SCORE

    # -------------------------------------------------
    # Apply regime weighting (if engine exists)
    # -------------------------------------------------
    if regime_label and apply_regime_weights:
        try:
            curves = apply_regime_weights(curves, regime_label)
        except Exception:
            # fail soft — nooit crashen
            pass

    scores = []
    weights = []

    expected_weight = 0.0

    # -------------------------------------------------
    # Evaluate curves
    # -------------------------------------------------
    for curve_row in curves:

        curve = curve_row.get("curve")
        if not curve:
            continue

        input_key = curve.get("input")
        if input_key not in indicator_values:
            continue

        expected_weight += float(curve_row.get("weight", 1.0))

        x = indicator_values[input_key]

        try:
            y = evaluate_curve(curve, x)
        except Exception:
            continue  # fail soft

        # clamp score
        y = max(MIN_SCORE, min(float(y), MAX_SCORE))

        # dominance protection
        weight = float(curve_row.get("weight", 1.0))
        weight = min(weight, MAX_WEIGHT_PER_FACTOR)

        scores.append(y * weight)
        weights.append(weight)

    # -------------------------------------------------
    # No usable data
    # -------------------------------------------------
    if not scores or not weights:
        return MIN_SCORE

    actual_weight = sum(weights)

    if actual_weight <= 0:
        return MIN_SCORE

    # -------------------------------------------------
    # Weight drift protection
    # voorkomt dat 1 indicator ineens alles bepaalt
    # -------------------------------------------------
    coverage = 1.0

    if expected_weight > 0:
        coverage = actual_weight / expected_weight

        # harde ondergrens → data onvoldoende
        if coverage < 0.5:
            return MIN_SCORE

    # -------------------------------------------------
    # Weighted score
    # -------------------------------------------------
    raw_score = (sum(scores) / actual_weight) * coverage

    # clamp
    raw_score = max(MIN_SCORE, min(raw_score, MAX_SCORE))

    # -------------------------------------------------
    # Velocity clamp
    # -------------------------------------------------
    final_score = clamp_score_velocity(
        new_score=raw_score,
        prev_score=prev_score,
    )

    return round(
        max(MIN_SCORE, min(final_score, MAX_SCORE)),
        2,
    )
