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
# Helpers
# =====================================================

def _safe_float(value):
    try:
        return float(value)
    except Exception:
        return None


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
    # Apply regime weighting (optional)
    # -------------------------------------------------

    if regime_label and apply_regime_weights:

        try:
            curves = apply_regime_weights(curves, regime_label)
        except Exception:
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

        x = _safe_float(indicator_values[input_key])

        if x is None:
            continue

        weight = _safe_float(curve_row.get("weight", 1.0))

        if weight is None:
            weight = 1.0

        expected_weight += weight

        try:
            y = evaluate_curve(curve, x)
        except Exception:
            continue

        y = _safe_float(y)

        if y is None:
            continue

        # clamp score
        y = max(MIN_SCORE, min(y, MAX_SCORE))

        # dominance protection
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
    # -------------------------------------------------

    coverage = 1.0

    if expected_weight > 0:

        coverage = actual_weight / expected_weight

        # extreem lage coverage → fallback
        if coverage < 0.35:
            return MIN_SCORE

    # -------------------------------------------------
    # Weighted score
    # -------------------------------------------------

    raw_score = (sum(scores) / actual_weight) * coverage

    raw_score = max(MIN_SCORE, min(raw_score, MAX_SCORE))

    # -------------------------------------------------
    # Velocity clamp
    # -------------------------------------------------

    final_score = clamp_score_velocity(
        new_score=raw_score,
        prev_score=prev_score,
    )

    final_score = max(MIN_SCORE, min(final_score, MAX_SCORE))

    return round(final_score, 2)
