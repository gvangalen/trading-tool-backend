import logging
from typing import Dict, Optional

from backend.engine.transition_detector import get_transition_risk_value

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# =========================================================
# CONFIG (institutional defaults)
# =========================================================

DEFAULT_WEIGHTS = {
    "market_score": 0.35,
    "technical_score": 0.25,
    "macro_score": 0.25,
    "setup_score": 0.15,
}

BASELINE_PRESSURE = 0.5


# =========================================================
# HELPERS
# =========================================================

def _clamp(v: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(v, hi))


def _normalize_score(score: Optional[float]) -> float:
    """
    Converts 0–100 score → 0–1
    Fail-safe → neutral
    """
    try:
        if score is None:
            return BASELINE_PRESSURE

        score = float(score)

        if score <= 0:
            return 0.0

        return _clamp(score / 100.0)

    except Exception:
        return BASELINE_PRESSURE


# =========================================================
# CORE ENGINE
# =========================================================

def calculate_market_pressure(
    scores: Dict[str, float],
    user_id: int,
    *,
    weights: Dict[str, float] = DEFAULT_WEIGHTS,
) -> float:
    """
    Institutional risk synthesizer.

    Combines:

    ✔ multi-factor scores
    ✔ transition risk
    ✔ regime stability proxy

    Returns:

        0.0 → extreme defensive
        0.5 → neutral
        1.0 → aggressive risk-on
    """

    if not scores:
        return BASELINE_PRESSURE

    # -----------------------------------------------------
    # Weighted score pressure
    # -----------------------------------------------------

    weighted_sum = 0.0
    weight_total = 0.0

    for key, weight in weights.items():

        try:
            weight = float(weight)
        except Exception:
            continue

        normalized = _normalize_score(scores.get(key))

        weighted_sum += normalized * weight
        weight_total += weight

    if weight_total == 0:
        base_pressure = BASELINE_PRESSURE
    else:
        base_pressure = weighted_sum / weight_total

    # -----------------------------------------------------
    # Transition overlay (VERY important)
    # -----------------------------------------------------

    transition_risk = get_transition_risk_value(user_id)

    if transition_risk is None:
        transition_risk = 0.5

    transition_risk = _clamp(float(transition_risk))

    stability = 1.0 - transition_risk

    # instability should hit harder than stability boosts
    if transition_risk > 0.7:
        penalty = transition_risk * 0.55
    elif transition_risk > 0.5:
        penalty = transition_risk * 0.35
    else:
        penalty = transition_risk * 0.20

    pressure = base_pressure * stability
    pressure -= penalty

    # -----------------------------------------------------
    # Regime acceleration boost
    # -----------------------------------------------------

    if base_pressure > 0.7 and transition_risk < 0.3:
        pressure += 0.08  # allow aggressiveness in clean regimes

    # -----------------------------------------------------
    # Clamp (BOT SAFETY)
    # -----------------------------------------------------

    pressure = _clamp(pressure)

    return round(pressure, 4)


# =========================================================
# ENGINE HELPER (BOT SAFE)
# =========================================================

def get_market_pressure(
    user_id: int,
    scores: Dict[str, float],
) -> float:
    """
    Bot-safe accessor.

    NEVER crashes.
    ALWAYS returns float.
    """

    try:
        return calculate_market_pressure(
            scores=scores,
            user_id=user_id,
        )

    except Exception as e:
        logger.warning("Market pressure fallback triggered: %s", e)
        return BASELINE_PRESSURE
