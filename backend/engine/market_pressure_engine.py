import logging
from typing import Dict, Optional

from backend.engine.transition_detector import get_transition_risk_value
from backend.ai_core.regime_memory import get_regime_memory  # 🔥 NEW

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# =========================================================
# CONFIG
# =========================================================

DEFAULT_WEIGHTS = {
    "market_score": 0.35,
    "technical_score": 0.25,
    "macro_score": 0.25,
    "setup_score": 0.15,
}

BASELINE_PRESSURE = 0.5


# 🔥 REGIME MODIFIERS (institutional behavior)
REGIME_PRESSURE_MAP = {
    "risk_off": 0.65,
    "distribution": 0.70,
    "range": 0.85,
    "accumulation": 1.05,
    "risk_on": 1.15,
}


# =========================================================
# HELPERS
# =========================================================

def _clamp(v: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(v, hi))


def _normalize_score(score: Optional[float]) -> float:
    try:
        if score is None:
            return BASELINE_PRESSURE

        score = float(score)

        if score <= 0:
            return 0.0

        return _clamp(score / 100.0)

    except Exception:
        return BASELINE_PRESSURE


def _get_regime_modifier(user_id: int) -> float:
    """
    Pull regime from memory.

    NEVER crashes the engine.
    """

    try:
        regime = get_regime_memory(user_id)

        if not regime:
            return 1.0

        label = (regime.get("label") or "").lower()

        return REGIME_PRESSURE_MAP.get(label, 1.0)

    except Exception as e:
        logger.warning("Regime modifier fallback: %s", e)
        return 1.0


# =========================================================
# CORE ENGINE
# =========================================================

def calculate_market_pressure(
    scores: Dict[str, float],
    user_id: int,
    *,
    weights: Dict[str, float] = DEFAULT_WEIGHTS,
) -> float:

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

    base_pressure = (
        weighted_sum / weight_total
        if weight_total > 0
        else BASELINE_PRESSURE
    )

    # -----------------------------------------------------
    # Transition overlay
    # -----------------------------------------------------

    transition_risk = get_transition_risk_value(user_id)

    if transition_risk is None:
        transition_risk = 0.5

    transition_risk = _clamp(float(transition_risk))

    stability = 1.0 - transition_risk

    if transition_risk > 0.7:
        penalty = transition_risk * 0.55
    elif transition_risk > 0.5:
        penalty = transition_risk * 0.35
    else:
        penalty = transition_risk * 0.20

    pressure = base_pressure * stability
    pressure -= penalty

    # -----------------------------------------------------
    # 🔥 REGIME MODIFIER (NEW — BIG UPGRADE)
    # -----------------------------------------------------

    regime_modifier = _get_regime_modifier(user_id)

    pressure *= regime_modifier

    # -----------------------------------------------------
    # Regime acceleration boost
    # -----------------------------------------------------

    if base_pressure > 0.7 and transition_risk < 0.3:
        pressure += 0.08

    # -----------------------------------------------------
    # Clamp
    # -----------------------------------------------------

    pressure = _clamp(pressure)

    return round(pressure, 4)


# =========================================================
# ENGINE HELPER
# =========================================================

def get_market_pressure(
    user_id: int,
    scores: Dict[str, float],
) -> float:

    try:
        return calculate_market_pressure(
            scores=scores,
            user_id=user_id,
        )

    except Exception as e:
        logger.warning("Market pressure fallback triggered: %s", e)
        return BASELINE_PRESSURE
