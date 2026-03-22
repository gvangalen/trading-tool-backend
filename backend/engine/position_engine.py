from typing import Dict, Any
import logging

from backend.engine.decision_engine import decide_amount

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def _safe_float(v, fallback: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return fallback


def _clamp(v: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(v, hi))


def calculate_position(
    *,
    setup: Dict[str, Any],
    scores: Dict[str, float],
    regime_memory: Dict[str, Any] | None,
    transition_risk: float | None,
) -> Dict[str, Any]:

    decision = decide_amount(
        setup=setup,
        scores=scores,
        regime_memory=regime_memory,
        transition_risk=transition_risk,
    )

    # -----------------------------
    # Extract values
    # -----------------------------
    base_amount = _safe_float(decision.get("base_amount"), 0.0)
    final_amount = _safe_float(decision.get("final_amount"), 0.0)
    exposure_multiplier = _safe_float(decision.get("exposure_multiplier"), 1.0)

    # -----------------------------
    # Position size (CORE LOGIC)
    # -----------------------------
    if base_amount > 0:
        position_size = final_amount / base_amount
    else:
        position_size = 0.0

    position_size = round(_clamp(position_size), 3)

    logger.info(
        "📊 PositionEngine | base=%.2f final=%.2f size=%.3f exposure=%.2f",
        base_amount,
        final_amount,
        position_size,
        exposure_multiplier,
    )

    return {
        "base_amount": round(base_amount, 2),
        "final_amount": round(final_amount, 2),
        "position_size": position_size,
        "exposure_multiplier": exposure_multiplier,
        "decision": decision,  # 🔥 rename van raw → duidelijker
    }
