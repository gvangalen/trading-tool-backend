from __future__ import annotations

import logging
from datetime import date
from typing import Any, Dict, Optional

from backend.engine.transition_detector import (
    compute_transition_detector,
    get_transition_risk_value,
)
from backend.engine.market_pressure_engine import get_market_pressure
from backend.engine.exposure_engine import (
    compute_exposure_multiplier,
    apply_exposure_to_amount,
)
from backend.engine.decision_engine import decide_amount, DecisionEngineError
from backend.ai_core.regime_memory import get_latest_regime_memory

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# =========================================================
# Config
# =========================================================

DEFAULT_ACTION_RULES = {
    "min_market_score_to_buy": 55.0,
    "max_transition_risk_to_buy": 0.60,
    "min_market_pressure_to_buy": 0.52,
}

EXPOSURE_CAP = 2.0


# =========================================================
# Helpers
# =========================================================

def _safe_float(x: Any, fallback: Optional[float] = None) -> Optional[float]:
    try:
        if x is None:
            return fallback
        return float(x)
    except Exception:
        return fallback


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(x, hi))


# ⭐ NEW — prevents schema drift between agents
def _normalize_scores(scores: Dict[str, float]) -> Dict[str, float]:
    """
    Accepts BOTH formats:

    macro OR macro_score
    """

    return {
        "macro_score": scores.get("macro_score", scores.get("macro", 10)),
        "technical_score": scores.get("technical_score", scores.get("technical", 10)),
        "market_score": scores.get("market_score", scores.get("market", 10)),
        "setup_score": scores.get("setup_score", scores.get("setup", 10)),
    }


# =========================================================
# Core brain
# =========================================================

def run_bot_brain(
    *,
    user_id: int,
    setup: Dict[str, Any],
    scores: Dict[str, float],
    action_rules: Optional[Dict[str, float]] = None,
) -> Dict[str, Any]:

    rules = {**DEFAULT_ACTION_RULES, **(action_rules or {})}

    # ⭐ normalize once
    scores = _normalize_scores(scores)

    # -------------------------------------------------
    # 1) Regime Memory
    # -------------------------------------------------

    regime_memory = None
    try:
        regime_memory = get_latest_regime_memory(user_id)
    except Exception as e:
        logger.warning("Regime memory unavailable: %s", e)

    # -------------------------------------------------
    # 2) Transition Risk
    # -------------------------------------------------

    try:
        transition_risk = float(get_transition_risk_value(user_id))
        transition_risk = _clamp(transition_risk, 0.0, 1.0)
        transition_snapshot = compute_transition_detector(user_id)
    except Exception as e:
        logger.warning("Transition detector fallback: %s", e)
        transition_risk = 0.5
        transition_snapshot = None

    # -------------------------------------------------
    # 3) Market Pressure
    # -------------------------------------------------

    try:
        market_pressure = float(
            get_market_pressure(
                user_id=user_id,
                scores=scores,
            )
        )
        market_pressure = _clamp(market_pressure, 0.0, 1.0)
    except Exception as e:
        logger.warning("Market pressure fallback: %s", e)
        market_pressure = 0.5

    # -------------------------------------------------
    # 4) Exposure Multiplier
    # -------------------------------------------------

    exposure_pack = compute_exposure_multiplier(
        regime_memory=regime_memory,
        transition_risk=transition_risk,
    )

    exposure_multiplier = _safe_float(
        exposure_pack.get("multiplier"), 1.0
    ) or 1.0

    # ⭐ institutional safety cap
    exposure_multiplier = _clamp(
        exposure_multiplier,
        0.0,
        EXPOSURE_CAP,
    )

    # -------------------------------------------------
    # 5) Base Amount
    # -------------------------------------------------

    try:
        base_amount = float(decide_amount(setup=setup, scores=scores))
        base_amount = max(0.0, base_amount)
        base_reason = "Base amount via DecisionEngine."
    except DecisionEngineError as e:
        base_amount = 0.0
        base_reason = f"DecisionEngineError: {e}"
    except Exception as e:
        base_amount = 0.0
        base_reason = f"DecisionEngine fallback: {e}"

    # -------------------------------------------------
    # 6) Apply Exposure
    # -------------------------------------------------

    final_amount = apply_exposure_to_amount(
        base_amount,
        exposure_multiplier,
    )

    # -------------------------------------------------
    # 7) Action Logic (deterministic)
    # -------------------------------------------------

    market_score = _safe_float(scores.get("market_score"), 10)

    action = "hold"
    reason_parts = []

    if base_amount <= 0 or final_amount <= 0:
        reason_parts.append("No executable size.")
        reason_parts.append(base_reason)

    elif transition_risk > rules["max_transition_risk_to_buy"]:
        reason_parts.append("Transition risk too high.")

    elif market_pressure < rules["min_market_pressure_to_buy"]:
        reason_parts.append("Market pressure too low.")

    elif market_score < rules["min_market_score_to_buy"]:
        reason_parts.append("Market score below threshold.")

    else:
        action = "buy"
        reason_parts.append("All allocator conditions satisfied.")

    # -------------------------------------------------
    # Confidence
    # -------------------------------------------------

    confidence_components = []

    if isinstance(regime_memory, dict):
        rconf = _safe_float(regime_memory.get("confidence"), None)
        if rconf is not None:
            if rconf > 1:
                rconf = rconf / 100.0
            confidence_components.append(_clamp(rconf, 0.0, 1.0))

    confidence_components.append(1.0 - transition_risk)
    confidence_components.append(market_pressure)

    confidence = None
    if confidence_components:
        confidence = round(
            sum(confidence_components) / len(confidence_components),
            3,
        )

    return {
        "date": date.today().isoformat(),
        "action": action,
        "amount_eur": round(float(final_amount), 2),
        "confidence": confidence,
        "reason": " ".join(reason_parts),
        "debug": {
            "scores": scores,
            "transition_risk": transition_risk,
            "transition_snapshot": transition_snapshot,
            "market_pressure": market_pressure,
            "regime_memory": regime_memory,
            "exposure": exposure_pack,
            "base_amount": base_amount,
            "final_amount": final_amount,
            "base_reason": base_reason,
        },
    }
