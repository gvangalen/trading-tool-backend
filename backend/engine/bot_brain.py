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
from backend.engine.decision_engine import decide_amount
from backend.ai_core.regime_memory import get_regime_memory

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


def _normalize_scores(scores: Dict[str, float]) -> Dict[str, float]:
    """
    Accepteert zowel:
      - {macro_score, technical_score, market_score, setup_score}
    als:
      - {macro, technical, market, setup}
    """
    return {
        "macro_score": scores.get("macro_score", scores.get("macro", 10)),
        "technical_score": scores.get("technical_score", scores.get("technical", 10)),
        "market_score": scores.get("market_score", scores.get("market", 10)),
        "setup_score": scores.get("setup_score", scores.get("setup", 10)),
    }


def _classify_risk_state(transition_risk: float, pressure: float) -> str:
    if transition_risk > 0.75:
        return "unstable"
    if transition_risk > 0.6:
        return "transition"
    if pressure < 0.4:
        return "defensive"
    if pressure > 0.7 and transition_risk < 0.3:
        return "risk_on"
    return "neutral"


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
    """
    Single-source decision brain.

    Output contract (used by trading_bot_agent):
      - action: buy|hold (optioneel later sell)
      - amount_eur: float
      - confidence: 0..1
      - reason: str
      - market_pressure: 0..1
      - transition_risk: 0..1
      - exposure_multiplier: float
      - watch_levels: dict|None
      - monitoring: bool
      - alerts_active: bool
    """

    rules = {**DEFAULT_ACTION_RULES, **(action_rules or {})}
    scores = _normalize_scores(scores)

    # -------------------------------------------------
    # 1️⃣ Regime Memory
    # -------------------------------------------------
    regime_memory = None
    regime_label = None
    regime_confidence = None

    try:
        regime_memory = get_regime_memory(user_id)
        if isinstance(regime_memory, dict):
            regime_label = regime_memory.get("regime_label")
            regime_confidence = regime_memory.get("confidence")
    except Exception as e:
        logger.warning("Regime memory unavailable: %s", e)

    # -------------------------------------------------
    # 2️⃣ Transition Risk
    # -------------------------------------------------
    try:
        transition_risk = float(get_transition_risk_value(user_id))
        transition_risk = _clamp(transition_risk, 0.0, 1.0)
        transition_snapshot = compute_transition_detector(user_id)
    except Exception as e:
        logger.warning("Transition detector fallback: watch mode %s", e)
        transition_risk = 0.5
        transition_snapshot = None

    # -------------------------------------------------
    # 3️⃣ Market Pressure
    # -------------------------------------------------
    try:
        market_pressure = float(get_market_pressure(user_id=user_id, scores=scores))
        market_pressure = _clamp(market_pressure, 0.0, 1.0)
    except Exception as e:
        logger.warning("Market pressure fallback: %s", e)
        market_pressure = 0.5

    # -------------------------------------------------
    # 4️⃣ Volatility Regime
    # -------------------------------------------------
    if transition_risk > 0.7:
        volatility_state = "expanding"
    elif market_pressure < 0.35:
        volatility_state = "compressed"
    else:
        volatility_state = "normal"

    # -------------------------------------------------
    # 5️⃣ Trend Strength
    # -------------------------------------------------
    trend_strength = (
        float(scores.get("technical_score", 10)) * 0.6
        + float(scores.get("market_score", 10)) * 0.4
    ) / 100.0

    # -------------------------------------------------
    # 6️⃣ Structure Bias
    # -------------------------------------------------
    if trend_strength > 0.65:
        structure_bias = "trend"
    elif trend_strength < 0.35:
        structure_bias = "range"
    else:
        structure_bias = "neutral"

    # -------------------------------------------------
    # 7️⃣ Risk Environment
    # -------------------------------------------------
    risk_environment = ((1.0 - transition_risk) * 0.5) + (market_pressure * 0.5)

    # -------------------------------------------------
    # 8️⃣ Exposure Multiplier
    # -------------------------------------------------
    exposure_pack = compute_exposure_multiplier(
        regime_memory=regime_memory,
        transition_risk=transition_risk,
    )

    exposure_multiplier = _clamp(
        _safe_float(exposure_pack.get("multiplier"), 1.0) or 1.0,
        0.0,
        EXPOSURE_CAP,
    )

    # -------------------------------------------------
    # 9️⃣ Base Amount
    # -------------------------------------------------
    try:
        base_amount = float(decide_amount(setup=setup, scores=scores))
        base_amount = max(0.0, base_amount)
        base_reason = "Base amount via DecisionEngine."
    except Exception as e:
        base_amount = 0.0
        base_reason = f"DecisionEngine fallback: {e}"

    # -------------------------------------------------
    # 🔟 Apply Exposure
    # -------------------------------------------------
    final_amount = apply_exposure_to_amount(base_amount, exposure_multiplier)

    # -------------------------------------------------
    # 11️⃣ Risk State
    # -------------------------------------------------
    risk_state = _classify_risk_state(transition_risk, market_pressure)

    # -------------------------------------------------
    # 12️⃣ Action Logic
    # -------------------------------------------------
    market_score = _safe_float(scores.get("market_score"), 10) or 10.0
    action = "hold"
    reason_parts = []

    if base_amount <= 0 or final_amount <= 0:
        reason_parts.append("No executable size.")
        reason_parts.append(base_reason)

    elif transition_risk > float(rules["max_transition_risk_to_buy"]):
        reason_parts.append("Transition risk too high.")

    elif market_pressure < float(rules["min_market_pressure_to_buy"]):
        reason_parts.append("Market pressure too low.")

    elif market_score < float(rules["min_market_score_to_buy"]):
        reason_parts.append("Market score below threshold.")

    else:
        action = "buy"
        reason_parts.append("All allocator conditions satisfied.")

    # -------------------------------------------------
    # 🆕 13️⃣ WATCH LEVELS (KEY UPDATE)
    # -------------------------------------------------
    watch_levels = None
    try:
        entry = (
            setup.get("entry_price")
            or setup.get("entry")
            or setup.get("trigger_price")
        )
        if entry:
            entry = float(entry)
            watch_levels = {
                "pullback_zone": round(entry * 0.96, 2),
                "breakout_trigger": round(entry * 1.03, 2),
            }
    except Exception:
        watch_levels = None

    # monitoring active when bot is waiting
    monitoring = action == "hold"

    # alerts active when market is tradable but waiting for trigger
    alerts_active = (
        monitoring
        and transition_risk < 0.75
        and market_pressure > 0.35
    )

    # -------------------------------------------------
    # 14️⃣ Confidence
    # -------------------------------------------------
    confidence_components = []

    if isinstance(regime_confidence, (int, float)):
        rc = float(regime_confidence)
        if rc > 1:
            rc = rc / 100.0
        confidence_components.append(_clamp(rc, 0.0, 1.0))

    confidence_components.append(_clamp(1.0 - transition_risk, 0.0, 1.0))
    confidence_components.append(_clamp(market_pressure, 0.0, 1.0))

    confidence = round(sum(confidence_components) / max(1, len(confidence_components)), 3)

    # -------------------------------------------------
    # 15️⃣ Trade Quality
    # -------------------------------------------------
    trade_quality = round(
        (
            risk_environment * 0.4
            + trend_strength * 0.3
            + (float(scores.get("setup_score", 10)) / 100.0) * 0.3
        )
        * 100.0,
        1,
    )

    # -------------------------------------------------
    # FINAL OUTPUT
    # -------------------------------------------------
    return {
        "date": date.today().isoformat(),
        "action": action,
        "amount_eur": round(float(final_amount), 2),
        "confidence": confidence,
        "reason": " ".join(reason_parts),
        # regime
        "regime": regime_label,
        "risk_state": risk_state,
        # market state
        "market_pressure": market_pressure,
        "transition_risk": transition_risk,
        "volatility_state": volatility_state,
        # structure
        "trend_strength": trend_strength,
        "structure_bias": structure_bias,
        # environment
        "risk_environment": risk_environment,
        # sizing
        "exposure_multiplier": exposure_multiplier,
        # scoring
        "trade_quality": trade_quality,
        # 🆕 WATCH MODE INTELLIGENCE
        "watch_levels": watch_levels,
        "monitoring": monitoring,
        "alerts_active": alerts_active,
        # debug
        "debug": {
            "scores": scores,
            "transition_snapshot": transition_snapshot,
            "regime_memory": regime_memory,
            "exposure": exposure_pack,
            "base_amount": base_amount,
            "final_amount": final_amount,
            "base_reason": base_reason,
        },
    }
