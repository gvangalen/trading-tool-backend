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

def _extract_decision_amount(decision_result: Any) -> tuple[float, str]:
    """
    Decision engine kan teruggeven:
    - float/int
    - dict met amount_eur / amount / reason
    """

    if isinstance(decision_result, dict):
        amount = _safe_float(
            decision_result.get("amount_eur", decision_result.get("amount")),
            0.0,
        ) or 0.0

        reason = str(
            decision_result.get("reason")
            or decision_result.get("message")
            or "Base amount via DecisionEngine dict."
        )

        return max(0.0, amount), reason

    amount = _safe_float(decision_result, 0.0) or 0.0
    return max(0.0, amount), "Base amount via DecisionEngine."


# =========================================================
# Market Cycle
# =========================================================


def _determine_market_cycle(
    trend_strength: float,
    market_pressure: float,
    transition_risk: float,
) -> str:
    if trend_strength < 0.35 and market_pressure < 0.45:
        return "accumulation"

    if trend_strength >= 0.35 and market_pressure >= 0.45 and transition_risk < 0.6:
        return "expansion"

    if trend_strength >= 0.55 and transition_risk > 0.5:
        return "distribution"

    if trend_strength < 0.35 and transition_risk > 0.6:
        return "correction"

    return "neutral"


# =========================================================
# Temperature
# =========================================================


def _determine_temperature(market_pressure: float) -> str:
    if market_pressure > 0.75:
        return "hot"

    if market_pressure > 0.55:
        return "warm"

    if market_pressure > 0.35:
        return "cool"

    return "cold"


# =========================================================
# Trend Detection
# =========================================================


def _determine_trend(value: float) -> str:
    if value > 0.65:
        return "bullish"

    if value < 0.35:
        return "bearish"

    return "trading_range"


# =========================================================
# Dashboard mapping helpers
# =========================================================


def _to_score_100(value: float) -> int:
    return round(_clamp(value, 0.0, 1.0) * 100)


def _map_volatility_state_to_score(volatility_state: str) -> int:
    mapping = {
        "compressed": 20,
        "normal": 50,
        "expanding": 80,
    }
    return mapping.get((volatility_state or "").lower(), 50)


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
        market_pressure = float(
            get_market_pressure(user_id=user_id, scores=scores)
        )
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
    # 6.5️⃣ Market Cycle
    # -------------------------------------------------

    market_cycle = _determine_market_cycle(
        trend_strength=trend_strength,
        market_pressure=market_pressure,
        transition_risk=transition_risk,
    )

    # -------------------------------------------------
    # 6.6️⃣ Temperature
    # -------------------------------------------------

    temperature = _determine_temperature(market_pressure)

    # -------------------------------------------------
    # 6.7️⃣ Multi timeframe trends
    # -------------------------------------------------

    short_trend = _determine_trend(trend_strength)

    mid_trend = _determine_trend(
        (trend_strength * 0.7) + (market_pressure * 0.3)
    )

    long_trend = _determine_trend(
        (trend_strength * 0.5)
        + (market_pressure * 0.3)
        + ((1 - transition_risk) * 0.2)
    )

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
        decision_result = decide_amount(setup=setup, scores=scores)
        logger.info("DecisionEngine raw output: %s", decision_result)

        base_amount, base_reason = _extract_decision_amount(decision_result)

    except Exception as e:
        base_amount = 0.0
        base_reason = f"DecisionEngine fallback: {e}"
        decision_result = None

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
    # 13️⃣ Confidence
    # -------------------------------------------------

    confidence_components = []

    if isinstance(regime_confidence, (int, float)):
        rc = float(regime_confidence)

        if rc > 1:
            rc = rc / 100.0

        confidence_components.append(_clamp(rc, 0.0, 1.0))

    confidence_components.append(_clamp(1.0 - transition_risk, 0.0, 1.0))
    confidence_components.append(_clamp(market_pressure, 0.0, 1.0))

    confidence = round(
        sum(confidence_components) / max(1, len(confidence_components)),
        3,
    )

    # -------------------------------------------------
    # 14️⃣ Trade Quality
    # -------------------------------------------------

    trade_quality = round(
        (
            risk_environment * 0.4
            + trend_strength * 0.3
            + (float(scores.get("setup_score", 10)) / 100.0) * 0.3
        ) * 100.0,
        1,
    )

    # -------------------------------------------------
    # 15️⃣ Dashboard-ready metrics
    # -------------------------------------------------

    market_pressure_score = _to_score_100(market_pressure)
    transition_risk_score = _to_score_100(transition_risk)
    trend_strength_score = _to_score_100(trend_strength)
    setup_quality_score = round(trade_quality)
    volatility_score = _map_volatility_state_to_score(volatility_state)

    # -------------------------------------------------
    # 16️⃣ Watch levels for hold / observe mode
    # -------------------------------------------------

    entry_value = _safe_float(setup.get("entry"))
    stop_value = _safe_float(setup.get("stop_loss"))

    raw_targets = setup.get("targets") or []
    if not isinstance(raw_targets, list):
        raw_targets = []

    clean_targets = []
    for t in raw_targets:
        tv = _safe_float(t)
        if tv is not None:
            clean_targets.append(tv)

    watch_levels = {
        "entry": entry_value,
        "stop_loss": stop_value,
        "targets": clean_targets,
        "pullback_zone": entry_value,
        "breakout_trigger": clean_targets[0] if clean_targets else None,
    }

    monitoring = any(
        v is not None and v != []
        for v in [
            watch_levels.get("entry"),
            watch_levels.get("stop_loss"),
            watch_levels.get("pullback_zone"),
            watch_levels.get("breakout_trigger"),
            watch_levels.get("targets"),
        ]
    )

    alerts_active = monitoring

    # -------------------------------------------------
    # FINAL OUTPUT
    # -------------------------------------------------

    return {
        "date": date.today().isoformat(),
        "action": action,
        "amount_eur": round(float(final_amount), 2),
        "confidence": confidence,
        "reason": " ".join(reason_parts),

        # regime / market structure
        "regime": regime_label,
        "cycle": market_cycle,
        "temperature": temperature,

        # trends
        "short_trend": short_trend,
        "mid_trend": mid_trend,
        "long_trend": long_trend,

        # raw engine state
        "market_pressure": market_pressure,
        "transition_risk": transition_risk,
        "volatility_state": volatility_state,
        "trend_strength": trend_strength,
        "structure_bias": structure_bias,
        "risk_environment": risk_environment,
        "risk_state": risk_state,

        # sizing
        "exposure_multiplier": exposure_multiplier,

        # scoring
        "trade_quality": trade_quality,

        # watch / monitoring
        "watch_levels": watch_levels,
        "monitoring": monitoring,
        "alerts_active": alerts_active,

        # dashboard-ready metrics
        "metrics": {
            "market_pressure": market_pressure_score,
            "transition_risk": transition_risk_score,
            "setup_quality": setup_quality_score,
            "volatility": volatility_score,
            "trend_strength": trend_strength_score,
            "position_size": exposure_multiplier,
        },

        # debug
        "debug": {
            "scores": scores,
            "transition_snapshot": transition_snapshot,
            "regime_memory": regime_memory,
            "exposure": exposure_pack,
            "decision_result": decision_result,
            "base_amount": base_amount,
            "final_amount": final_amount,
            "base_reason": base_reason,
            "watch_levels": watch_levels,
        },
    }
