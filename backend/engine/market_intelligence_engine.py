from __future__ import annotations

import logging
from datetime import date
from typing import Any, Dict, Optional

from backend.engine.transition_detector import compute_transition_detector
from backend.engine.market_pressure_engine import get_market_pressure

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


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
# Trend / Regime logic
# =========================================================

def _determine_trend(value: float) -> str:
    if value > 0.65:
        return "bullish"
    if value < 0.35:
        return "bearish"
    return "trading_range"


def _determine_structure_bias(trend_strength: float) -> str:
    if trend_strength > 0.65:
        return "trend"
    if trend_strength < 0.35:
        return "range"
    return "neutral"


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


def _determine_temperature(market_pressure: float) -> str:
    if market_pressure > 0.75:
        return "hot"
    if market_pressure > 0.55:
        return "warm"
    if market_pressure > 0.35:
        return "cool"
    return "cold"


def _determine_volatility_state(
    transition_risk: float,
    market_pressure: float,
) -> str:
    if transition_risk > 0.7:
        return "expanding"
    if market_pressure < 0.35:
        return "compressed"
    return "normal"


def _classify_risk_state(
    transition_risk: float,
    pressure: float,
) -> str:
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
# Core engine
# =========================================================

def compute_market_intelligence(
    *,
    user_id: int,
    scores: Dict[str, float],
) -> Dict[str, Any]:
    """
    Losse Market Intelligence engine.

    Input:
    - user_id
    - scores: macro / technical / market / setup

    Output:
    - cycle
    - temperature
    - trend state
    - metrics
    - state
    """

    scores = _normalize_scores(scores)

    # -------------------------------------------------
    # Transition detector
    # -------------------------------------------------

    try:
        transition_snapshot = compute_transition_detector(user_id)
        transition_risk = _safe_float(
            transition_snapshot.get("normalized_risk"),
            0.5,
        ) or 0.5
        transition_risk = _clamp(transition_risk, 0.0, 1.0)

    except Exception as e:
        logger.warning("Transition detector fallback: %s", e)
        transition_snapshot = None
        transition_risk = 0.5

    # -------------------------------------------------
    # Market pressure
    # -------------------------------------------------
    
    try:
        raw_pressure = get_market_pressure(
            user_id=user_id,
            scores=scores,
        )
    
        market_pressure = _safe_float(raw_pressure, 0.5)
    
        # 🔥 EXTRA GUARD
        if market_pressure is None:
            market_pressure = 0.5
    
        market_pressure = _clamp(market_pressure, 0.0, 1.0)
    
    except Exception as e:
        logger.warning("Market pressure fallback: %s", e)
        market_pressure = 0.5

    # -------------------------------------------------
    # Trend strength
    # -------------------------------------------------

    trend_strength = (
        float(scores.get("technical_score", 10)) * 0.6
        + float(scores.get("market_score", 10)) * 0.4
    ) / 100.0

    trend_strength = _clamp(trend_strength, 0.0, 1.0)

    # -------------------------------------------------
    # Derived states
    # -------------------------------------------------

    structure_bias = _determine_structure_bias(trend_strength)

    market_cycle = _determine_market_cycle(
        trend_strength=trend_strength,
        market_pressure=market_pressure,
        transition_risk=transition_risk,
    )

    temperature = _determine_temperature(market_pressure)

    volatility_state = _determine_volatility_state(
        transition_risk=transition_risk,
        market_pressure=market_pressure,
    )

    short_trend = _determine_trend(trend_strength)

    mid_trend = _determine_trend(
        (trend_strength * 0.7) + (market_pressure * 0.3)
    )

    long_trend = _determine_trend(
        (trend_strength * 0.5)
        + (market_pressure * 0.3)
        + ((1 - transition_risk) * 0.2)
    )

    risk_environment = (
        ((1.0 - transition_risk) * 0.5) + (market_pressure * 0.5)
    )
    risk_environment = round(_clamp(risk_environment, 0.0, 1.0), 4)

    risk_state = _classify_risk_state(
        transition_risk=transition_risk,
        pressure=market_pressure,
    )

    # -------------------------------------------------
    # Dashboard-ready metrics
    # -------------------------------------------------

    market_pressure_score = _to_score_100(market_pressure)
    transition_risk_score = _to_score_100(transition_risk)
    trend_strength_score = _to_score_100(trend_strength)
    volatility_score = _map_volatility_state_to_score(volatility_state)

    setup_score = round(
        _clamp(
            float(scores.get("setup_score", 10)) / 100.0,
            0.0,
            1.0,
        ) * 100.0
    )

    # -------------------------------------------------
    # Final output
    # -------------------------------------------------

    return {
        "cycle": market_cycle,
        "temperature": temperature,

        "trend": {
            "short": short_trend,
            "mid": mid_trend,
            "long": long_trend,
        },

        "metrics": {
            "market_pressure": market_pressure_score,
            "transition_risk": transition_risk_score,
            "setup_quality": int(setup_score),
            "volatility": volatility_score,
            "trend_strength": trend_strength_score,
        },

        "state": {
            "market_pressure": round(market_pressure, 4),
            "transition_risk": round(transition_risk, 4),
            "trend_strength": round(trend_strength, 4),
            "risk_environment": risk_environment,
            "risk_state": risk_state,
            "structure_bias": structure_bias,
            "volatility_state": volatility_state,
        },

        "debug": {
            "scores": scores,
            "transition_snapshot": transition_snapshot,
        },

        "generated_at": date.today().isoformat(),
    }


# =========================================================
# Convenience wrapper
# =========================================================

def get_market_intelligence(
    *,
    user_id: int,
    scores: Dict[str, float],
) -> Dict[str, Any]:
    try:
        return compute_market_intelligence(
            user_id=user_id,
            scores=scores,
        )
    except Exception as e:
        logger.warning("Market intelligence fallback triggered: %s", e)

        return {
            "cycle": "neutral",
            "temperature": "cool",
            "trend": {
                "short": "trading_range",
                "mid": "trading_range",
                "long": "trading_range",
            },
            "metrics": {
                "market_pressure": 50,
                "transition_risk": 50,
                "setup_quality": 10,
                "volatility": 50,
                "trend_strength": 50,
            },
            "state": {
                "market_pressure": 0.5,
                "transition_risk": 0.5,
                "trend_strength": 0.5,
                "risk_environment": 0.5,
                "risk_state": "neutral",
                "structure_bias": "neutral",
                "volatility_state": "normal",
            },
            "debug": {
                "scores": _normalize_scores(scores or {}),
                "transition_snapshot": None,
                "fallback_reason": str(e),
            },
            "generated_at": date.today().isoformat(),
        }
