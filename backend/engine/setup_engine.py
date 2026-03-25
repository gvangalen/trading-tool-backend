from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# =========================================================
# Helpers
# =========================================================

def _safe_float(value: Any, fallback: Optional[float] = None) -> Optional[float]:
    try:
        if value is None:
            return fallback
        return float(value)
    except Exception:
        return fallback


def _safe_str(value: Any, fallback: str = "") -> str:
    if value is None:
        return fallback
    return str(value).strip()


def _normalize_setup_type(setup: Dict[str, Any]) -> str:
    """
    New model:
    - setup_type = dca_basic / dca_smart / breakout

    Backward compatibility:
    - old strategy_type = dca -> dca_basic
    - everything else -> breakout
    """
    setup_type = _safe_str(setup.get("setup_type")).lower()
    if setup_type:
        return setup_type

    old_type = _safe_str(setup.get("strategy_type")).lower()
    if old_type == "dca":
        return "dca_basic"

    return "breakout"


def _today_info() -> Dict[str, Any]:
    now = datetime.utcnow()
    weekday = now.strftime("%A").lower()   # monday, tuesday, ...
    month_day = now.day
    return {
        "weekday": weekday,
        "month_day": month_day,
    }


def _is_dca_time(setup: Dict[str, Any]) -> tuple[bool, str]:
    """
    Supports:
    - daily
    - weekly + dca_day
    - monthly + dca_month_day
    """
    frequency = _safe_str(setup.get("dca_frequency"), "weekly").lower()
    dca_day = _safe_str(setup.get("dca_day"), "monday").lower()
    dca_month_day = int(_safe_float(setup.get("dca_month_day"), 1) or 1)

    today = _today_info()
    today_weekday = today["weekday"]
    today_month_day = today["month_day"]

    if frequency == "daily":
        return True, "Daily DCA schedule active"

    if frequency == "weekly":
        if today_weekday == dca_day:
            return True, f"Weekly DCA active ({dca_day})"
        return False, f"Weekly DCA waiting for {dca_day}"

    if frequency == "monthly":
        if today_month_day == dca_month_day:
            return True, f"Monthly DCA active (day {dca_month_day})"
        return False, f"Monthly DCA waiting for day {dca_month_day}"

    return False, f"Unknown DCA frequency: {frequency}"


# =========================================================
# Setup dispatcher
# =========================================================

def run_setup_logic(
    *,
    setup: Dict[str, Any],
    scores: Dict[str, float],
    market_pressure: float,
    transition_risk: float,
    trend_strength: float,
    risk_environment: float,
    final_amount: float,
    watch_levels: Dict[str, Any],
    portfolio_context: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Central setup dispatcher.

    Returns:
    {
        "setup_type": str,
        "action": "buy" | "hold" | "observe",
        "reason": str,
        "intent_note": str,
    }
    """
    setup_type = _normalize_setup_type(setup)

    logger.info(
        "🧩 SetupEngine | setup_type=%s | final_amount=%.2f | market_pressure=%.3f | transition_risk=%.3f",
        setup_type,
        float(final_amount or 0.0),
        float(market_pressure or 0.0),
        float(transition_risk or 0.0),
    )

    if setup_type == "dca_basic":
        result = _run_dca_basic(
            setup=setup,
            final_amount=final_amount,
        )

    elif setup_type == "dca_smart":
        result = _run_dca_smart(
            setup=setup,
            scores=scores,
            market_pressure=market_pressure,
            transition_risk=transition_risk,
            trend_strength=trend_strength,
            risk_environment=risk_environment,
            final_amount=final_amount,
        )

    elif setup_type == "breakout":
        result = _run_breakout(
            setup=setup,
            scores=scores,
            market_pressure=market_pressure,
            transition_risk=transition_risk,
            trend_strength=trend_strength,
            final_amount=final_amount,
            watch_levels=watch_levels,
            portfolio_context=portfolio_context,
        )

    else:
        result = {
            "action": "hold",
            "reason": f"Unknown setup type: {setup_type}",
            "intent_note": "No executable setup logic found",
        }

    result["setup_type"] = setup_type
    return result


# =========================================================
# 1) DCA BASIC
# =========================================================

def _run_dca_basic(
    *,
    setup: Dict[str, Any],
    final_amount: float,
) -> Dict[str, Any]:
    """
    Basic DCA:
    - buys on its schedule
    - no market filter
    - only depends on size > 0
    """
    is_time, timing_reason = _is_dca_time(setup)

    if not is_time:
        return {
            "action": "hold",
            "reason": timing_reason,
            "intent_note": "Basic DCA inactive due to schedule",
        }

    if (final_amount or 0.0) <= 0:
        return {
            "action": "hold",
            "reason": "Basic DCA scheduled, but no executable size",
            "intent_note": "Position engine returned zero size",
        }

    return {
        "action": "buy",
        "reason": f"Basic DCA active. {timing_reason}",
        "intent_note": "Always buy on schedule",
    }


# =========================================================
# 2) DCA SMART
# =========================================================

def _run_dca_smart(
    *,
    setup: Dict[str, Any],
    scores: Dict[str, float],
    market_pressure: float,
    transition_risk: float,
    trend_strength: float,
    risk_environment: float,
    final_amount: float,
) -> Dict[str, Any]:
    """
    Smart DCA:
    - must be on schedule
    - then applies market filters
    - only buys in acceptable market conditions

    Core idea:
    - avoid buying in unstable / weak conditions
    - buy when market quality is decent enough
    """
    is_time, timing_reason = _is_dca_time(setup)

    if not is_time:
        return {
            "action": "hold",
            "reason": timing_reason,
            "intent_note": "Smart DCA inactive due to schedule",
        }

    if (final_amount or 0.0) <= 0:
        return {
            "action": "hold",
            "reason": "Smart DCA scheduled, but no executable size",
            "intent_note": "Position engine returned zero size",
        }

    macro_score = _safe_float(scores.get("macro_score"), 10.0) or 10.0
    technical_score = _safe_float(scores.get("technical_score"), 10.0) or 10.0
    market_score = _safe_float(scores.get("market_score"), 10.0) or 10.0
    setup_score = _safe_float(scores.get("setup_score"), 10.0) or 10.0

    combined_score = round(
        (macro_score + technical_score + market_score + setup_score) / 4.0,
        1,
    )

    # Hard blocks
    if transition_risk >= 0.75:
        return {
            "action": "hold",
            "reason": f"Smart DCA blocked: transition risk too high ({transition_risk:.2f})",
            "intent_note": "Avoid unstable market transitions",
        }

    if market_pressure < 0.35:
        return {
            "action": "hold",
            "reason": f"Smart DCA blocked: market pressure too weak ({market_pressure:.2f})",
            "intent_note": "Wait for stronger market participation",
        }

    if combined_score < 40:
        return {
            "action": "hold",
            "reason": f"Smart DCA blocked: combined score too low ({combined_score})",
            "intent_note": "Wait for better multi-factor alignment",
        }

    # Cautious zone
    if transition_risk >= 0.60:
        return {
            "action": "hold",
            "reason": f"Smart DCA cautious hold: elevated transition risk ({transition_risk:.2f})",
            "intent_note": "Conditions not clean enough yet",
        }

    if market_pressure < 0.50:
        return {
            "action": "hold",
            "reason": f"Smart DCA cautious hold: market pressure below preferred threshold ({market_pressure:.2f})",
            "intent_note": "Wait for more supportive market context",
        }

    # Favorable buy
    quality_note = (
        f"macro={macro_score:.0f}, technical={technical_score:.0f}, "
        f"market={market_score:.0f}, setup={setup_score:.0f}, combined={combined_score}"
    )

    return {
        "action": "buy",
        "reason": f"Smart DCA active. {timing_reason}. Favorable conditions detected.",
        "intent_note": (
            f"Buy allowed: pressure={market_pressure:.2f}, transition_risk={transition_risk:.2f}, "
            f"trend_strength={trend_strength:.2f}, risk_environment={risk_environment:.2f}. "
            f"{quality_note}"
        ),
    }


# =========================================================
# 3) BREAKOUT
# =========================================================

def _run_breakout(
    *,
    setup: Dict[str, Any],
    scores: Dict[str, float],
    market_pressure: float,
    transition_risk: float,
    trend_strength: float,
    final_amount: float,
    watch_levels: Dict[str, Any],
    portfolio_context: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Breakout setup:
    - waits for breakout_trigger
    - can only buy if live price confirms breakout
    - still needs sufficient market quality
    """
    if (final_amount or 0.0) <= 0:
        return {
            "action": "hold",
            "reason": "Breakout setup has no executable size",
            "intent_note": "Position engine returned zero size",
        }

    breakout_level = _safe_float(watch_levels.get("breakout_trigger"))
    live_price = _safe_float(portfolio_context.get("live_price"))

    market_score = _safe_float(scores.get("market_score"), 10.0) or 10.0
    technical_score = _safe_float(scores.get("technical_score"), 10.0) or 10.0

    if breakout_level is None:
        return {
            "action": "hold",
            "reason": "Breakout setup missing breakout trigger",
            "intent_note": "No breakout trigger configured",
        }

    if live_price is None:
        return {
            "action": "hold",
            "reason": "Breakout setup waiting for live price",
            "intent_note": "No live market price available",
        }

    if live_price <= breakout_level:
        return {
            "action": "hold",
            "reason": f"Waiting for breakout above {breakout_level:.2f}",
            "intent_note": f"Live price {live_price:.2f} still below breakout level",
        }

    # Quality checks after breakout
    if transition_risk >= 0.70:
        return {
            "action": "hold",
            "reason": f"Breakout detected but blocked: transition risk too high ({transition_risk:.2f})",
            "intent_note": "Avoid fake breakout in unstable market",
        }

    if market_pressure < 0.45:
        return {
            "action": "hold",
            "reason": f"Breakout detected but blocked: market pressure too weak ({market_pressure:.2f})",
            "intent_note": "Breakout lacks sufficient participation",
        }

    if technical_score < 40 or market_score < 40:
        return {
            "action": "hold",
            "reason": (
                f"Breakout detected but blocked: weak confirmation "
                f"(technical={technical_score:.0f}, market={market_score:.0f})"
            ),
            "intent_note": "Need stronger confirmation after breakout",
        }

    return {
        "action": "buy",
        "reason": f"Breakout confirmed above {breakout_level:.2f}",
        "intent_note": (
            f"Live price={live_price:.2f}, breakout={breakout_level:.2f}, "
            f"pressure={market_pressure:.2f}, transition_risk={transition_risk:.2f}, "
            f"trend_strength={trend_strength:.2f}"
        ),
    }
