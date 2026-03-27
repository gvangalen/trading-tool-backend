from __future__ import annotations

import logging
from datetime import date
from typing import Any, Dict, Optional

from backend.engine.position_engine import calculate_position
from backend.engine.market_intelligence_engine import get_market_intelligence
from backend.engine.guardrails_engine import apply_guardrails
from backend.engine.trade_plan_engine import build_trade_plan
from backend.ai_core.regime_memory import get_regime_memory

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# =========================================================
# Config
# =========================================================

DEFAULT_ACTION_RULES = {
    "min_confidence_to_buy": 60.0,
    "min_confidence_to_watch": 40.0,
    "min_market_score_to_buy": 55.0,
    "min_market_pressure_to_buy": 0.52,
    "max_transition_risk_to_buy": 0.60,
    "entry_tolerance_pct": 0.01,   # 1% boven entry nog ok
    "chase_limit_pct": 0.03,       # >3% boven entry = niet jagen
}

EXPOSURE_CAP = 1.0


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


def _safe_str(x: Any, fallback: str = "") -> str:
    if x is None:
        return fallback
    return str(x).strip()


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(x, hi))


def _normalize_scores(scores: Dict[str, float]) -> Dict[str, float]:
    return {
        "macro_score": _safe_float(scores.get("macro_score", scores.get("macro", 10)), 10.0) or 10.0,
        "technical_score": _safe_float(scores.get("technical_score", scores.get("technical", 10)), 10.0) or 10.0,
        "market_score": _safe_float(scores.get("market_score", scores.get("market", 10)), 10.0) or 10.0,
        "setup_score": _safe_float(scores.get("setup_score", scores.get("setup", 10)), 10.0) or 10.0,
    }


def _classify_risk_state(transition_risk: float, pressure: float) -> str:
    if transition_risk > 0.75:
        return "unstable"
    if transition_risk > 0.60:
        return "transition"
    if pressure < 0.40:
        return "defensive"
    if pressure > 0.70 and transition_risk < 0.30:
        return "risk_on"
    return "neutral"


def _normalize_setup_type(setup: Dict[str, Any], snapshot: Dict[str, Any]) -> str:
    setup_type = _safe_str(
        snapshot.get("setup_type")
        or setup.get("setup_type")
        or setup.get("strategy_type")
    ).lower()

    if setup_type in {"dca", "trade"}:
        return setup_type

    # backward compatibility
    if setup_type in {"dca_basic", "dca_smart"}:
        return "dca"
    if setup_type in {"breakout", "manual", "trading"}:
        return "trade"

    return "unknown"


def _extract_snapshot(portfolio_context: Dict[str, Any]) -> Dict[str, Any]:
    snapshot = portfolio_context.get("active_strategy") or portfolio_context.get("strategy_snapshot") or {}
    return snapshot if isinstance(snapshot, dict) else {}


def _extract_levels(setup: Dict[str, Any], snapshot: Dict[str, Any]) -> Dict[str, Any]:
    entry = _safe_float(snapshot.get("entry"), _safe_float(setup.get("entry")))
    stop_loss = _safe_float(snapshot.get("stop_loss"), _safe_float(setup.get("stop_loss")))

    raw_targets = snapshot.get("targets")
    if raw_targets is None:
        raw_targets = setup.get("targets") or []

    if not isinstance(raw_targets, list):
        raw_targets = []

    targets = []
    for t in raw_targets:
        tv = _safe_float(t)
        if tv is not None:
            targets.append(tv)

    return {
        "entry": entry,
        "stop_loss": stop_loss,
        "targets": targets,
    }


def _build_fallback_trade_plan(
    *,
    symbol: str,
    action: str,
    levels: Dict[str, Any],
    reason: str,
) -> Dict[str, Any]:
    entry = levels.get("entry")
    stop = levels.get("stop_loss")
    targets = levels.get("targets") or []

    entry_plan = []
    if entry is not None:
        entry_plan.append({
            "type": "reference",
            "label": "Primary entry",
            "price": entry,
        })

    formatted_targets = []
    for i, t in enumerate(targets):
        formatted_targets.append({
            "label": f"TP{i+1}",
            "price": t,
        })

    return {
        "symbol": (symbol or "BTC").upper(),
        "side": (action or "hold").lower(),
        "entry_plan": entry_plan,
        "stop_loss": {"price": stop},
        "targets": formatted_targets,
        "position": {"units": None},
        "risk": {
            "risk_per_unit": None,
            "reward_per_unit": None,
            "risk_eur": None,
            "rr": None,
            "regime": None,
            "note": reason,
        },
    }


def _build_action_decision(
    *,
    setup_type: str,
    snapshot: Dict[str, Any],
    levels: Dict[str, Any],
    normalized_scores: Dict[str, float],
    market_pressure: float,
    transition_risk: float,
    rules: Dict[str, float],
    live_price: Optional[float],
    final_amount: float,
) -> Dict[str, Any]:
    confidence_score = _safe_float(snapshot.get("confidence_score"), 0.0) or 0.0
    market_score = _safe_float(normalized_scores.get("market_score"), 10.0) or 10.0
    technical_score = _safe_float(normalized_scores.get("technical_score"), 10.0) or 10.0

    entry = levels.get("entry")
    stop_loss = levels.get("stop_loss")
    targets = levels.get("targets") or []

    min_confidence_to_buy = _safe_float(rules.get("min_confidence_to_buy"), 60.0) or 60.0
    min_confidence_to_watch = _safe_float(rules.get("min_confidence_to_watch"), 40.0) or 40.0
    min_market_score_to_buy = _safe_float(rules.get("min_market_score_to_buy"), 55.0) or 55.0
    min_market_pressure_to_buy = _safe_float(rules.get("min_market_pressure_to_buy"), 0.52) or 0.52
    max_transition_risk_to_buy = _safe_float(rules.get("max_transition_risk_to_buy"), 0.60) or 0.60
    entry_tolerance_pct = _safe_float(rules.get("entry_tolerance_pct"), 0.01) or 0.01
    chase_limit_pct = _safe_float(rules.get("chase_limit_pct"), 0.03) or 0.03

    if final_amount <= 0:
        return {
            "action": "hold",
            "reason": "No executable size from position engine",
            "intent_note": "Position sizing returned zero",
            "confidence_score": confidence_score,
        }

    if confidence_score < min_confidence_to_watch:
        return {
            "action": "hold",
            "reason": f"Confidence too low ({confidence_score:.1f})",
            "intent_note": "Snapshot confidence below watch threshold",
            "confidence_score": confidence_score,
        }

    if market_score < min_market_score_to_buy:
        return {
            "action": "hold",
            "reason": f"Market score too weak ({market_score:.1f})",
            "intent_note": "Market score below buy threshold",
            "confidence_score": confidence_score,
        }

    if market_pressure < min_market_pressure_to_buy:
        return {
            "action": "hold",
            "reason": f"Market pressure too weak ({market_pressure:.2f})",
            "intent_note": "Insufficient market participation",
            "confidence_score": confidence_score,
        }

    if transition_risk > max_transition_risk_to_buy:
        return {
            "action": "hold",
            "reason": f"Transition risk too high ({transition_risk:.2f})",
            "intent_note": "Avoid unstable market state",
            "confidence_score": confidence_score,
        }

    if setup_type == "dca":
        if confidence_score >= min_confidence_to_buy:
            return {
                "action": "buy",
                "reason": f"DCA strategy active with strong confidence ({confidence_score:.1f})",
                "intent_note": "Scheduled DCA selected as active strategy",
                "confidence_score": confidence_score,
            }

        return {
            "action": "hold",
            "reason": f"DCA active but confidence not strong enough ({confidence_score:.1f})",
            "intent_note": "Wait for cleaner conditions",
            "confidence_score": confidence_score,
        }

    if setup_type == "trade":
        if entry is None or stop_loss is None or not targets:
            return {
                "action": "hold",
                "reason": "Trade strategy missing entry/stop/targets",
                "intent_note": "Snapshot levels incomplete",
                "confidence_score": confidence_score,
            }

        if live_price is None:
            return {
                "action": "observe",
                "reason": "Trade setup ready, waiting for live price",
                "intent_note": "No live price available",
                "confidence_score": confidence_score,
            }

        upper_buy_zone = entry * (1.0 + entry_tolerance_pct)
        chase_limit = entry * (1.0 + chase_limit_pct)

        if live_price < entry:
            return {
                "action": "observe",
                "reason": f"Trade setup valid, waiting for entry zone ({entry:.2f})",
                "intent_note": f"Live price {live_price:.2f} still below entry",
                "confidence_score": confidence_score,
            }

        if entry <= live_price <= upper_buy_zone and confidence_score >= min_confidence_to_buy:
            return {
                "action": "buy",
                "reason": f"Trade setup confirmed near entry ({live_price:.2f} vs {entry:.2f})",
                "intent_note": f"Technical={technical_score:.1f}, market={market_score:.1f}",
                "confidence_score": confidence_score,
            }

        if upper_buy_zone < live_price <= chase_limit:
            return {
                "action": "observe",
                "reason": f"Price extended above entry but still near zone ({live_price:.2f})",
                "intent_note": "Wait for controlled pullback or renewed confirmation",
                "confidence_score": confidence_score,
            }

        return {
            "action": "hold",
            "reason": f"Price too extended above entry ({live_price:.2f})",
            "intent_note": "Do not chase trade",
            "confidence_score": confidence_score,
        }

    return {
        "action": "hold",
        "reason": f"Unknown setup type: {setup_type}",
        "intent_note": "No valid execution model",
        "confidence_score": confidence_score,
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
    portfolio_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    portfolio_context = portfolio_context or {}
    setup = setup or {}
    scores = scores or {}

    rules = {**DEFAULT_ACTION_RULES, **(action_rules or {})}
    normalized_scores = _normalize_scores(scores)

    snapshot = _extract_snapshot(portfolio_context)
    setup_type = _normalize_setup_type(setup, snapshot)

    # -------------------------------------------------
    # 1️⃣ Regime Memory
    # -------------------------------------------------
    regime_memory = None
    regime_label = None
    regime_confidence = None

    try:
        regime_memory = get_regime_memory(user_id)
        if isinstance(regime_memory, dict):
            regime_label = regime_memory.get("regime_label") or regime_memory.get("label")
            regime_confidence = _safe_float(regime_memory.get("confidence"))
    except Exception as e:
        logger.warning("Regime memory unavailable: %s", e)

    # -------------------------------------------------
    # 2️⃣ Market Intelligence
    # -------------------------------------------------
    market_intelligence = get_market_intelligence(
        user_id=user_id,
        scores=normalized_scores,
    )
    if not market_intelligence:
        raise RuntimeError("market_intelligence_empty")

    trend_block = market_intelligence.get("trend") or {}
    state_block = market_intelligence.get("state") or {}
    metrics_block = market_intelligence.get("metrics") or {}

    market_cycle = market_intelligence.get("cycle")
    temperature = market_intelligence.get("temperature")

    short_trend = trend_block.get("short")
    mid_trend = trend_block.get("mid")
    long_trend = trend_block.get("long")

    volatility_state = state_block.get("volatility_state")
    structure_bias = state_block.get("structure_bias")
    risk_environment = _safe_float(state_block.get("risk_environment"), 0.5) or 0.5
    trend_strength = _safe_float(state_block.get("trend_strength"), 0.5) or 0.5
    market_pressure = _safe_float(state_block.get("market_pressure"), 0.5) or 0.5
    transition_risk = _safe_float(state_block.get("transition_risk"), 0.5) or 0.5

    # -------------------------------------------------
    # 3️⃣ Position Engine
    # -------------------------------------------------
    try:
        position = calculate_position(
            setup=setup,
            scores=normalized_scores,
            regime_memory=regime_memory,
            transition_risk=transition_risk,
        )

        base_amount = _safe_float(position.get("base_amount"), 0.0) or 0.0
        suggested_amount = _safe_float(position.get("final_amount"), 0.0) or 0.0
        position_size = _safe_float(position.get("position_size"), 0.0) or 0.0
        exposure_multiplier = _safe_float(position.get("exposure_multiplier"), 1.0) or 1.0
        decision_result = position.get("raw") or {}
        base_reason = decision_result.get("setup_reason") or "Position engine result"

        logger.info(
            "📊 PositionEngine result | setup_type=%s | base=%.2f final=%.2f size=%.3f exposure=%.2f",
            setup_type or "unknown",
            base_amount,
            suggested_amount,
            position_size,
            exposure_multiplier,
        )

    except Exception as e:
        logger.warning("PositionEngine fallback triggered: %s", e)
        base_amount = 0.0
        suggested_amount = 0.0
        position_size = 0.0
        exposure_multiplier = 1.0
        decision_result = {}
        base_reason = f"PositionEngine fallback: {e}"

    # -------------------------------------------------
    # 4️⃣ Levels from snapshot (source of truth)
    # -------------------------------------------------
    levels = _extract_levels(setup, snapshot)
    entry_value = levels["entry"]
    stop_value = levels["stop_loss"]
    clean_targets = levels["targets"]

    live_price = _safe_float(
        portfolio_context.get("live_price")
        or portfolio_context.get("current_price")
    )

    monitoring = (
        entry_value is not None
        or stop_value is not None
        or bool(clean_targets)
    )
    alerts_active = monitoring

    # -------------------------------------------------
    # 5️⃣ Risk State
    # -------------------------------------------------
    risk_state = _classify_risk_state(transition_risk, market_pressure)

    # -------------------------------------------------
    # 6️⃣ Action Decision
    # -------------------------------------------------
    setup_result = _build_action_decision(
        setup_type=setup_type,
        snapshot=snapshot,
        levels=levels,
        normalized_scores=normalized_scores,
        market_pressure=market_pressure,
        transition_risk=transition_risk,
        rules=rules,
        live_price=live_price,
        final_amount=suggested_amount,
    )

    action = setup_result.get("action", "hold")
    strategy_reason = setup_result.get("reason", "No setup reason")
    setup_intent_note = setup_result.get("intent_note", "")
    confidence_score = _safe_float(setup_result.get("confidence_score"), 0.0) or 0.0

    # snapshot confidence blijft leading, maar fallback op regime/market als leeg
    if confidence_score <= 0:
        confidence_parts = []
        if isinstance(regime_confidence, (int, float)):
            rc = float(regime_confidence)
            if rc > 1:
                rc = rc / 100.0
            confidence_parts.append(_clamp(rc, 0.0, 1.0))

        confidence_parts.append(_clamp(1.0 - transition_risk, 0.0, 1.0))
        confidence_parts.append(_clamp(market_pressure, 0.0, 1.0))

        confidence = round(sum(confidence_parts) / max(1, len(confidence_parts)), 3)
    else:
        confidence = round(_clamp(confidence_score / 100.0, 0.0, 1.0), 3)

    # -------------------------------------------------
    # 7️⃣ Guardrails
    # -------------------------------------------------
    try:
        proposed_amount = suggested_amount if action == "buy" else 0.0

        guardrails_result = apply_guardrails(
            proposed_amount_eur=proposed_amount,
            portfolio_value_eur=_safe_float(portfolio_context.get("portfolio_value_eur"), 0.0) or 0.0,
            current_asset_value_eur=_safe_float(portfolio_context.get("current_asset_value_eur"), 0.0) or 0.0,
            today_allocated_eur=_safe_float(portfolio_context.get("today_allocated_eur"), 0.0) or 0.0,
            kill_switch=portfolio_context.get("kill_switch", True),
            max_trade_risk_eur=_safe_float(
                portfolio_context.get("max_trade_risk_eur")
                or setup.get("max_risk_per_trade"),
                None,
            ),
            daily_allocation_eur=_safe_float(portfolio_context.get("daily_allocation_eur"), None),
            max_asset_exposure_pct=_safe_float(portfolio_context.get("max_asset_exposure_pct"), None),
            total_budget_eur=_safe_float(portfolio_context.get("total_budget_eur"), None),
        )

    except Exception as e:
        logger.warning("Guardrails fallback triggered: %s", e)
        guardrails_result = {
            "allowed": suggested_amount > 0,
            "adjusted_amount_eur": round(float(suggested_amount), 2),
            "original_amount_eur": round(float(suggested_amount), 2),
            "warnings": [],
            "blocked_by": None,
            "reason": None,
            "debug_code": "guardrails_fallback",
            "guardrails": {},
        }

    adjusted_amount = _safe_float(
        guardrails_result.get("adjusted_amount_eur"),
        suggested_amount,
    ) or 0.0

    guardrail_reason = (
        guardrails_result.get("blocked_by")
        or guardrails_result.get("reason")
        or (
            guardrails_result.get("warnings")[0]
            if guardrails_result.get("warnings")
            else None
        )
    )

    if action == "buy" and adjusted_amount <= 0:
        action = "hold"
        if guardrail_reason:
            strategy_reason = f"{strategy_reason} Blocked by guardrails: {guardrail_reason}"
        else:
            strategy_reason = f"{strategy_reason} Blocked by guardrails."

    # -------------------------------------------------
    # 8️⃣ Position size split
    # -------------------------------------------------
    intent_position_size = round(_clamp(position_size, 0.0, EXPOSURE_CAP), 3)

    execution_position_size = 0.0
    if base_amount > 0:
        execution_position_size = round(
            _clamp(adjusted_amount / base_amount, 0.0, EXPOSURE_CAP),
            3,
        )

    # -------------------------------------------------
    # 9️⃣ Trade Quality
    # -------------------------------------------------
    trade_quality = round(
        (
            risk_environment * 0.4
            + trend_strength * 0.3
            + (float(normalized_scores.get("setup_score", 10)) / 100.0) * 0.3
        ) * 100.0,
        1,
    )

    # -------------------------------------------------
    # 🔟 Trade Plan
    # -------------------------------------------------
    
    snapshot_payload = {
        "entry": entry_value,
        "stop_loss": stop_value,
        "targets": clean_targets,
    }
    
    decision_payload = {
        "action": action,
        "symbol": (
            setup.get("symbol")
            or snapshot.get("symbol")
            or portfolio_context.get("symbol")
            or "BTC"
        ),
        "live_price": live_price,
    }
    
    bot_payload = {
        "min_rr": _safe_float(setup.get("min_rr"), 1.5) or 1.5,
        "max_risk_per_trade": _safe_float(
            portfolio_context.get("max_trade_risk_eur")
            or setup.get("max_risk_per_trade"),
            None,
        ),
        "strategy_type": setup_type,
    }
    
    brain_context = {
        "regime": regime_label,
        "reason": strategy_reason,
    }
    
    try:
        trade_plan = build_trade_plan(
            snapshot=snapshot_payload,
            brain=brain_context,
            decision=decision_payload,
            bot=bot_payload,
        )
    except Exception as e:
        logger.warning("Trade plan engine error: %s", e)
        trade_plan = None
    
    # -------------------------------------------------
    # Final output
    # -------------------------------------------------
    return {
        "date": date.today().isoformat(),
        "action": action,
        "amount_eur": round(float(adjusted_amount), 2),
        "confidence": confidence,
        "reason": strategy_reason,

        "setup_type": setup_type,
        "setup_intent_note": setup_intent_note,

        "regime": regime_label,
        "cycle": market_cycle,
        "temperature": temperature,

        "trend": {
            "short": short_trend,
            "mid": mid_trend,
            "long": long_trend,
        },

        "market_pressure": round(_clamp(market_pressure, 0.0, 1.0), 4),
        "transition_risk": round(_clamp(transition_risk, 0.0, 1.0), 4),
        "volatility_state": volatility_state,
        "trend_strength": round(_clamp(trend_strength, 0.0, 1.0), 4),
        "structure_bias": structure_bias,
        "risk_environment": risk_environment,
        "risk_state": risk_state,

        "base_amount": round(float(base_amount), 2),
        "exposure_multiplier": exposure_multiplier,

        "position_size": intent_position_size,
        "execution_position_size": execution_position_size,

        "trade_quality": trade_quality,

        "watch_levels": levels,
        "monitoring": monitoring,
        "alerts_active": alerts_active,

        "guardrails_result": guardrails_result,
        "guardrail_reason": guardrail_reason,

        "trade_plan": trade_plan,

        "metrics": {
            **metrics_block,
            "position_size": intent_position_size,
            "execution_position_size": execution_position_size,
        },

        "debug": {
            "scores": normalized_scores,
            "market_intelligence": market_intelligence,
            "regime_memory": regime_memory,
            "decision_result": decision_result,
            "base_amount": base_amount,
            "suggested_amount": suggested_amount,
            "adjusted_amount": adjusted_amount,
            "base_reason": base_reason,
            "levels": levels,
            "guardrails_result": guardrails_result,
            "intent_position_size": intent_position_size,
            "execution_position_size": execution_position_size,
            "setup_type": setup_type,
            "setup_intent_note": setup_intent_note,
            "snapshot": snapshot,
            "setup_result": setup_result,
        },
    }
