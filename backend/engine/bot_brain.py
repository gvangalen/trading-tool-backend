from __future__ import annotations

import logging
from datetime import date
from typing import Any, Dict, Optional

from backend.engine.position_engine import calculate_position
from backend.engine.market_intelligence_engine import get_market_intelligence
from backend.engine.guardrails_engine import apply_guardrails
from backend.engine.trade_plan_engine import build_trade_plan
from backend.ai_core.regime_memory import get_regime_memory
from backend.engine.setup_engine import run_setup_logic

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

# 🔥 FIX — multiplier mag NOOIT boven 1.0
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
    - dict met final_amount / sized_amount / base_amount / amount_eur / amount
    """

    if isinstance(decision_result, dict):
        amount = _safe_float(
            decision_result.get("final_amount")
            or decision_result.get("sized_amount")
            or decision_result.get("base_amount")
            or decision_result.get("amount_eur")
            or decision_result.get("amount"),
            0.0,
        ) or 0.0

        reason = str(
            decision_result.get("reason")
            or decision_result.get("message")
            or decision_result.get("exposure_reason")
            or "Base amount via DecisionEngine dict."
        )

        logger.info(
            "💵 Extracted decision amount | final=%s sized=%s base=%s resolved=%s",
            decision_result.get("final_amount"),
            decision_result.get("sized_amount"),
            decision_result.get("base_amount"),
            amount,
        )

        return max(0.0, amount), reason

    amount = _safe_float(decision_result, 0.0) or 0.0

    logger.info("💵 Extracted scalar decision amount | resolved=%s", amount)

    return max(0.0, amount), "Base amount via DecisionEngine."


def _default_trade_plan(
    symbol: str,
    action: str,
    reason: str = "watch_mode",
    watch_levels: Optional[dict] = None,
) -> dict:
    symbol = (symbol or "BTC").upper()
    side = (action or "observe").lower()

    entry_plan = []
    targets = []
    stop_loss = {"price": None}

    if watch_levels:
        pullback = watch_levels.get("pullback_zone")
        breakout = watch_levels.get("breakout_trigger")
        entry = watch_levels.get("entry")

        if pullback is not None:
            entry_plan.append(
                {
                    "type": "watch",
                    "label": "Observe pullback zone",
                    "price": pullback,
                }
            )

        if breakout is not None:
            entry_plan.append(
                {
                    "type": "watch",
                    "label": "Watch breakout",
                    "price": breakout,
                }
            )

        if entry is not None and not entry_plan:
            entry_plan.append(
                {
                    "type": "watch",
                    "label": "Potential entry",
                    "price": entry,
                }
            )

        stop = watch_levels.get("stop_loss")
        if stop is not None:
            stop_loss = {"price": stop}

        for i, t in enumerate(watch_levels.get("targets") or []):
            if t is not None:
                targets.append(
                    {
                        "label": f"TP{i+1}",
                        "price": t,
                    }
                )

    return {
        "symbol": symbol,
        "side": side,
        "entry_plan": entry_plan,
        "stop_loss": stop_loss,
        "targets": targets,
        "risk": {
            "rr": None,
            "risk_eur": None,
        },
        "notes": [reason],
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

    # -------------------------------------------------
    # Setup / Strategy type normalisatie
    # -------------------------------------------------
    raw_setup_type = setup.get("setup_type")
    raw_strategy_type = setup.get("strategy_type")
    
    if raw_setup_type:
        setup_type = str(raw_setup_type).lower().strip()
    elif raw_strategy_type:
        setup_type = str(raw_strategy_type).lower().strip()
    else:
        setup_type = "unknown"
    
    # backward compat
    strategy_type = setup_type

    # -------------------------------------------------
    # 1️⃣ Regime Memory
    # -------------------------------------------------
    regime_memory = None
    regime_label = None
    regime_confidence = None

    try:
        regime_memory = get_regime_memory(user_id)

        if isinstance(regime_memory, dict):
            regime_label = (
                regime_memory.get("regime_label")
                or regime_memory.get("label")
            )
            regime_confidence = regime_memory.get("confidence")

    except Exception as e:
        logger.warning("Regime memory unavailable: %s", e)

    # -------------------------------------------------
    # 2️⃣ Market Intelligence (single source)
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
    # 3️⃣ Position Engine (OOK VOOR DCA)
    # -------------------------------------------------
    try:
        position = calculate_position(
            setup=setup,
            scores=normalized_scores,
            regime_memory=regime_memory,
            transition_risk=transition_risk,
        )

        base_amount = _safe_float(position.get("base_amount"), 0.0) or 0.0
        final_amount = _safe_float(position.get("final_amount"), 0.0) or 0.0
        position_size = _safe_float(position.get("position_size"), 0.0) or 0.0
        exposure_multiplier = _safe_float(position.get("exposure_multiplier"), 1.0) or 1.0

        decision_result = position.get("raw") or {}
        base_reason = decision_result.get("setup_reason") or "Position engine result"

        logger.info(
            "📊 PositionEngine result | strategy=%s | base=%.2f final=%.2f size=%.3f exposure=%.2f",
            strategy_type or "unknown",
            base_amount,
            final_amount,
            position_size,
            exposure_multiplier,
        )

    except Exception as e:
        logger.warning("PositionEngine fallback triggered: %s", e)

        base_amount = 0.0
        final_amount = 0.0
        position_size = 0.0
        exposure_multiplier = 1.0
        decision_result = {}
        base_reason = f"PositionEngine fallback: {e}"

    # -------------------------------------------------
    # 4️⃣ Watch levels
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
    # 5️⃣ Risk State
    # -------------------------------------------------
    risk_state = _classify_risk_state(transition_risk, market_pressure)

    # -------------------------------------------------
    # 6️⃣ Setup Engine (NEW CORE LOGIC)
    # -------------------------------------------------
    
    setup_result = run_setup_logic(
        setup=setup,
        scores=normalized_scores,
        market_pressure=market_pressure,
        transition_risk=transition_risk,
        trend_strength=trend_strength,
        risk_environment=risk_environment,
        final_amount=final_amount,
        watch_levels=watch_levels,
        portfolio_context=portfolio_context,
    )
    
    action = setup_result.get("action", "hold")
    strategy_reason = setup_result.get("reason", "No setup reason")
    setup_type = setup_result.get("setup_type")
    setup_intent_note = setup_result.get("intent_note")

    # -------------------------------------------------
    # 7️⃣ Guardrails (ALTIJD!)
    # -------------------------------------------------
    try:
        guardrails_result = apply_guardrails(
            proposed_amount_eur=final_amount,
            portfolio_value_eur=_safe_float(
                portfolio_context.get("portfolio_value_eur"),
                0.0,
            ) or 0.0,
            current_asset_value_eur=_safe_float(
                portfolio_context.get("current_asset_value_eur"),
                0.0,
            ) or 0.0,
            today_allocated_eur=_safe_float(
                portfolio_context.get("today_allocated_eur"),
                0.0,
            ) or 0.0,
            kill_switch=portfolio_context.get("kill_switch", True),
            max_trade_risk_eur=_safe_float(
                portfolio_context.get("max_trade_risk_eur")
                or setup.get("max_risk_per_trade"),
                None,
            ),
            daily_allocation_eur=_safe_float(
                portfolio_context.get("daily_allocation_eur"),
                None,
            ),
            max_asset_exposure_pct=_safe_float(
                portfolio_context.get("max_asset_exposure_pct"),
                None,
            ),
            total_budget_eur=_safe_float(
                portfolio_context.get("total_budget_eur"),
                None,
            ),
        )

    except Exception as e:
        logger.warning("Guardrails fallback triggered: %s", e)
        guardrails_result = {
            "allowed": final_amount > 0,
            "adjusted_amount_eur": round(float(final_amount), 2),
            "original_amount_eur": round(float(final_amount), 2),
            "warnings": [],
            "blocked_by": None,
            "reason": None,
            "debug_code": "guardrails_fallback",
            "guardrails": {},
        }

    adjusted_amount = _safe_float(
        guardrails_result.get("adjusted_amount_eur"),
        final_amount,
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

    if adjusted_amount <= 0:
        action = "hold"
        if guardrail_reason:
            strategy_reason = f"{strategy_reason} Blocked by guardrails: {guardrail_reason}"
        else:
            strategy_reason = f"{strategy_reason} Blocked by guardrails."

    # -------------------------------------------------
    # 8️⃣ Position size split
    # -------------------------------------------------
    # 🔥 BELANGRIJK:
    # position_size = engine intent (voor UI / market suggestion)
    # execution_position_size = na guardrails
    intent_position_size = round(_clamp(position_size, 0.0, 1.0), 3)

    execution_position_size = 0.0
    if base_amount > 0:
        execution_position_size = round(
            _clamp(adjusted_amount / base_amount, 0.0, 1.0),
            3,
        )

    # -------------------------------------------------
    # 9️⃣ Confidence
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
    # 🔟 Trade Quality
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
    # 11️⃣ Trade Plan Engine
    # -------------------------------------------------
    if setup_type and setup_type.startswith("dca"):
        snapshot_payload = {
            "entry": entry_value,
            "stop_loss": None,
            "targets": [],
        }
    else:
        snapshot_payload = {
            "entry": entry_value,
            "stop_loss": stop_value,
            "targets": clean_targets,
        }

    decision_payload = {
        "action": action,
        "symbol": setup.get("symbol"),
        "live_price": portfolio_context.get("live_price"),
    }

    bot_payload = {
        "min_rr": _safe_float(setup.get("min_rr"), 1.5) or 1.5,
        "max_risk_per_trade": _safe_float(
            portfolio_context.get("max_trade_risk_eur")
            or setup.get("max_risk_per_trade"),
            None,
        ),
        "strategy_type": strategy_type,
    }

    brain_context = {
        "regime": regime_label,
        "reason": strategy_reason,
    }

    trade_plan = None

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

    if not trade_plan:
        trade_plan = {
            "symbol": setup.get("symbol", "BTC"),
            "side": action,
            "entry_plan": [],
            "stop_loss": {"price": None},
            "targets": [],
            "risk": {"rr": None, "risk_eur": None},
            "notes": ["fallback"],
        }

    # -------------------------------------------------
    # Final output
    # -------------------------------------------------
    return {
        "date": date.today().isoformat(),
        "action": action,
        "amount_eur": round(float(adjusted_amount), 2),
        "confidence": confidence,
        "reason": strategy_reason,
    
        # 🔥 NIEUW — setup info (BELANGRIJK)
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
    
        # 🔥 UI moet deze gebruiken
        "position_size": intent_position_size,
    
        # 🔥 execution na guardrails
        "execution_position_size": execution_position_size,
    
        "trade_quality": trade_quality,
    
        "watch_levels": watch_levels,
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
            "final_amount": final_amount,
            "adjusted_amount": adjusted_amount,
            "base_reason": base_reason,
            "watch_levels": watch_levels,
            "guardrails_result": guardrails_result,
            "intent_position_size": intent_position_size,
            "execution_position_size": execution_position_size,
    
            # 🔥 EXTRA DEBUG (SUPER HANDIG)
            "setup_type": setup_type,
            "setup_intent_note": setup_intent_note,
        },
    }
