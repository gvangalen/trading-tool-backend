from __future__ import annotations

import logging
from datetime import date
from typing import Any, Dict, Optional

from backend.engine.transition_detector import compute_transition_detector
from backend.engine.market_pressure_engine import get_market_pressure
from backend.engine.market_intelligence_engine import get_market_intelligence
from backend.engine.decision_engine import decide_amount
from backend.engine.guardrails_engine import apply_guardrails
from backend.engine.trade_plan_engine import build_trade_plan
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

    # ✅ FIX HIER
    portfolio_context = portfolio_context or {}
    setup = setup or {}
    scores = scores or {}

    rules = {**DEFAULT_ACTION_RULES, **(action_rules or {})}
    # =================================================
    # 🔥 DCA MODE (V1 SIMPEL)
    # =================================================
    
    strategy_type = str(setup.get("strategy_type") or "").lower().strip()
    
    if strategy_type == "dca":
    
        logger.info("🟢 DCA MODE ACTIVE")
    
        base_amount = float(setup.get("base_amount") or 0.0)
    
        action = "buy"
        final_amount = base_amount
    
        # guardrails blijven actief
        try:
            guardrails_result = apply_guardrails(
                proposed_amount_eur=final_amount,
                portfolio_value_eur=_safe_float(
                    portfolio_context.get("portfolio_value_eur"), 0.0
                ) or 0.0,
                current_asset_value_eur=_safe_float(
                    portfolio_context.get("current_asset_value_eur"), 0.0
                ) or 0.0,
                today_allocated_eur=_safe_float(
                    portfolio_context.get("today_allocated_eur"), 0.0
                ) or 0.0,
                kill_switch=portfolio_context.get("kill_switch", True),
                max_trade_risk_eur=_safe_float(
                    portfolio_context.get("max_trade_risk_eur"), None
                ),
                daily_allocation_eur=_safe_float(
                    portfolio_context.get("daily_allocation_eur"), None
                ),
                max_asset_exposure_pct=_safe_float(
                    portfolio_context.get("max_asset_exposure_pct"), None
                ),
            )
        except Exception:
            guardrails_result = {
                "allowed": True,
                "adjusted_amount_eur": final_amount,
                "original_amount_eur": final_amount,
                "warnings": [],
                "blocked_by": None,
            }
    
        adjusted_amount = guardrails_result.get("adjusted_amount_eur", final_amount)
    
        if adjusted_amount <= 0:
            action = "hold"
    
        return {
            "date": date.today().isoformat(),
            "action": action,
            "amount_eur": round(float(adjusted_amount), 2),
            "confidence": 0.7,
            "reason": "DCA strategy active",
    
            "trade_plan": _default_trade_plan(
                symbol=setup.get("symbol", "BTC"),
                action=action,
                reason="dca_mode",
            ),
    
            "guardrails_result": guardrails_result,
    
            "metrics": {
                "market_pressure": 50,
                "transition_risk": 50,
                "setup_quality": 50,
                "volatility": 50,
                "trend_strength": 50,
                "position_size": 50,
            },
    
            "debug": {
                "mode": "dca",
                "base_amount": base_amount,
                "adjusted_amount": adjusted_amount,
            },
        }

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
    # 2️⃣ Transition Risk (raw 0..1)
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
        transition_risk = 0.5
        transition_snapshot = None

    # -------------------------------------------------
    # 3️⃣ Market Pressure (raw 0..1)
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
    # 4️⃣ Market Intelligence Engine
    # -------------------------------------------------
    market_intelligence = {}

    try:
        market_intelligence = get_market_intelligence(
            user_id=user_id,
            scores=scores,
        )

        market_cycle = market_intelligence.get("cycle", "neutral")
        temperature = market_intelligence.get("temperature", "cool")

        trend_block = market_intelligence.get("trend", {}) or {}
        short_trend = trend_block.get("short", "trading_range")
        mid_trend = trend_block.get("mid", "trading_range")
        long_trend = trend_block.get("long", "trading_range")

        state_block = market_intelligence.get("state", {}) or {}
        volatility_state = state_block.get("volatility_state", "normal")
        structure_bias = state_block.get("structure_bias", "neutral")
        risk_environment = _safe_float(
            state_block.get("risk_environment"),
            0.5,
        ) or 0.5

        metrics_block = market_intelligence.get("metrics", {}) or {}

        trend_strength = (
            _safe_float(state_block.get("trend_strength"), None)
            if state_block.get("trend_strength") is not None
            else None
        )

        if trend_strength is None:
            trend_strength = (
                float(scores.get("technical_score", 10)) * 0.6
                + float(scores.get("market_score", 10)) * 0.4
            ) / 100.0

        trend_strength = _clamp(float(trend_strength), 0.0, 1.0)

    except Exception as e:
        logger.warning("Market intelligence fallback: %s", e)

        market_intelligence = {}

        market_cycle = "neutral"
        temperature = "cool"

        short_trend = "trading_range"
        mid_trend = "trading_range"
        long_trend = "trading_range"

        volatility_state = "normal"
        structure_bias = "neutral"
        risk_environment = 0.5

        trend_strength = (
            float(scores.get("technical_score", 10)) * 0.6
            + float(scores.get("market_score", 10)) * 0.4
        ) / 100.0
        trend_strength = _clamp(trend_strength, 0.0, 1.0)

        metrics_block = {}

    # -------------------------------------------------
    # 5️⃣ Position Sizing via Decision Engine
    # -------------------------------------------------
    try:
        decision_result = decide_amount(
            setup=setup,
            scores=scores,
            regime_memory=regime_memory,
            transition_risk=transition_risk,
        )

        logger.info("DecisionEngine raw output: %s", decision_result)

        final_amount, base_reason = _extract_decision_amount(decision_result)

        base_amount = _safe_float(
            decision_result.get("base_amount") if isinstance(decision_result, dict) else 0.0,
            0.0,
        ) or 0.0

        exposure_multiplier = _safe_float(
            decision_result.get("exposure_multiplier") if isinstance(decision_result, dict) else 1.0,
            1.0,
        ) or 1.0

    except Exception as e:
        logger.warning("DecisionEngine fallback triggered: %s", e)

        decision_result = None
        base_amount = 0.0
        final_amount = 0.0
        exposure_multiplier = 1.0
        base_reason = f"DecisionEngine fallback: {e}"

    exposure_multiplier = _clamp(exposure_multiplier, 0.0, EXPOSURE_CAP)

    # -------------------------------------------------
    # 6️⃣ Risk State
    # -------------------------------------------------
    risk_state = _classify_risk_state(transition_risk, market_pressure)

    # -------------------------------------------------
    # 7️⃣ Watch levels
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
    # 8️⃣ Action Logic
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

    strategy_reason = " ".join(reason_parts).strip() or "Engine decision"

    # -------------------------------------------------
    # 9️⃣ Guardrails Engine
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
            "guardrails": {
                "kill_switch": portfolio_context.get("kill_switch", True),
                "max_trade_risk_eur": portfolio_context.get("max_trade_risk_eur"),
                "daily_allocation_eur": portfolio_context.get("daily_allocation_eur"),
                "max_asset_exposure_pct": portfolio_context.get("max_asset_exposure_pct"),
                "current_asset_exposure_pct": 0.0,
            },
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
    # 10️⃣ Confidence
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
    # 11️⃣ Trade Quality
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
    # 12️⃣ Dashboard-ready metrics (ALTIJD 0..100)
    # -------------------------------------------------
    market_pressure_score = (
        metrics_block.get("market_pressure")
        if metrics_block.get("market_pressure") is not None
        else metrics_block.get("market_pressure_score")
    )

    transition_risk_score = (
        metrics_block.get("transition_risk")
        if metrics_block.get("transition_risk") is not None
        else metrics_block.get("transition_risk_score")
    )

    setup_quality_score = (
        metrics_block.get("setup_quality")
        if metrics_block.get("setup_quality") is not None
        else metrics_block.get("setup_quality_score")
    )

    volatility_score = (
        metrics_block.get("volatility")
        if metrics_block.get("volatility") is not None
        else metrics_block.get("volatility_score")
    )

    trend_strength_score = (
        metrics_block.get("trend_strength")
        if metrics_block.get("trend_strength") is not None
        else metrics_block.get("trend_strength_score")
    )

    if market_pressure_score is None:
        market_pressure_score = round(_clamp(market_pressure, 0.0, 1.0) * 100)

    if transition_risk_score is None:
        transition_risk_score = round(_clamp(transition_risk, 0.0, 1.0) * 100)

    if setup_quality_score is None:
        setup_quality_score = round(trade_quality)

    if volatility_score is None:
        volatility_score = 50

    if trend_strength_score is None:
        trend_strength_score = round(_clamp(trend_strength, 0.0, 1.0) * 100)

    market_pressure_score = round(_clamp(_safe_float(market_pressure_score, 0.0) or 0.0, 0.0, 100.0), 2)
    transition_risk_score = round(_clamp(_safe_float(transition_risk_score, 0.0) or 0.0, 0.0, 100.0), 2)
    setup_quality_score = round(_clamp(_safe_float(setup_quality_score, 0.0) or 0.0, 0.0, 100.0), 2)
    volatility_score = round(_clamp(_safe_float(volatility_score, 50.0) or 50.0, 0.0, 100.0), 2)
    trend_strength_score = round(_clamp(_safe_float(trend_strength_score, 0.0) or 0.0, 0.0, 100.0), 2)

    # -------------------------------------------------
    # 13️⃣ Trade Plan Engine
    # -------------------------------------------------
    snapshot_payload = {
        "entry": entry_value,
        "stop_loss": stop_value,
        "targets": clean_targets,
    }

    decision_payload = {
        "action": action,
        "symbol": setup.get("symbol"),
    }

    bot_payload = {
        "min_rr": _safe_float(setup.get("min_rr"), 1.5) or 1.5,
        "max_risk_per_trade": _safe_float(
            portfolio_context.get("max_trade_risk_eur")
            or setup.get("max_risk_per_trade"),
            None,
        ),
    }

    brain_context = {
        "regime": regime_label,
        "reason": strategy_reason,
    }

    trade_plan = None

    try:
        if action in ("buy", "short", "sell"):
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
        trade_plan = _default_trade_plan(
            symbol=setup.get("symbol", "BTC"),
            action=action,
            reason="watch_mode",
            watch_levels=watch_levels,
        )

    # -------------------------------------------------
    # Final output
    # -------------------------------------------------
    return {
        "date": date.today().isoformat(),
        "action": action,
        "amount_eur": round(float(adjusted_amount), 2),
        "confidence": confidence,
        "reason": strategy_reason,

        "regime": regime_label,
        "cycle": market_cycle,
        "temperature": temperature,

        "short_trend": short_trend,
        "mid_trend": mid_trend,
        "long_trend": long_trend,

        # raw values voor engine/debug
        "market_pressure": round(_clamp(market_pressure, 0.0, 1.0), 4),
        "transition_risk": round(_clamp(transition_risk, 0.0, 1.0), 4),
        "volatility_state": volatility_state,
        "trend_strength": round(_clamp(trend_strength, 0.0, 1.0), 4),
        "structure_bias": structure_bias,
        "risk_environment": risk_environment,
        "risk_state": risk_state,

        "base_amount": round(float(base_amount), 2),
        "exposure_multiplier": exposure_multiplier,

        "trade_quality": trade_quality,

        "watch_levels": watch_levels,
        "monitoring": monitoring,
        "alerts_active": alerts_active,

        "guardrails_result": guardrails_result,
        "guardrail_reason": guardrail_reason,

        "trade_plan": trade_plan,

        # frontend/UI moet HIERUIT lezen
        "metrics": {
            "market_pressure": market_pressure_score,
            "transition_risk": transition_risk_score,
            "setup_quality": setup_quality_score,
            "volatility": volatility_score,
            "trend_strength": trend_strength_score,
            "position_size": round(_clamp(exposure_multiplier * 100.0, 0.0, 100.0), 2),
        },

        "debug": {
            "scores": scores,
            "transition_snapshot": transition_snapshot,
            "market_intelligence": market_intelligence,
            "regime_memory": regime_memory,
            "decision_result": decision_result,
            "base_amount": base_amount,
            "final_amount": final_amount,
            "adjusted_amount": adjusted_amount,
            "base_reason": base_reason,
            "watch_levels": watch_levels,
            "guardrails_result": guardrails_result,
        },
    }
