# backend/engine/bot_brain.py
"""
BOT BRAIN (single orchestration layer)

Doel:
- 1 duidelijke flow waar ALLE engines samenkomen
- deterministic & explainable
- NOOIT crashen
- return altijd een complete beslissing dict

Flow:
scores
 -> regime (memory)
 -> transition risk
 -> market pressure
 -> exposure multiplier
 -> decision amount (base sizing via curve)
 -> apply exposure to amount (final amount)
 -> action suggestion (buy/hold) based on thresholds (optioneel)

Belangrijk:
- Dit bestand rekent zelf NIET creatief.
- Engines doen de logica; bot_brain orchestreert.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, Optional

# Engines
from backend.engine.transition_detector import (
    compute_transition_detector,
    get_transition_risk_value,  # 0..1
)
from backend.engine.market_pressure_engine import get_market_pressure  # 0..1
from backend.engine.exposure_engine import (
    compute_exposure_multiplier,
    apply_exposure_to_amount,
)
from backend.engine.decision_engine import decide_amount, DecisionEngineError

# Regime memory accessor (jullie hebben dit in report agent; hier importen als het bestaat)
# Als dit pad anders is in jouw repo, pas dit importje aan.
try:
    from backend.ai_agents.report_ai_agent import get_regime_memory  # type: ignore
except Exception:  # pragma: no cover
    get_regime_memory = None  # fallback


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# =========================================================
# Config (institutional defaults)
# =========================================================

DEFAULT_POLICY_CAPS = {
    "min": 0.0,
    "max": 2.0,
}

# Bot action thresholds (optioneel, maar handig als basis)
# Jij kunt dit later DB-driven maken per bot.
DEFAULT_ACTION_RULES = {
    # minimum score om überhaupt "buy" te overwegen
    "min_market_score_to_buy": 55.0,
    # als transition risk hoog is → altijd hold
    "max_transition_risk_to_buy": 0.60,  # 0..1
    # als market pressure te laag is → hold
    "min_market_pressure_to_buy": 0.52,  # 0..1
}


# =========================================================
# Output contract
# =========================================================

@dataclass
class BotDecision:
    decision_date: date
    action: str  # buy|sell|hold (voor nu buy/hold)
    amount_eur: float
    confidence: Optional[float]
    setup_match: Optional[str]
    reason: str
    debug: Dict[str, Any]


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


def _normalize_setup_match(setup: Optional[Dict[str, Any]]) -> Optional[str]:
    if not setup:
        return None
    # prefer name, else id
    name = setup.get("name")
    if isinstance(name, str) and name.strip():
        return name.strip()
    sid = setup.get("id")
    if sid is not None:
        return str(sid)
    return None


# =========================================================
# Core brain
# =========================================================

def run_bot_brain(
    *,
    user_id: int,
    setup: Dict[str, Any],
    scores: Dict[str, float],
    action_rules: Optional[Dict[str, float]] = None,
    policy_caps: Optional[Dict[str, float]] = None,
) -> Dict[str, Any]:
    """
    Single entrypoint voor de bot.

    Inputs:
    - user_id
    - setup: minimaal { base_amount, execution_mode, decision_curve? }
    - scores: { market_score, macro_score, technical_score, setup_score, ... }

    Returns dict:
    {
      "action": "buy"|"hold",
      "amount_eur": float,
      "confidence": float|None,
      "setup_match": str|None,
      "reason": str,
      "debug": {...}
    }
    """

    rules = {**DEFAULT_ACTION_RULES, **(action_rules or {})}
    caps = policy_caps or DEFAULT_POLICY_CAPS

    # -----------------------------
    # 1) Regime memory (best effort)
    # -----------------------------
    regime_memory = None
    if callable(get_regime_memory):
        try:
            regime_memory = get_regime_memory(user_id)  # {"label":..,"confidence":..,"signals":..,"narrative":..}
        except Exception as e:
            logger.warning("Regime memory unavailable: %s", e)
            regime_memory = None

    # -----------------------------
    # 2) Transition risk (0..1)
    # -----------------------------
    transition_risk = 0.5
    transition_snapshot = None
    try:
        transition_risk = float(get_transition_risk_value(user_id))
        transition_risk = _clamp(transition_risk, 0.0, 1.0)
        transition_snapshot = compute_transition_detector(user_id)
    except Exception as e:
        logger.warning("Transition detector fallback: %s", e)
        transition_risk = 0.5
        transition_snapshot = {
            "transition_risk": 50,
            "normalized_risk": 0.5,
            "primary_flag": "fallback",
            "signals": {},
            "narrative": "Transition risk unavailable (fallback).",
            "confidence": 0.25,
        }

    # -----------------------------
    # 3) Market pressure (0..1)
    # -----------------------------
    market_pressure = 0.5
    try:
        market_pressure = float(get_market_pressure(user_id=user_id, scores=scores))
        market_pressure = _clamp(market_pressure, 0.0, 1.0)
    except Exception as e:
        logger.warning("Market pressure fallback: %s", e)
        market_pressure = 0.5

    # -----------------------------
    # 4) Exposure multiplier (0..2)
    # -----------------------------
    exposure_pack = compute_exposure_multiplier(
        regime_memory=regime_memory,
        transition_risk=transition_risk,  # IMPORTANT: 0..1
        policy_caps=caps,
    )
    exposure_multiplier = _safe_float(exposure_pack.get("multiplier"), 1.0) or 1.0
    exposure_multiplier = _clamp(exposure_multiplier, caps.get("min", 0.0), caps.get("max", 2.0))

    # -----------------------------
    # 5) Base amount via DecisionEngine (curve sizing)
    # -----------------------------
    base_amount = 0.0
    base_reason = ""
    try:
        base_amount = float(decide_amount(setup=setup, scores=scores))
        base_amount = max(0.0, base_amount)
        base_reason = "Base amount resolved via decision curve."
    except DecisionEngineError as e:
        # hard validation errors → hold + 0
        base_amount = 0.0
        base_reason = f"DecisionEngineError: {e}"
    except Exception as e:
        base_amount = 0.0
        base_reason = f"DecisionEngine fallback: {e}"

    # -----------------------------
    # 6) Apply exposure to get final amount
    # -----------------------------
    final_amount = apply_exposure_to_amount(base_amount, exposure_multiplier)

    # -----------------------------
    # 7) Action suggestion (buy vs hold)
    # -----------------------------
    market_score = _safe_float(scores.get("market_score"), None)
    min_score = float(rules.get("min_market_score_to_buy", 55.0))
    max_tr = float(rules.get("max_transition_risk_to_buy", 0.60))
    min_mp = float(rules.get("min_market_pressure_to_buy", 0.52))

    action = "hold"
    reason_parts = []

    # hard guard: invalid base size
    if base_amount <= 0 or final_amount <= 0:
        action = "hold"
        reason_parts.append("No executable size (base or final amount is zero).")
        reason_parts.append(base_reason)
    else:
        # guards
        if transition_risk > max_tr:
            action = "hold"
            reason_parts.append(f"Transition risk too high ({transition_risk:.2f} > {max_tr:.2f}).")
        elif market_pressure < min_mp:
            action = "hold"
            reason_parts.append(f"Market pressure too low ({market_pressure:.2f} < {min_mp:.2f}).")
        elif market_score is not None and market_score < min_score:
            action = "hold"
            reason_parts.append(f"Market score below threshold ({market_score:.1f} < {min_score:.1f}).")
        else:
            action = "buy"
            reason_parts.append("Conditions met: regime/exposure ok + pressure ok + transition risk acceptable.")

    # Confidence (simple, explainable)
    # combine regime confidence (if available) with inverse transition risk
    conf = None
    rconf = None
    if isinstance(regime_memory, dict):
        rconf = _safe_float(regime_memory.get("confidence"), None)

    # normalize rconf to 0..1 if needed
    if rconf is not None:
        if rconf > 1:
            rconf = rconf / 100.0
        rconf = _clamp(rconf, 0.0, 1.0)

    # confidence heuristics
    conf_components = []
    if rconf is not None:
        conf_components.append(rconf)
    conf_components.append(1.0 - transition_risk)
    conf_components.append(market_pressure)

    if conf_components:
        conf = round(sum(conf_components) / len(conf_components), 3)

    setup_match = _normalize_setup_match(setup)

    decision = BotDecision(
        decision_date=date.today(),
        action=action,
        amount_eur=float(final_amount),
        confidence=conf,
        setup_match=setup_match,
        reason=" ".join(reason_parts).strip(),
        debug={
            "scores": scores,
            "market_score": market_score,
            "transition_risk": transition_risk,
            "transition_snapshot": transition_snapshot,
            "market_pressure": market_pressure,
            "regime_memory": regime_memory,
            "exposure": exposure_pack,
            "base_amount": base_amount,
            "final_amount": final_amount,
            "rules": rules,
            "policy_caps": caps,
            "base_reason": base_reason,
        },
    )

    return {
        "date": decision.decision_date.isoformat(),
        "action": decision.action,
        "amount_eur": decision.amount_eur,
        "confidence": decision.confidence,
        "setup_match": decision.setup_match,
        "reason": decision.reason,
        "debug": decision.debug,
    }
