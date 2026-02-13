# backend/engine/policy_engine.py

import logging
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# ============================================================
# Policy output contract
# ============================================================

@dataclass
class PolicyDecision:
    risk_mode: str  # "risk_off" | "neutral" | "risk_on" | "no_data"
    allowed_actions: List[str]  # e.g. ["buy", "hold"] or ["hold"]
    max_exposure_multiplier: float  # hard cap for exposure engine
    min_exposure_multiplier: float  # floor
    max_position_eur: Optional[float]  # optional cap per decision
    cooldown_hours: int
    notes: List[str]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# ============================================================
# Helpers
# ============================================================

def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(v, hi))


def _safe_float(v: Any, fallback: Optional[float] = None) -> Optional[float]:
    try:
        if v is None:
            return fallback
        return float(v)
    except Exception:
        return fallback


def _score(scores: Dict[str, Any], key: str) -> Optional[float]:
    return _safe_float(scores.get(key))


# ============================================================
# Core policy
# ============================================================

def evaluate_policy(
    *,
    scores: Dict[str, float],
    transition_risk: float,
    market_pressure: float,
    regime_label: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Policy Engine (rule-based)

    Inputs:
      - scores: {macro_score, market_score, technical_score, setup_score} 0..100
      - transition_risk: 0..1 (0 stable, 1 unstable)
      - market_pressure: 0..1 (0 defensive, 1 aggressive)
      - regime_label: optional string

    Output:
      - PolicyDecision dict (safe for bot + report)
    """

    # Fail-safe normalization
    transition_risk = _clamp(_safe_float(transition_risk, 0.5) or 0.5, 0.0, 1.0)
    market_pressure = _clamp(_safe_float(market_pressure, 0.5) or 0.5, 0.0, 1.0)

    mkt = _score(scores, "market_score")
    tech = _score(scores, "technical_score")
    macro = _score(scores, "macro_score")
    setup = _score(scores, "setup_score")

    notes: List[str] = []
    label = (regime_label or "").strip().lower().replace(" ", "_")

    # ------------------------------------------------------------
    # Determine baseline risk_mode
    # ------------------------------------------------------------
    if not scores:
        decision = PolicyDecision(
            risk_mode="no_data",
            allowed_actions=["hold"],
            max_exposure_multiplier=0.5,
            min_exposure_multiplier=0.05,
            max_position_eur=None,
            cooldown_hours=24,
            notes=["No scores available → default HOLD."],
        )
        return decision.to_dict()

    # Base regime from pressure
    if market_pressure >= 0.72:
        risk_mode = "risk_on"
    elif market_pressure <= 0.42:
        risk_mode = "risk_off"
    else:
        risk_mode = "neutral"

    # Transition overrides (institutional: instability dominates)
    if transition_risk >= 0.80:
        risk_mode = "risk_off"
        notes.append("Transition risk extreme → forced risk-off posture.")
    elif transition_risk >= 0.65 and risk_mode == "risk_on":
        risk_mode = "neutral"
        notes.append("Transition risk elevated → de-risk from risk-on to neutral.")

    # Regime label soft nudges (optional)
    if label in ("distribution", "risk_off"):
        risk_mode = "risk_off"
        notes.append(f"Regime label '{label}' → risk-off bias.")
    elif label in ("accumulation",) and risk_mode == "risk_off" and transition_risk < 0.55:
        risk_mode = "neutral"
        notes.append("Accumulation-like regime → relax from risk-off to neutral (if stability allows).")

    # ------------------------------------------------------------
    # Hard filters (scores)
    # ------------------------------------------------------------
    if mkt is not None and mkt <= 30:
        risk_mode = "risk_off"
        notes.append("Market score very low → risk-off.")
    if tech is not None and tech <= 30 and transition_risk > 0.55:
        risk_mode = "risk_off"
        notes.append("Weak technical + elevated transition → risk-off.")
    if macro is not None and macro <= 25:
        notes.append("Macro score very weak → cap exposure.")

    # ------------------------------------------------------------
    # Allowed actions
    # ------------------------------------------------------------
    allowed = ["hold"]

    # Base rule: never force SELL here (sell policy is bot/exchange specific)
    # We allow BUY only when risk_mode supports it.
    if risk_mode == "risk_on":
        allowed = ["buy", "hold"]
    elif risk_mode == "neutral":
        allowed = ["buy", "hold"]
    else:
        allowed = ["hold"]

    # Setup gating: if setup_score is very weak, don't allow buy (even in neutral)
    if setup is not None and setup < 40 and "buy" in allowed:
        allowed = ["hold"]
        notes.append("Setup score weak → no-buy gate.")

    # Extra gate: if transition is high, only allow buy if setup is very strong
    if transition_risk >= 0.65 and "buy" in allowed:
        if setup is None or setup < 75:
            allowed = ["hold"]
            notes.append("High transition risk → buy only allowed with strong setup (>=75).")

    # ------------------------------------------------------------
    # Exposure caps
    # ------------------------------------------------------------
    # Default caps per risk_mode
    if risk_mode == "risk_on":
        max_mult = 1.35
        min_mult = 0.10
        cooldown = 6
    elif risk_mode == "neutral":
        max_mult = 1.00
        min_mult = 0.08
        cooldown = 12
    else:  # risk_off
        max_mult = 0.55
        min_mult = 0.05
        cooldown = 24

    # Transition risk tightens caps
    if transition_risk >= 0.80:
        max_mult = min(max_mult, 0.35)
        cooldown = max(cooldown, 36)
        notes.append("Extreme transition → hard cap on exposure.")
    elif transition_risk >= 0.65:
        max_mult = min(max_mult, 0.60)
        notes.append("Elevated transition → tighter exposure cap.")

    # Macro weakness tightens further
    if macro is not None and macro <= 25:
        max_mult = min(max_mult, 0.50)
        notes.append("Macro very weak → additional exposure cap.")

    # Final clamps
    max_mult = _clamp(float(max_mult), 0.05, 2.0)
    min_mult = _clamp(float(min_mult), 0.01, max_mult)

    decision = PolicyDecision(
        risk_mode=risk_mode,
        allowed_actions=allowed,
        max_exposure_multiplier=max_mult,
        min_exposure_multiplier=min_mult,
        max_position_eur=None,  # keep None unless you want a hard EUR cap
        cooldown_hours=int(cooldown),
        notes=notes,
    )

    return decision.to_dict()
