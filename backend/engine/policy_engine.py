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
    risk_mode: str
    allowed_actions: List[str]
    max_exposure_multiplier: float
    min_exposure_multiplier: float
    max_position_eur: Optional[float]
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


def _normalize_label(label: Optional[str]) -> str:
    if not label:
        return "neutral"
    return label.strip().lower().replace(" ", "_")


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

    transition_risk = _clamp(_safe_float(transition_risk, 0.5) or 0.5, 0.0, 1.0)
    market_pressure = _clamp(_safe_float(market_pressure, 0.5) or 0.5, 0.0, 1.0)

    mkt = _score(scores, "market_score")
    tech = _score(scores, "technical_score")
    macro = _score(scores, "macro_score")
    setup = _score(scores, "setup_score")

    notes: List[str] = []
    label = _normalize_label(regime_label)

    if not scores:

        logger.warning("Policy engine: no scores provided")

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

    # ------------------------------------------------------------
    # Base risk mode via pressure
    # ------------------------------------------------------------

    if market_pressure >= 0.72:
        risk_mode = "risk_on"

    elif market_pressure <= 0.42:
        risk_mode = "risk_off"

    else:
        risk_mode = "neutral"

    # ------------------------------------------------------------
    # Transition overrides
    # ------------------------------------------------------------

    if transition_risk >= 0.80:

        risk_mode = "risk_off"
        notes.append("Transition risk extreme → forced risk-off posture.")

    elif transition_risk >= 0.65 and risk_mode == "risk_on":

        risk_mode = "neutral"
        notes.append("Transition risk elevated → de-risk from risk-on to neutral.")

    # ------------------------------------------------------------
    # Regime hints
    # ------------------------------------------------------------

    if label in ("distribution", "risk_off"):

        risk_mode = "risk_off"
        notes.append(f"Regime '{label}' → risk-off bias.")

    elif label in ("accumulation",) and risk_mode == "risk_off" and transition_risk < 0.55:

        risk_mode = "neutral"
        notes.append("Accumulation regime → relax to neutral.")

    # ------------------------------------------------------------
    # Hard score filters
    # ------------------------------------------------------------

    if mkt is not None and mkt <= 30:

        risk_mode = "risk_off"
        notes.append("Market score very low.")

    if tech is not None and tech <= 30 and transition_risk > 0.55:

        risk_mode = "risk_off"
        notes.append("Weak technical + unstable regime.")

    if macro is not None and macro <= 25:

        notes.append("Macro extremely weak.")

    # ------------------------------------------------------------
    # Allowed actions
    # ------------------------------------------------------------

    allowed = ["hold"]

    if risk_mode in ("risk_on", "neutral"):
        allowed = ["buy", "hold"]

    # Setup gate
    if setup is not None and setup < 40 and "buy" in allowed:

        allowed = ["hold"]
        notes.append("Setup score weak → buy blocked.")

    # High instability gate
    if transition_risk >= 0.65 and "buy" in allowed:

        if setup is None or setup < 75:

            allowed = ["hold"]
            notes.append("High transition risk → only buy with strong setup.")

    # ------------------------------------------------------------
    # Exposure caps
    # ------------------------------------------------------------

    if risk_mode == "risk_on":

        max_mult = 1.35
        min_mult = 0.10
        cooldown = 6

    elif risk_mode == "neutral":

        max_mult = 1.0
        min_mult = 0.08
        cooldown = 12

    else:

        max_mult = 0.55
        min_mult = 0.05
        cooldown = 24

    # Transition tightening
    if transition_risk >= 0.80:

        max_mult = min(max_mult, 0.35)
        cooldown = max(cooldown, 36)
        notes.append("Extreme transition → exposure cap.")

    elif transition_risk >= 0.65:

        max_mult = min(max_mult, 0.60)
        notes.append("Elevated transition → tighter cap.")

    # Macro weakness tightening
    if macro is not None and macro <= 25:

        max_mult = min(max_mult, 0.50)
        notes.append("Macro weakness → extra cap.")

    # Final clamps
    max_mult = _clamp(float(max_mult), 0.05, 2.0)
    min_mult = _clamp(float(min_mult), 0.01, max_mult)

    decision = PolicyDecision(
        risk_mode=risk_mode,
        allowed_actions=allowed,
        max_exposure_multiplier=max_mult,
        min_exposure_multiplier=min_mult,
        max_position_eur=None,
        cooldown_hours=int(cooldown),
        notes=notes,
    )

    logger.info(
        "Policy decision | risk_mode=%s | allowed=%s | max_mult=%.2f",
        risk_mode,
        allowed,
        max_mult,
    )

    return decision.to_dict()
