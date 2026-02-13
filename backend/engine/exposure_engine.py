from typing import Dict, Any


# =====================================================
# Exceptions
# =====================================================

class ExposureEngineError(Exception):
    pass


# =====================================================
# Hard Risk Guards (DO NOT REMOVE)
# =====================================================

MIN_EXPOSURE = 0.0
MAX_EXPOSURE = 2.0
DEFAULT_EXPOSURE = 1.0


# =====================================================
# Regime → Base Exposure Map
# =====================================================

REGIME_EXPOSURE_MAP = {
    "accumulation": 1.4,
    "bull": 1.25,
    "neutral": 1.0,
    "distribution": 0.55,
    "bear": 0.35,
}


# =====================================================
# Transition Dampening
# EXPECTS 0–1 !!!
# =====================================================

def _transition_dampener(transition_risk: float) -> float:

    if transition_risk is None:
        return 1.0

    try:
        risk = float(transition_risk)
    except Exception:
        return 1.0

    # clamp
    risk = max(0.0, min(risk, 1.0))

    if risk >= 0.8:
        return 0.25
    if risk >= 0.6:
        return 0.45
    if risk >= 0.4:
        return 0.65
    if risk >= 0.2:
        return 0.85

    return 1.0


# =====================================================
# Confidence Booster
# =====================================================

def _confidence_booster(confidence: float) -> float:

    if confidence is None:
        return 1.0

    try:
        c = float(confidence)
    except Exception:
        return 1.0

    # normalize
    if c > 1:
        c = c / 100.0

    if c >= 0.8:
        return 1.1

    if c <= 0.4:
        return 0.9

    return 1.0


# =====================================================
# Clamp helper
# =====================================================

def _clamp(value: float) -> float:
    return max(MIN_EXPOSURE, min(value, MAX_EXPOSURE))


# =====================================================
# PUBLIC ENGINE
# =====================================================

def compute_exposure_multiplier(
    regime_memory: Dict[str, Any] | None,
    transition_risk: float | None,
    *,
    policy_caps: Dict[str, float] | None = None,
) -> Dict[str, Any]:

    if not regime_memory:
        return {
            "multiplier": DEFAULT_EXPOSURE,
            "risk_mode": "neutral",
            "reason": "Geen regime data — fallback naar baseline exposure.",
            "components": {},
        }

    label = str(regime_memory.get("label", "neutral")).lower()
    confidence = regime_memory.get("confidence")

    base_exposure = REGIME_EXPOSURE_MAP.get(label, DEFAULT_EXPOSURE)

    dampener = _transition_dampener(transition_risk)
    booster = _confidence_booster(confidence)

    raw_exposure = base_exposure * dampener * booster
    final_exposure = _clamp(raw_exposure)

    # 🔥 POLICY CAP (VERY IMPORTANT)
    if policy_caps:
        min_cap = policy_caps.get("min", MIN_EXPOSURE)
        max_cap = policy_caps.get("max", MAX_EXPOSURE)

        final_exposure = max(min_cap, min(final_exposure, max_cap))

    final_exposure = round(final_exposure, 3)

    # -------------------------------------------------
    # Risk mode
    # -------------------------------------------------
    if final_exposure <= 0.4:
        risk_mode = "risk_off"
    elif final_exposure <= 0.9:
        risk_mode = "defensive"
    elif final_exposure <= 1.2:
        risk_mode = "neutral"
    else:
        risk_mode = "risk_on"

    reason = (
        f"Regime={label} | base={base_exposure} | "
        f"transition_adj={dampener} | confidence_adj={booster}"
    )

    return {
        "multiplier": final_exposure,
        "risk_mode": risk_mode,
        "reason": reason,
        "components": {
            "base_exposure": base_exposure,
            "transition_dampener": dampener,
            "confidence_booster": booster,
        },
    }


# =====================================================
# Apply exposure to amount
# =====================================================

def apply_exposure_to_amount(
    amount: float,
    exposure_multiplier: float,
) -> float:

    try:
        amount = float(amount)
        exposure_multiplier = float(exposure_multiplier)
    except Exception:
        return 0.0

    if amount <= 0:
        return 0.0

    exposure_multiplier = _clamp(exposure_multiplier)

    final_amount = amount * exposure_multiplier

    if final_amount <= 0:
        return 0.0

    return round(final_amount, 2)
