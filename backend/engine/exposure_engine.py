from typing import Dict, Any


# =====================================================
# Exceptions
# =====================================================

class ExposureEngineError(Exception):
    pass


# =====================================================
# Hard Risk Guards (DO NOT REMOVE)
# =====================================================

MIN_EXPOSURE = 0.0      # bot mag volledig risk-off gaan
MAX_EXPOSURE = 2.0      # voorkomt hidden leverage
DEFAULT_EXPOSURE = 1.0


# =====================================================
# Regime → Base Exposure Map
# (Institutionele logica)
# =====================================================

REGIME_EXPOSURE_MAP = {
    # Accumulatie → agressiever kopen toegestaan
    "accumulation": 1.4,

    # Bull trend → normaal risk-on
    "bull": 1.25,

    # Neutraal → baseline
    "neutral": 1.0,

    # Distributie → kapitaal beschermen
    "distribution": 0.55,

    # Bear → survival mode
    "bear": 0.35,
}


# =====================================================
# Transition Dampening
# voorkomt dat je vol gas koopt tijdens regime shift
# =====================================================

def _transition_dampener(transition_risk: float) -> float:
    """
    Zet transition risk (0-100) om naar exposure multiplier.

    Filosofie:
    - 0–20  : vrijwel geen risico
    - 20–40 : lichte voorzichtigheid
    - 40–60 : halve exposure
    - 60–80 : heavy dampening
    - 80+   : survival mode
    """

    if transition_risk is None:
        return 1.0

    try:
        risk = float(transition_risk)
    except Exception:
        return 1.0

    if risk >= 80:
        return 0.25
    if risk >= 60:
        return 0.45
    if risk >= 40:
        return 0.65
    if risk >= 20:
        return 0.85

    return 1.0


# =====================================================
# Confidence Booster
# sterk regime = iets meer size toegestaan
# =====================================================

def _confidence_booster(confidence: float) -> float:
    """
    confidence verwacht 0–1 of 0–100
    """

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
) -> Dict[str, Any]:
    """
    Core exposure allocator.

    Inputs:
        regime_memory = {
            "label": "bull",
            "confidence": 0.72
        }

        transition_risk = 0–100

    Returns:
        {
            "multiplier": 0.83,
            "risk_mode": "neutral",
            "reason": "...",
            "components": {...}
        }
    """

    # -------------------------------------------------
    # Fail-safe defaults
    # -------------------------------------------------
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
    final_exposure = round(_clamp(raw_exposure), 3)

    # -------------------------------------------------
    # Risk mode (handig voor logging / UI)
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
    """
    Laatste safety layer vóór order execution.

    - voorkomt negatieve orders
    - voorkomt nan
    - clamp exposure
    """

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
