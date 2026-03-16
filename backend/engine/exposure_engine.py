from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# =====================================================
# Exceptions
# =====================================================

class ExposureEngineError(Exception):
    pass


# =====================================================
# Hard Risk Guards
# =====================================================

MIN_EXPOSURE = 0.0
MAX_EXPOSURE = 2.0
DEFAULT_EXPOSURE = 1.0


# =====================================================
# Regime → Base Exposure Map
# =====================================================

REGIME_EXPOSURE_MAP = {
    "accumulation": 1.4,
    "risk_on": 1.25,
    "bull": 1.25,
    "neutral": 1.0,
    "range": 0.9,
    "distribution": 0.55,
    "risk_off": 0.35,
    "bear": 0.35,
}


# =====================================================
# Regime aliases
# =====================================================

REGIME_ALIASES = {
    "bull_market": "risk_on",
    "bullish": "risk_on",
    "bear_market": "risk_off",
    "bearish": "risk_off",
    "sideways": "range",
}


# =====================================================
# Clamp helper
# =====================================================

def _clamp(value: float, lo: float = MIN_EXPOSURE, hi: float = MAX_EXPOSURE) -> float:
    return max(lo, min(value, hi))


def _safe_float(v, fallback: float | None = None) -> float | None:
    try:
        if v is None:
            return fallback
        return float(v)
    except Exception:
        return fallback


def _normalize_label(label: str | None) -> str:

    if not label:
        return "neutral"

    key = str(label).strip().lower().replace(" ", "_")

    return REGIME_ALIASES.get(key, key)


# =====================================================
# Transition Dampener
# =====================================================

def _transition_dampener(transition_risk: float | None) -> float:

    risk = _safe_float(transition_risk, 0.0)

    risk = _clamp(risk, 0.0, 1.0)

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

def _confidence_booster(confidence: float | None) -> float:

    c = _safe_float(confidence, 1.0)

    if c > 1:
        c = c / 100.0

    c = _clamp(c, 0.0, 1.0)

    if c >= 0.8:
        return 1.1

    if c <= 0.4:
        return 0.9

    return 1.0


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

    # -------------------------------------------------
    # Resolve regime label
    # -------------------------------------------------

    label = (
        regime_memory.get("label")
        or regime_memory.get("regime_label")
        or "neutral"
    )

    label = _normalize_label(label)

    confidence = regime_memory.get("confidence")

    base_exposure = REGIME_EXPOSURE_MAP.get(label, DEFAULT_EXPOSURE)

    dampener = _transition_dampener(transition_risk)
    booster = _confidence_booster(confidence)

    raw_exposure = base_exposure * dampener * booster

    final_exposure = _clamp(raw_exposure)

    # -------------------------------------------------
    # POLICY CAPS
    # -------------------------------------------------

    if policy_caps:

        min_cap = _safe_float(policy_caps.get("min"), MIN_EXPOSURE)
        max_cap = _safe_float(policy_caps.get("max"), MAX_EXPOSURE)

        final_exposure = max(min_cap, min(final_exposure, max_cap))

    final_exposure = round(final_exposure, 3)

    # -------------------------------------------------
    # Risk mode classification
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
        f"Regime={label} | "
        f"base={base_exposure} | "
        f"transition_adj={dampener} | "
        f"confidence_adj={booster}"
    )

    logger.info(
        "ExposureEngine | regime=%s | multiplier=%.3f",
        label,
        final_exposure,
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
