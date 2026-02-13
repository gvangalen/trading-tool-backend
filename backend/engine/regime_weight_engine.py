from typing import Dict, List, Optional
import copy


class RegimeWeightEngineError(Exception):
    pass


# =====================================================
# Regime aliases (🔥 voorkomt silent mismatches)
# =====================================================

REGIME_ALIASES = {
    "risk-off": "risk_off",
    "risk off": "risk_off",
    "bear": "risk_off",

    "risk-on": "risk_on",
    "risk on": "risk_on",
    "bull": "risk_on",

    "sideways": "range",
    "chop": "range",

    "distribution_phase": "distribution",
    "early_distribution": "distribution",

    "accumulation_phase": "accumulation",
}


# =====================================================
# Default regime map
# =====================================================

DEFAULT_REGIME_WEIGHTS: Dict[str, Dict[str, float]] = {

    "risk_off": {
        "market_score": 0.8,
        "technical_score": 1.2,
        "macro_score": 1.3,
        "sentiment_score": 0.7,
        "volatility_score": 1.2,
    },

    "risk_on": {
        "market_score": 1.3,
        "technical_score": 1.1,
        "macro_score": 0.9,
        "sentiment_score": 1.2,
        "volatility_score": 0.8,
    },

    "range": {
        "market_score": 0.9,
        "technical_score": 1.3,
        "macro_score": 1.0,
        "sentiment_score": 0.9,
        "volatility_score": 1.1,
    },

    "distribution": {
        "market_score": 0.8,
        "technical_score": 1.2,
        "macro_score": 1.25,
        "sentiment_score": 0.8,
        "volatility_score": 1.15,
    },

    "accumulation": {
        "market_score": 1.15,
        "technical_score": 1.2,
        "macro_score": 1.0,
        "sentiment_score": 0.95,
        "volatility_score": 1.0,
    },

    "neutral": {},
}


# =====================================================
# Helpers
# =====================================================

def _normalize_key(k: Optional[str]) -> str:
    if not k:
        return "neutral"

    key = k.strip().lower().replace(" ", "_")

    # 🔥 alias mapping
    return REGIME_ALIASES.get(key, key)


def _safe_float(x, fallback: float = 1.0) -> float:
    try:
        v = float(x)
        if v <= 0:
            return fallback
        return v
    except Exception:
        return fallback


# =====================================================
# Main API
# =====================================================

def apply_regime_weights(
    curves: List[Dict],
    regime_label: str,
    *,
    regime_weight_map: Optional[Dict[str, Dict[str, float]]] = None,
    min_weight: float = 0.25,
    max_weight: float = 2.5,
) -> List[Dict]:

    if not isinstance(curves, list) or not curves:
        return curves or []

    label = _normalize_key(regime_label)

    weight_map = regime_weight_map or DEFAULT_REGIME_WEIGHTS
    multipliers = weight_map.get(label) or weight_map.get("neutral") or {}

    # Unknown regime → unchanged
    if not multipliers:
        return curves

    adjusted = copy.deepcopy(curves)

    for row in adjusted:

        curve = row.get("curve") or {}
        input_key = _normalize_key(curve.get("input"))

        if not input_key:
            continue

        base_w = _safe_float(row.get("weight", 1.0), fallback=1.0)
        regime_mult = _safe_float(multipliers.get(input_key, 1.0), fallback=1.0)

        new_w = base_w * regime_mult
        new_w = max(min_weight, min(new_w, max_weight))

        row["weight"] = round(float(new_w), 6)

    return adjusted
