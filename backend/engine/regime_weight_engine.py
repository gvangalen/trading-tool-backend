from typing import Dict, List, Optional
import copy


class RegimeWeightEngineError(Exception):
    pass


# =====================================================
# Default regime map (kan je later DB-driven maken)
# =====================================================
DEFAULT_REGIME_WEIGHTS: Dict[str, Dict[str, float]] = {
    # Risk-off: minder gewicht op risk/momentum, meer op safety/structure
    "risk_off": {
        "market_score": 0.8,
        "technical_score": 1.2,
        "macro_score": 1.3,
        "sentiment_score": 0.7,
        "volatility_score": 1.2,
    },
    # Risk-on: momentum/market mag zwaarder
    "risk_on": {
        "market_score": 1.3,
        "technical_score": 1.1,
        "macro_score": 0.9,
        "sentiment_score": 1.2,
        "volatility_score": 0.8,
    },
    # Range / chop: techniek zwaarder, momentum minder
    "range": {
        "market_score": 0.9,
        "technical_score": 1.3,
        "macro_score": 1.0,
        "sentiment_score": 0.9,
        "volatility_score": 1.1,
    },
    # Distribution: reduce risk-taking signals, macro/structure zwaarder
    "distribution": {
        "market_score": 0.8,
        "technical_score": 1.2,
        "macro_score": 1.25,
        "sentiment_score": 0.8,
        "volatility_score": 1.15,
    },
    # Accumulation: market+technical zwaarder, macro neutraal
    "accumulation": {
        "market_score": 1.15,
        "technical_score": 1.2,
        "macro_score": 1.0,
        "sentiment_score": 0.95,
        "volatility_score": 1.0,
    },
    # Unknown / default
    "neutral": {},
}


# =====================================================
# Helpers
# =====================================================
def _normalize_key(k: Optional[str]) -> str:
    return (k or "").strip().lower().replace(" ", "_")


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
    """
    Past regime-gewichten toe op je curve rows.

    Input contract (curves):
    [
      {"curve": {... "input": "market_score" ...}, "weight": 1.0},
      {"curve": {... "input": "macro_score" ...}, "weight": 1.0},
    ]

    Output:
    - Zelfde structuur terug
    - weight wordt aangepast: weight * regime_multiplier
    - Clamp: min_weight..max_weight

    Fail-soft:
    - Onbekend regime -> curves ongewijzigd
    - Missing input key -> unchanged row
    """

    if not isinstance(curves, list) or not curves:
        return curves or []

    label = _normalize_key(regime_label)

    weight_map = regime_weight_map or DEFAULT_REGIME_WEIGHTS
    multipliers = weight_map.get(label) or weight_map.get("neutral") or {}

    # Unknown regime → unchanged
    if not multipliers:
        return curves

    # Deepcopy zodat caller nooit side-effects krijgt
    adjusted = copy.deepcopy(curves)

    for row in adjusted:
        curve = row.get("curve") or {}
        input_key = _normalize_key(curve.get("input"))

        # skip if no input
        if not input_key:
            continue

        base_w = _safe_float(row.get("weight", 1.0), fallback=1.0)
        regime_mult = _safe_float(multipliers.get(input_key, 1.0), fallback=1.0)

        new_w = base_w * regime_mult
        new_w = max(min_weight, min(new_w, max_weight))

        row["weight"] = round(float(new_w), 6)

    return adjusted
