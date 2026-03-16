"""
Position sizing presets (data-only).

Doel:
- Presets zijn puur JSON/dict data (geen code-logica).
- curve_engine bepaalt multiplier.
- decision_engine bepaalt richting.

Conventies:
- x = score (0–100)
- y = multiplier op base_amount
"""

MIN_MULTIPLIER = 0.05
MAX_MULTIPLIER = 3.0


# =====================================================
# CONTRARIAN DCA
# =====================================================

DCA_CONTRARIAN = {
    "name": "Contrarian DCA",
    "description": "Koop meer bij zwakte, schaal af bij euforie.",
    "input": "market_score",
    "min_multiplier": MIN_MULTIPLIER,
    "max_multiplier": MAX_MULTIPLIER,
    "points": [
        {"x": 0, "y": 1.7},
        {"x": 20, "y": 1.5},
        {"x": 40, "y": 1.2},
        {"x": 60, "y": 1.0},
        {"x": 80, "y": 0.5},
        {"x": 90, "y": 0.05},  # nooit 0
        {"x": 100, "y": 0.05},
    ],
}


# =====================================================
# TREND FOLLOWING DCA
# =====================================================

DCA_TREND_FOLLOWING = {
    "name": "Trend Following DCA",
    "description": "Investeer meer bij kracht, minder bij zwakte.",
    "input": "market_score",
    "min_multiplier": MIN_MULTIPLIER,
    "max_multiplier": MAX_MULTIPLIER,
    "points": [
        {"x": 0, "y": 0.5},
        {"x": 20, "y": 0.7},
        {"x": 40, "y": 0.9},
        {"x": 60, "y": 1.1},
        {"x": 80, "y": 1.4},
        {"x": 100, "y": 1.5},
    ],
}


# =====================================================
# PRESET MAP
# =====================================================

PRESET_MAP = {
    "dca_contrarian": DCA_CONTRARIAN,
    "dca_trend_following": DCA_TREND_FOLLOWING,
}


# =====================================================
# HELPER
# =====================================================

def get_curve_preset(name: str):
    """
    Haalt preset veilig op.

    voorkomt crashes in decision_engine
    """

    if not name:
        return None

    return PRESET_MAP.get(str(name).lower())
