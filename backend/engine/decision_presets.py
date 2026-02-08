# backend/engine/decision_presets.py
"""
Decision curve presets (data-only).

Doel:
- Presets zijn puur JSON/dict data (geen code-logica).
- DecisionEngine blijft generiek: curve bepaalt gedrag.

Conventies:
- x = score (0–100)
- y = multiplier op base_amount
- input = score-key uit `scores` dict, default: "market_score"
"""


# ------------------------------------------------------------
# DCA_CONTRARIAN
# Meer kopen bij zwakte (lage market_score), pauze bij euforie.
#
# Eigenschap:
# - market_score 20 => 1.5x (150%)
# - market_score 90 => 0.0x (pauze)
# ------------------------------------------------------------
DCA_CONTRARIAN = {
    "input": "market_score",
    "points": [
        {"x": 0, "y": 1.7},
        {"x": 20, "y": 1.5},
        {"x": 40, "y": 1.2},
        {"x": 60, "y": 1.0},
        {"x": 80, "y": 0.5},
        {"x": 90, "y": 0.0},
        {"x": 100, "y": 0.0},
    ],
}


# ------------------------------------------------------------
# DCA_TREND_FOLLOWING
# Meer kopen bij kracht (hoge market_score), minder bij zwakte.
#
# Test contract:
# - market_score 80 => 1.4x (140%)
# ------------------------------------------------------------
DCA_TREND_FOLLOWING = {
    "input": "market_score",
    "points": [
        {"x": 0, "y": 0.5},
        {"x": 20, "y": 0.7},
        {"x": 40, "y": 0.9},
        {"x": 60, "y": 1.1},
        {"x": 80, "y": 1.4},
        {"x": 100, "y": 1.5},
    ],
}


# Optioneel: handig voor UI dropdown of validatie
PRESET_MAP = {
    "dca_contrarian": DCA_CONTRARIAN,
    "dca_trend_following": DCA_TREND_FOLLOWING,
}
