import logging
from typing import Any, Dict, Optional


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# =========================================================
# UI reason mapping
# =========================================================

BLOCK_REASON_LABELS = {
    "kill_switch": "Bot is disabled",
    "daily_allocation": "Daily allocation reached",
    "asset_exposure": "Asset exposure limit reached",
    "no_allocatable_size": "No valid trade setup",
}


# =========================================================
# Helpers
# =========================================================

def _safe_float(value: Any, fallback: float = 0.0) -> float:
    try:
        if value is None:
            return fallback
        return float(value)
    except Exception:
        return fallback


def _safe_bool(value: Any, fallback: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return fallback
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on", "active"}
    return bool(value)


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(value, high))


def _round_money(value: float) -> float:
    return round(float(value), 2)


# =========================================================
# Core guardrails
# =========================================================

def apply_guardrails(
    *,
    proposed_amount_eur: float,
    portfolio_value_eur: float = 0.0,
    current_asset_value_eur: float = 0.0,
    today_allocated_eur: float = 0.0,
    kill_switch: bool = True,
    max_trade_risk_eur: Optional[float] = None,
    daily_allocation_eur: Optional[float] = None,
    max_asset_exposure_pct: Optional[float] = None,
) -> Dict[str, Any]:

    original_amount = max(_safe_float(proposed_amount_eur, 0.0), 0.0)
    adjusted_amount = original_amount
    warnings = []
    blocked_by = None

    portfolio_value = max(_safe_float(portfolio_value_eur, 0.0), 0.0)
    current_asset_value = max(_safe_float(current_asset_value_eur, 0.0), 0.0)
    today_allocated = max(_safe_float(today_allocated_eur, 0.0), 0.0)

    kill_switch = _safe_bool(kill_switch, True)

    max_trade_risk = _safe_float(max_trade_risk_eur, 0.0)
    daily_allocation = _safe_float(daily_allocation_eur, 0.0)

    max_asset_exposure = _safe_float(max_asset_exposure_pct, 100.0)

    logger.info(
        "Guardrails input | proposed=%s portfolio=%s asset_value=%s daily_allocated=%s max_trade_risk=%s daily_limit=%s max_exposure=%s",
        original_amount,
        portfolio_value,
        current_asset_value,
        today_allocated,
        max_trade_risk,
        daily_allocation,
        max_asset_exposure,
    )

    # -----------------------------------------------------
    # 1. Kill switch
    # -----------------------------------------------------

    if not kill_switch:

        blocked_by = "kill_switch"

        return {
            "allowed": False,
            "adjusted_amount_eur": 0.0,
            "original_amount_eur": _round_money(original_amount),
            "warnings": ["kill_switch_off"],
            "blocked_by": blocked_by,
            "reason": BLOCK_REASON_LABELS.get(blocked_by),
            "debug_code": blocked_by,
            "guardrails": {
                "kill_switch": False,
                "max_trade_risk_eur": _round_money(max_trade_risk),
                "daily_allocation_eur": _round_money(daily_allocation),
                "max_asset_exposure_pct": max_asset_exposure,
                "current_asset_exposure_pct": _calculate_exposure_pct(
                    current_asset_value_eur=current_asset_value,
                    portfolio_value_eur=portfolio_value,
                ),
            },
        }

    # -----------------------------------------------------
    # 2. Max trade risk
    # -----------------------------------------------------

    if max_trade_risk > 0 and adjusted_amount > max_trade_risk:
        adjusted_amount = max_trade_risk
        warnings.append("max_trade_risk_trimmed")

    # -----------------------------------------------------
    # 3. Daily allocation
    # -----------------------------------------------------

    if daily_allocation > 0:

        remaining_daily = max(daily_allocation - today_allocated, 0.0)

        if remaining_daily <= 0:

            blocked_by = "daily_allocation"

            return {
                "allowed": False,
                "adjusted_amount_eur": 0.0,
                "original_amount_eur": _round_money(original_amount),
                "warnings": warnings + ["daily_allocation_reached"],
                "blocked_by": blocked_by,
                "reason": BLOCK_REASON_LABELS.get(blocked_by),
                "debug_code": blocked_by,
                "guardrails": {
                    "kill_switch": True,
                    "max_trade_risk_eur": _round_money(max_trade_risk),
                    "daily_allocation_eur": _round_money(daily_allocation),
                    "remaining_daily_eur": 0.0,
                    "max_asset_exposure_pct": max_asset_exposure,
                    "current_asset_exposure_pct": _calculate_exposure_pct(
                        current_asset_value_eur=current_asset_value,
                        portfolio_value_eur=portfolio_value,
                    ),
                },
            }

        if adjusted_amount > remaining_daily:
            adjusted_amount = remaining_daily
            warnings.append("daily_allocation_trimmed")

    # -----------------------------------------------------
    # 4. Max asset exposure
    # -----------------------------------------------------

    if max_asset_exposure > 0 and portfolio_value > 0:

        max_asset_value_allowed = portfolio_value * (
            _clamp(max_asset_exposure, 0.0, 100.0) / 100.0
        )

        remaining_asset_capacity = max(
            max_asset_value_allowed - current_asset_value,
            0.0,
        )

        logger.info(
            "Exposure check | current_asset=%s max_pct=%s max_allowed=%s remaining=%s",
            current_asset_value,
            max_asset_exposure,
            max_asset_value_allowed,
            remaining_asset_capacity,
        )

        if remaining_asset_capacity <= 0:

            blocked_by = "asset_exposure"

            return {
                "allowed": False,
                "adjusted_amount_eur": 0.0,
                "original_amount_eur": _round_money(original_amount),
                "warnings": warnings + ["asset_exposure_limit_reached"],
                "blocked_by": blocked_by,
                "reason": BLOCK_REASON_LABELS.get(blocked_by),
                "debug_code": blocked_by,
                "guardrails": {
                    "kill_switch": True,
                    "max_trade_risk_eur": _round_money(max_trade_risk),
                    "daily_allocation_eur": _round_money(daily_allocation),
                    "max_asset_exposure_pct": max_asset_exposure,
                    "current_asset_exposure_pct": _calculate_exposure_pct(
                        current_asset_value_eur=current_asset_value,
                        portfolio_value_eur=portfolio_value,
                    ),
                    "remaining_asset_capacity_eur": 0.0,
                },
            }

        if adjusted_amount > remaining_asset_capacity:
            adjusted_amount = remaining_asset_capacity
            warnings.append("asset_exposure_trimmed")

    # -----------------------------------------------------
    # Final result
    # -----------------------------------------------------

    adjusted_amount = max(adjusted_amount, 0.0)
    adjusted_amount = _round_money(adjusted_amount)

    allowed = adjusted_amount > 0

    if not allowed and not blocked_by:
        blocked_by = "no_allocatable_size"

    result = {
        "allowed": allowed,
        "adjusted_amount_eur": adjusted_amount,
        "original_amount_eur": _round_money(original_amount),
        "warnings": warnings,
        "blocked_by": blocked_by,
        "reason": BLOCK_REASON_LABELS.get(blocked_by),
        "debug_code": blocked_by,
        "guardrails": {
            "kill_switch": True,
            "max_trade_risk_eur": _round_money(max_trade_risk),
            "daily_allocation_eur": _round_money(daily_allocation),
            "remaining_daily_eur": _round_money(
                max(daily_allocation - today_allocated, 0.0)
            ) if daily_allocation > 0 else None,
            "max_asset_exposure_pct": max_asset_exposure,
            "current_asset_exposure_pct": _calculate_exposure_pct(
                current_asset_value_eur=current_asset_value,
                portfolio_value_eur=portfolio_value,
            ),
            "remaining_asset_capacity_eur": _round_money(
                _calculate_remaining_asset_capacity(
                    portfolio_value_eur=portfolio_value,
                    current_asset_value_eur=current_asset_value,
                    max_asset_exposure_pct=max_asset_exposure,
                )
            ) if max_asset_exposure > 0 and portfolio_value > 0 else None,
        },
    }

    logger.info(
        "Guardrails result | original=%s adjusted=%s allowed=%s blocked_by=%s warnings=%s",
        original_amount,
        adjusted_amount,
        allowed,
        blocked_by,
        warnings,
    )

    return result


# =========================================================
# Exposure helpers
# =========================================================

def _calculate_exposure_pct(
    *,
    current_asset_value_eur: float,
    portfolio_value_eur: float,
) -> float:

    asset_value = max(_safe_float(current_asset_value_eur, 0.0), 0.0)
    portfolio_value = max(_safe_float(portfolio_value_eur, 0.0), 0.0)

    if portfolio_value <= 0:
        return 0.0

    return round((asset_value / portfolio_value) * 100.0, 2)


def _calculate_remaining_asset_capacity(
    *,
    portfolio_value_eur: float,
    current_asset_value_eur: float,
    max_asset_exposure_pct: float,
) -> float:

    portfolio_value = max(_safe_float(portfolio_value_eur, 0.0), 0.0)
    current_asset_value = max(_safe_float(current_asset_value_eur, 0.0), 0.0)

    max_asset_exposure_pct = _clamp(
        _safe_float(max_asset_exposure_pct, 100.0),
        0.0,
        100.0,
    )

    if portfolio_value <= 0 or max_asset_exposure_pct <= 0:
        return 0.0

    max_asset_value_allowed = portfolio_value * (
        max_asset_exposure_pct / 100.0
    )

    return max(
        max_asset_value_allowed - current_asset_value,
        0.0,
    )
