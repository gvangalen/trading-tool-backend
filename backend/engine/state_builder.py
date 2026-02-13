# backend/engine/state_builder.py

import logging
from datetime import date
from typing import Any, Dict, Optional

from backend.utils.db import get_db_connection

from backend.engine.transition_detector import compute_transition_detector, get_transition_risk_value
from backend.engine.market_pressure_engine import get_market_pressure

# Optional imports (fail-soft)
try:
    from backend.engine.exposure_engine import get_exposure_multiplier  # preferred API
except Exception:
    get_exposure_multiplier = None  # type: ignore

from backend.engine.policy_engine import evaluate_policy

try:
    from backend.engine.decision_engine import decide_amount
except Exception:
    decide_amount = None  # type: ignore

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# ============================================================
# DB fetch helpers (minimal, bot-safe)
# ============================================================

def _to_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        return None


def fetch_latest_scores(user_id: int) -> Dict[str, Optional[float]]:
    """
    Returns latest daily_scores for user (bot-safe).
    """
    conn = get_db_connection()
    if not conn:
        return {
            "macro_score": None,
            "market_score": None,
            "technical_score": None,
            "setup_score": None,
        }

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT macro_score, market_score, technical_score, setup_score
                FROM daily_scores
                WHERE user_id = %s
                ORDER BY report_date DESC
                LIMIT 1;
                """,
                (user_id,),
            )
            row = cur.fetchone()

        if not row:
            return {
                "macro_score": None,
                "market_score": None,
                "technical_score": None,
                "setup_score": None,
            }

        return {
            "macro_score": _to_float(row[0]),
            "market_score": _to_float(row[1]),
            "technical_score": _to_float(row[2]),
            "setup_score": _to_float(row[3]),
        }

    finally:
        conn.close()


def fetch_latest_regime(user_id: int) -> Dict[str, Any]:
    """
    Reads latest regime_memory row (bot-safe).
    """
    conn = get_db_connection()
    if not conn:
        return {"label": None, "confidence": None, "signals": None, "narrative": None}

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT regime_label, confidence, signals_json, narrative
                FROM regime_memory
                WHERE user_id = %s
                ORDER BY date DESC
                LIMIT 1;
                """,
                (user_id,),
            )
            row = cur.fetchone()

        if not row:
            return {"label": None, "confidence": None, "signals": None, "narrative": None}

        return {
            "label": row[0],
            "confidence": _to_float(row[1]),
            "signals": row[2],
            "narrative": row[3],
        }

    finally:
        conn.close()


# ============================================================
# Unified State Builder (single source of truth for bot/report)
# ============================================================

def build_daily_state(
    *,
    user_id: int,
    setup: Optional[Dict[str, Any]] = None,
    scores_override: Optional[Dict[str, float]] = None,
    regime_override: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Builds one unified "brain state" snapshot.

    Output keys:
      - date, user_id
      - scores
      - regime
      - transition (full snap) + transition_risk (0..1)
      - market_pressure (0..1)
      - policy (allowed actions, caps, etc.)
      - exposure_multiplier (0..X)
      - sizing: base_amount, decided_amount, final_amount
    """

    # ------------------------------------------------------------
    # 1) Core inputs
    # ------------------------------------------------------------
    scores = scores_override or fetch_latest_scores(user_id)
    regime = regime_override or fetch_latest_regime(user_id)

    # transition (full + normalized)
    transition_snap = compute_transition_detector(user_id=user_id, lookback_days=14)
    transition_risk = get_transition_risk_value(user_id)  # normalized 0..1

    # market pressure (0..1)
    market_pressure = get_market_pressure(user_id=user_id, scores=scores)

    # ------------------------------------------------------------
    # 2) Policy decision (hard gates + caps)
    # ------------------------------------------------------------
    policy = evaluate_policy(
        scores=scores,
        transition_risk=transition_risk,
        market_pressure=market_pressure,
        regime_label=regime.get("label"),
    )

    # ------------------------------------------------------------
    # 3) Exposure multiplier (optional engine)
    # ------------------------------------------------------------
    exposure_multiplier = 1.0
    try:
        if callable(get_exposure_multiplier):
            exposure_multiplier = float(
                get_exposure_multiplier(
                    user_id=user_id,
                    scores=scores,
                    market_pressure=market_pressure,
                    transition_risk=transition_risk,
                    regime_label=regime.get("label"),
                    policy=policy,
                )
            )
        else:
            # fallback: use policy caps around market_pressure
            # map pressure (0..1) -> multiplier (min..max)
            mn = float(policy.get("min_exposure_multiplier", 0.05))
            mx = float(policy.get("max_exposure_multiplier", 1.0))
            exposure_multiplier = mn + (mx - mn) * float(market_pressure)
    except Exception as e:
        logger.warning("Exposure multiplier fallback: %s", e)
        exposure_multiplier = float(policy.get("min_exposure_multiplier", 0.05))

    # Clamp to policy caps (institutional hard-stop)
    try:
        mn = float(policy.get("min_exposure_multiplier", 0.05))
        mx = float(policy.get("max_exposure_multiplier", 1.0))
        exposure_multiplier = max(mn, min(exposure_multiplier, mx))
    except Exception:
        pass

    # ------------------------------------------------------------
    # 4) Sizing (decision engine) + final amount
    # ------------------------------------------------------------
    base_amount = None
    decided_amount = None
    final_amount = None
    sizing_error = None

    if setup:
        base_amount = setup.get("base_amount")

        if decide_amount is None:
            sizing_error = "decide_amount import failed"
        else:
            try:
                decided_amount = float(decide_amount(setup=setup, scores=scores))
            except Exception as e:
                sizing_error = str(e)

    # apply exposure multiplier only if BUY allowed
    allowed_actions = policy.get("allowed_actions") or ["hold"]
    can_buy = "buy" in allowed_actions

    if decided_amount is not None:
        if can_buy:
            final_amount = round(float(decided_amount) * float(exposure_multiplier), 2)
        else:
            final_amount = 0.0  # policy says hold → do not allocate

    # Optional hard cap in EUR (if you set it later)
    cap = policy.get("max_position_eur")
    if cap is not None and final_amount is not None:
        try:
            final_amount = round(min(float(final_amount), float(cap)), 2)
        except Exception:
            pass

    # ------------------------------------------------------------
    # 5) Result
    # ------------------------------------------------------------
    state = {
        "date": date.today().isoformat(),
        "user_id": user_id,
        "scores": scores,
        "regime": regime,
        "transition": transition_snap,
        "transition_risk": transition_risk,
        "market_pressure": market_pressure,
        "policy": policy,
        "exposure_multiplier": round(float(exposure_multiplier), 4),
        "sizing": {
            "base_amount": base_amount,
            "decided_amount": decided_amount,
            "final_amount": final_amount,
            "error": sizing_error,
        },
    }

    return state
