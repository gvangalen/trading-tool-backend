import json
import logging
from datetime import date
from typing import Any, Dict, Optional

from psycopg2.extras import Json

from backend.utils.db import get_db_connection
from backend.ai_core.transition_detector import compute_transition_detector

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def _safe_json(obj: Any) -> Any:
    from datetime import datetime
    if obj is None:
        return None
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: _safe_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_safe_json(v) for v in obj]
    return obj


def get_regime_memory(user_id: int) -> Optional[Dict[str, Any]]:
    conn = get_db_connection()
    if not conn:
        return None

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT date, regime_label, confidence, signals_json, narrative
                FROM regime_memory
                WHERE user_id = %s
                ORDER BY date DESC
                LIMIT 1;
                """,
                (user_id,),
            )
            row = cur.fetchone()
            if not row:
                return None

            return {
                "date": row[0].isoformat() if row[0] else None,
                "regime_label": row[1],
                "confidence": float(row[2]) if row[2] is not None else None,
                "signals_json": row[3],
                "narrative": row[4],
            }
    finally:
        conn.close()


def store_regime_memory(user_id: int) -> Dict[str, Any]:
    """
    Minimal, safe implementation:
    - computes transition detector (multi-day)
    - stores it inside signals_json as "transition"
    - writes a clean regime narrative line that your report can build on
    """
    today = date.today()

    # Transition layer (the new juice)
    transition = compute_transition_detector(user_id=user_id, lookback_days=14)

    # If you later add full regime classifier, this can be replaced/extended.
    # For now we set a conservative label derived from transition risk.
    r = transition.get("transition_risk", 50)

    if r >= 75:
        regime_label = "late_cycle_risk"
        regime_conf = 0.68
    elif r >= 60:
        regime_label = "transition_watch"
        regime_conf = 0.62
    elif r >= 45:
        regime_label = "regime_persistent"
        regime_conf = 0.58
    else:
        regime_label = "trend_supported"
        regime_conf = 0.60

    # signals_json becomes the single source of truth (regime + transitions)
    signals_json = {
        "regime": {
            "label": regime_label,
            "confidence": regime_conf,
        },
        "transition": transition,
    }

    # hedge-fund narrative
    narrative = transition.get("narrative") or "Regime persistent. No clear transition signature."

    conn = get_db_connection()
    if not conn:
        logger.error("❌ No DB connection in store_regime_memory")
        return {
            "regime_label": regime_label,
            "confidence": regime_conf,
            "signals_json": signals_json,
            "narrative": narrative,
        }

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO regime_memory (
                    user_id, date, regime_label, confidence, signals_json, narrative
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (user_id, date)
                DO UPDATE SET
                    regime_label = EXCLUDED.regime_label,
                    confidence   = EXCLUDED.confidence,
                    signals_json = EXCLUDED.signals_json,
                    narrative    = EXCLUDED.narrative,
                    created_at   = NOW();
                """,
                (
                    user_id,
                    today,
                    regime_label,
                    regime_conf,
                    Json(_safe_json(signals_json)),
                    narrative,
                ),
            )

        conn.commit()
        logger.info("🧠 regime_memory stored | user_id=%s | label=%s | risk=%s", user_id, regime_label, r)

        return {
            "regime_label": regime_label,
            "confidence": regime_conf,
            "signals_json": signals_json,
            "narrative": narrative,
        }

    except Exception:
        logger.error("❌ store_regime_memory failed", exc_info=True)
        conn.rollback()
        raise
    finally:
        conn.close()
