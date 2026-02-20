# backend/utils/scoring_engine.py
import logging
from dataclasses import dataclass
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Tuple

from backend.utils.db import get_db_connection

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# ============================================================
# Types
# ============================================================

@dataclass
class RuleRow:
    id: int
    indicator: str
    range_min: float
    range_max: float
    score: int
    trend: Optional[str]
    interpretation: Optional[str]
    action: Optional[str]
    score_mode: str
    is_active: bool
    weight: float


# ============================================================
# Helpers
# ============================================================

def _to_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None


def _to_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    try:
        return int(v)
    except Exception:
        try:
            return int(float(v))
        except Exception:
            return None


def _clamp_score(v: int) -> int:
    if v < 10:
        return 10
    if v > 100:
        return 100
    return v


def _apply_score_mode(score: int, score_mode: str) -> int:
    """
    - standard: score zoals rule
    - contrarian: 100 - score
    - custom: score zoals rule (custom rules zijn al "eigen")
    """
    s = _clamp_score(score)
    mode = (score_mode or "standard").strip().lower()

    if mode == "contrarian":
        return _clamp_score(100 - s)

    # standard / custom -> return as-is
    return s


def _table_names(category: str) -> Tuple[str, str]:
    """
    Returns: (rules_table, scores_table)
    """
    c = category.strip().lower()
    if c not in ("macro", "market", "technical"):
        raise ValueError("category must be: macro | market | technical")

    return (f"{c}_indicator_rules", f"{c}_indicator_scores")


def _score_mode_constraint(category: str) -> Tuple[str, ...]:
    # Jij hebt constraints op macro/technical. market heeft (nog) geen check constraint in je output,
    # maar we ondersteunen dezelfde 3 modes sowieso.
    return ("standard", "contrarian", "custom")


# ============================================================
# DB: Rules
# ============================================================

def fetch_rules_for_indicator(
    conn,
    category: str,
    indicator: str,
    only_active: bool = True,
) -> List[RuleRow]:
    rules_table, _ = _table_names(category)
    indicator = (indicator or "").strip()

    if not indicator:
        return []

    with conn.cursor() as cur:
        if only_active:
            cur.execute(
                f"""
                SELECT
                    id,
                    indicator,
                    range_min,
                    range_max,
                    score,
                    trend,
                    interpretation,
                    action,
                    COALESCE(score_mode, 'standard') AS score_mode,
                    COALESCE(is_active, TRUE)        AS is_active,
                    COALESCE(weight, 1)              AS weight
                FROM {rules_table}
                WHERE indicator = %s
                  AND COALESCE(is_active, TRUE) = TRUE
                ORDER BY range_min ASC, range_max ASC, id ASC
                """,
                (indicator,),
            )
        else:
            cur.execute(
                f"""
                SELECT
                    id,
                    indicator,
                    range_min,
                    range_max,
                    score,
                    trend,
                    interpretation,
                    action,
                    COALESCE(score_mode, 'standard') AS score_mode,
                    COALESCE(is_active, TRUE)        AS is_active,
                    COALESCE(weight, 1)              AS weight
                FROM {rules_table}
                WHERE indicator = %s
                ORDER BY range_min ASC, range_max ASC, id ASC
                """,
                (indicator,),
            )

        rows = cur.fetchall()

    out: List[RuleRow] = []
    for r in rows:
        out.append(
            RuleRow(
                id=int(r[0]),
                indicator=str(r[1]),
                range_min=float(r[2]),
                range_max=float(r[3]),
                score=int(r[4]),
                trend=r[5],
                interpretation=r[6],
                action=r[7],
                score_mode=str(r[8] or "standard"),
                is_active=bool(r[9]),
                weight=float(r[10] if r[10] is not None else 1.0),
            )
        )

    return out


def pick_rule_for_value(rules: List[RuleRow], value: Optional[float]) -> Optional[RuleRow]:
    """
    Matcht op range_min <= value <= range_max
    Als value None -> geen rule.
    """
    if value is None:
        return None
    for rule in rules:
        if rule.range_min <= value <= rule.range_max:
            return rule
    return None


# ============================================================
# Scoring (single indicator)
# ============================================================

def score_indicator(
    conn,
    category: str,
    indicator: str,
    value: Any,
) -> Dict[str, Any]:
    """
    Return payload:
    {
      indicator, value,
      base_score, score, score_mode, weight,
      trend, interpretation, action,
      matched_rule_id
    }
    """
    v = _to_float(value)

    rules = fetch_rules_for_indicator(conn, category=category, indicator=indicator, only_active=True)
    rule = pick_rule_for_value(rules, v)

    if not rule:
        # fallback: neutraal-minimum (jij wilde geen 0)
        return {
            "indicator": indicator,
            "value": v,
            "base_score": 10,
            "score": 10,
            "score_mode": "standard",
            "weight": 1.0,
            "trend": None,
            "interpretation": "Geen scoreregel match (fallback).",
            "action": "Geen actie.",
            "matched_rule_id": None,
        }

    base_score = _clamp_score(int(rule.score))
    final_score = _apply_score_mode(base_score, rule.score_mode)

    # weight clamp (safety)
    w = float(rule.weight if rule.weight is not None else 1.0)
    if w <= 0:
        w = 1.0

    return {
        "indicator": indicator,
        "value": v,
        "base_score": base_score,
        "score": final_score,
        "score_mode": rule.score_mode,
        "weight": w,
        "trend": rule.trend,
        "interpretation": rule.interpretation,
        "action": rule.action,
        "matched_rule_id": rule.id,
    }


# ============================================================
# Scoring (category: many indicators)
# ============================================================

def score_category(
    conn,
    user_id: int,
    category: str,
    indicator_values: Dict[str, Any],
    persist: bool = True,
    ts: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    indicator_values: {"RSI": 47.2, "MA200": 1, ...}

    Return:
    {
      "category": "technical",
      "timestamp": "...",
      "items": [ {indicator score payload...}, ... ],
      "weighted_score": 62,
      "raw_avg_score": 58,
      "total_weight": 7.5
    }
    """
    if ts is None:
        ts = datetime.utcnow()

    if not isinstance(indicator_values, dict):
        indicator_values = {}

    items: List[Dict[str, Any]] = []
    weighted_sum = 0.0
    raw_sum = 0.0
    total_weight = 0.0
    count = 0

    for indicator, value in indicator_values.items():
        if not indicator:
            continue

        scored = score_indicator(conn, category=category, indicator=str(indicator), value=value)
        items.append(scored)

        s = float(scored["score"] or 10)
        w = float(scored["weight"] or 1.0)

        weighted_sum += s * w
        raw_sum += s
        total_weight += w
        count += 1

    raw_avg = (raw_sum / count) if count > 0 else 10.0
    weighted_avg = (weighted_sum / total_weight) if total_weight > 0 else 10.0

    # scores als int (dashboard meters)
    raw_avg_i = _clamp_score(int(round(raw_avg)))
    weighted_avg_i = _clamp_score(int(round(weighted_avg)))

    if persist and count > 0:
        persist_indicator_scores(
            conn=conn,
            user_id=user_id,
            category=category,
            items=items,
            ts=ts,
        )

    return {
        "category": category,
        "timestamp": ts.isoformat(),
        "items": items,
        "raw_avg_score": raw_avg_i,
        "weighted_score": weighted_avg_i,
        "total_weight": float(total_weight),
    }


# ============================================================
# Persist to *_indicator_scores
# ============================================================

def persist_indicator_scores(
    conn,
    user_id: int,
    category: str,
    items: List[Dict[str, Any]],
    ts: Optional[datetime] = None,
) -> None:
    """
    Slaat per indicator 1 row per dag op (UNIQUE user_id, indicator, score_date)
    Werkt met jouw schema:
      indicator, value, score, trend, interpretation, action, timestamp, user_id
    """
    if ts is None:
        ts = datetime.utcnow()

    _, scores_table = _table_names(category)

    with conn.cursor() as cur:
        for it in items:
            indicator = str(it.get("indicator") or "").strip()
            if not indicator:
                continue

            value = it.get("value")
            score = _to_int(it.get("score"))
            trend = it.get("trend")
            interpretation = it.get("interpretation")
            action = it.get("action")

            # safety
            score = _clamp_score(int(score or 10))

            cur.execute(
                f"""
                INSERT INTO {scores_table} (
                    indicator,
                    value,
                    score,
                    trend,
                    interpretation,
                    action,
                    timestamp,
                    user_id
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (user_id, indicator, score_date)
                DO UPDATE SET
                    value = EXCLUDED.value,
                    score = EXCLUDED.score,
                    trend = EXCLUDED.trend,
                    interpretation = EXCLUDED.interpretation,
                    action = EXCLUDED.action,
                    timestamp = EXCLUDED.timestamp
                """,
                (
                    indicator,
                    value,
                    score,
                    trend,
                    interpretation,
                    action,
                    ts,
                    user_id,
                ),
            )


# ============================================================
# Convenience: run scoring with its own DB connection
# ============================================================

def run_category_scoring(
    user_id: int,
    category: str,
    indicator_values: Dict[str, Any],
    persist: bool = True,
    ts: Optional[datetime] = None,
) -> Dict[str, Any]:
    conn = get_db_connection()
    if not conn:
        raise RuntimeError("DB niet beschikbaar")

    try:
        result = score_category(
            conn=conn,
            user_id=user_id,
            category=category,
            indicator_values=indicator_values,
            persist=persist,
            ts=ts,
        )
        conn.commit()
        return result
    except Exception:
        conn.rollback()
        logger.exception("❌ run_category_scoring failed")
        raise
    finally:
        conn.close()
