# backend/utils/scoring_engine.py
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from backend.utils.db import get_db_connection

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# ============================================================
# Fixed buckets (UX/engine contract)
# ============================================================
FIXED_BUCKETS: List[Tuple[float, float]] = [
    (0.0, 20.0),
    (20.0, 40.0),
    (40.0, 60.0),
    (60.0, 80.0),
    (80.0, 100.0),
]

# Default “standard” scores per bucket (kan je later aanpassen)
DEFAULT_BUCKET_SCORES: List[int] = [10, 25, 50, 75, 100]


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
    user_id: Optional[int] = None  # ✅ nieuw: template (NULL) vs user override


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


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _clamp_score(v: int) -> int:
    # jij wil geen 0 — minimaal 10
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

    return s


def _table_names(category: str) -> Tuple[str, str]:
    """
    Returns: (rules_table, scores_table)
    """
    c = category.strip().lower()
    if c not in ("macro", "market", "technical"):
        raise ValueError("category must be: macro | market | technical")
    return (f"{c}_indicator_rules", f"{c}_indicator_scores")


# ============================================================
# Bucket enforcement (server-side contract)
# ============================================================
def _bucket_key(rmin: float, rmax: float) -> Tuple[float, float]:
    return (round(float(rmin), 4), round(float(rmax), 4))


def _fallback_fixed_rules(indicator: str, score_mode: str = "standard", weight: float = 1.0) -> List[RuleRow]:
    out: List[RuleRow] = []
    for i, (bmin, bmax) in enumerate(FIXED_BUCKETS):
        out.append(
            RuleRow(
                id=-1,
                indicator=indicator,
                range_min=bmin,
                range_max=bmax,
                score=DEFAULT_BUCKET_SCORES[i],
                trend=None,
                interpretation="Fallback bucket rule (auto).",
                action="Geen actie.",
                score_mode=score_mode,
                is_active=True,
                weight=weight,
                user_id=None,
            )
        )
    return out


def _force_fixed_buckets(indicator: str, rules: List[RuleRow]) -> List[RuleRow]:
    """
    Zorgt dat rules altijd EXACT 5 buckets zijn:
      0–20 / 20–40 / 40–60 / 60–80 / 80–100
    """
    if not rules:
        return _fallback_fixed_rules(indicator)

    # Neem score_mode/weight uit eerste rule (contract: consistent per indicator)
    first_mode = (rules[0].score_mode or "standard").strip().lower()
    first_weight = float(rules[0].weight if rules[0].weight is not None else 1.0)

    # map bestaande rules per bucket (alleen als bucket exact matcht)
    by_bucket: Dict[Tuple[float, float], RuleRow] = {}
    for r in rules:
        k = _bucket_key(r.range_min, r.range_max)
        if k in by_bucket:
            continue
        by_bucket[k] = r

    out: List[RuleRow] = []
    for i, (bmin, bmax) in enumerate(FIXED_BUCKETS):
        k = _bucket_key(bmin, bmax)

        if k in by_bucket:
            r = by_bucket[k]
            out.append(
                RuleRow(
                    id=int(r.id),
                    indicator=r.indicator,
                    range_min=bmin,
                    range_max=bmax,
                    score=int(r.score),
                    trend=r.trend,
                    interpretation=r.interpretation,
                    action=r.action,
                    score_mode=str(r.score_mode or first_mode),
                    is_active=bool(r.is_active),
                    weight=float(r.weight if r.weight is not None else first_weight),
                    user_id=r.user_id,
                )
            )
        else:
            # ontbrekende bucket → fallback invullen (maar wel mode/weight consistent houden)
            out.append(
                RuleRow(
                    id=-1,
                    indicator=indicator,
                    range_min=bmin,
                    range_max=bmax,
                    score=DEFAULT_BUCKET_SCORES[i],
                    trend=None,
                    interpretation="Bucket ontbreekt in DB (fallback).",
                    action="Geen actie.",
                    score_mode=first_mode,
                    is_active=True,
                    weight=first_weight,
                    user_id=None,
                )
            )

    return out


# ============================================================
# DB: Rules (USER override + TEMPLATE fallback)
# ============================================================
def fetch_rules_for_indicator(
    conn,
    category: str,
    indicator: str,
    user_id: Optional[int] = None,  # ✅ nieuw
    only_active: bool = True,
    enforce_fixed_buckets: bool = True,
) -> List[RuleRow]:
    rules_table, _ = _table_names(category)
    indicator = (indicator or "").strip()
    if not indicator:
        return []

    def _run_query(where_user_sql: str, params: tuple) -> List[tuple]:
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
                        COALESCE(weight, 1)              AS weight,
                        user_id
                    FROM {rules_table}
                    WHERE indicator = %s
                      AND {where_user_sql}
                      AND COALESCE(is_active, TRUE) = TRUE
                    ORDER BY range_min ASC, range_max ASC, id ASC
                    """,
                    params,
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
                        COALESCE(weight, 1)              AS weight,
                        user_id
                    FROM {rules_table}
                    WHERE indicator = %s
                      AND {where_user_sql}
                    ORDER BY range_min ASC, range_max ASC, id ASC
                    """,
                    params,
                )
            return cur.fetchall()

    # 1️⃣ user rules eerst
    rows: List[tuple] = []
    if user_id is not None:
        rows = _run_query("user_id = %s", (indicator, user_id))

    # 2️⃣ fallback template (NULL)
    if not rows:
        rows = _run_query("user_id IS NULL", (indicator,))

    rules: List[RuleRow] = []
    for r in rows:
        rules.append(
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
                user_id=int(r[11]) if r[11] is not None else None,
            )
        )

    if enforce_fixed_buckets:
        return _force_fixed_buckets(indicator, rules)

    return rules


def pick_rule_for_value(rules: List[RuleRow], value: Optional[float]) -> Optional[RuleRow]:
    """
    Matcht op:
      range_min <= value < range_max
    Laatste bucket: inclusive max.
    """
    if value is None or not rules:
        return None

    last_index = len(rules) - 1
    for idx, rule in enumerate(rules):
        if idx == last_index:
            if rule.range_min <= value <= rule.range_max:
                return rule
        else:
            if rule.range_min <= value < rule.range_max:
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
    user_id: Optional[int] = None,  # ✅ nieuw
) -> Dict[str, Any]:
    """
    Engine contract:
    - value hoort NORMALIZED 0–100 te zijn.
    - wij clampen voor safety.
    """
    v_raw = _to_float(value)
    v = None if v_raw is None else _clamp(v_raw, 0.0, 100.0)

    rules = fetch_rules_for_indicator(
        conn,
        category=category,
        indicator=indicator,
        user_id=user_id,
        only_active=True,
        enforce_fixed_buckets=True,
    )
    rule = pick_rule_for_value(rules, v)

    if not rule:
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
        "rules_user_id": rule.user_id,  # handig voor debug
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

        scored = score_indicator(
            conn,
            category=category,
            indicator=str(indicator),
            value=value,
            user_id=user_id,  # ✅ user-based override
        )
        items.append(scored)

        s = float(scored["score"] or 10)
        w = float(scored["weight"] or 1.0)

        weighted_sum += s * w
        raw_sum += s
        total_weight += w
        count += 1

    raw_avg = (raw_sum / count) if count > 0 else 10.0
    weighted_avg = (weighted_sum / total_weight) if total_weight > 0 else 10.0

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
