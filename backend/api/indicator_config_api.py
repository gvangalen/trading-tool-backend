import logging
from fastapi import APIRouter, HTTPException, Depends

from backend.utils.db import get_db_connection
from backend.utils.auth_utils import get_current_user

router = APIRouter()
logger = logging.getLogger(__name__)

TABLE_MAP = {
    "macro": "macro_indicator_rules",
    "technical": "technical_indicator_rules",
    "market": "market_indicator_rules",
}

# 🔒 Fixed buckets contract (UX-proof)
FIXED_BUCKETS = [
    (0.0, 20.0),
    (20.0, 40.0),
    (40.0, 60.0),
    (60.0, 80.0),
    (80.0, 100.0),
]


def _get_table(category: str) -> str:
    table = TABLE_MAP.get(category)
    if not table:
        raise HTTPException(status_code=400, detail="Ongeldige category")
    return table


def _clamp_weight(w) -> float:
    try:
        v = float(w)
    except Exception:
        v = 1.0
    return max(0.0, min(3.0, v))


def _clamp_score(s) -> int:
    try:
        v = int(float(s))
    except Exception:
        v = 50
    return max(10, min(100, v))


def _bucket_key(a: float, b: float):
    return (round(float(a), 4), round(float(b), 4))


def _rules_to_fixed_buckets(rows):
    active = [r for r in rows if r[8] is not False]

    by_bucket = {}
    for r in active:
        k = _bucket_key(r[0], r[1])
        if k in by_bucket:
            continue
        by_bucket[k] = r

    out = []
    for (bmin, bmax) in FIXED_BUCKETS:
        k = _bucket_key(bmin, bmax)
        if k in by_bucket:
            r = by_bucket[k]
            out.append({
                "range_min": float(bmin),
                "range_max": float(bmax),
                "score": _clamp_score(r[2]),
                "trend": r[3],
                "interpretation": r[4],
                "action": r[5],
            })
        else:
            out.append({
                "range_min": float(bmin),
                "range_max": float(bmax),
                "score": 50,
                "trend": None,
                "interpretation": "Bucket ontbreekt in DB (fallback).",
                "action": "Geen actie.",
            })
    return out


# =========================================================
# ✅ 1) GET indicator config (USER override + template fallback)
# =========================================================
@router.get("/indicator_config/{category}/{indicator}")
def get_indicator_config(
    category: str,
    indicator: str,
    current_user: dict = Depends(get_current_user),
):
    table = _get_table(category)
    user_id = current_user["id"]

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="DB niet beschikbaar")

    try:
        with conn.cursor() as cur:

            # 1️⃣ User-specific rules
            cur.execute(
                f"""
                SELECT range_min, range_max, score, trend,
                       interpretation, action, score_mode,
                       weight, is_active
                FROM {table}
                WHERE indicator=%s
                  AND user_id=%s
                ORDER BY range_min ASC
                """,
                (indicator, user_id),
            )
            rows = cur.fetchall()

            # 2️⃣ Template fallback
            if not rows:
                cur.execute(
                    f"""
                    SELECT range_min, range_max, score, trend,
                           interpretation, action, score_mode,
                           weight, is_active
                    FROM {table}
                    WHERE indicator=%s
                      AND user_id IS NULL
                    ORDER BY range_min ASC
                    """,
                    (indicator,),
                )
                rows = cur.fetchall()

        if not rows:
            return {
                "indicator": indicator,
                "category": category,
                "score_mode": "standard",
                "weight": 1.0,
                "rules": _rules_to_fixed_buckets([]),
            }

        score_mode = (rows[0][6] or "standard").strip().lower()
        weight = _clamp_weight(rows[0][7] or 1)
        rules = _rules_to_fixed_buckets(rows)

        return {
            "indicator": indicator,
            "category": category,
            "score_mode": score_mode,
            "weight": weight,
            "rules": rules,
        }

    finally:
        conn.close()


# =========================================================
# ✅ 2) UPDATE mode + weight (USER ONLY)
# =========================================================
@router.put("/indicator_config/settings")
def update_indicator_settings(
    payload: dict,
    current_user: dict = Depends(get_current_user),
):
    category = payload.get("category")
    indicator = payload.get("indicator")
    score_mode = (payload.get("score_mode") or "").strip().lower()
    weight = _clamp_weight(payload.get("weight", 1))
    user_id = current_user["id"]

    table = _get_table(category)

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {table}
                SET score_mode=%s,
                    weight=%s
                WHERE indicator=%s
                  AND user_id=%s
                """,
                (score_mode, weight, indicator, user_id),
            )
        conn.commit()
        return {"ok": True, "indicator": indicator}

    finally:
        conn.close()


# =========================================================
# ✅ 3) SAVE custom rules (USER ONLY)
# =========================================================
@router.post("/indicator_config/custom")
def save_custom_rules(
    payload: dict,
    current_user: dict = Depends(get_current_user),
):
    category = payload.get("category")
    indicator = payload.get("indicator")
    rules = payload.get("rules") or []
    weight = _clamp_weight(payload.get("weight", 1))
    user_id = current_user["id"]

    if len(rules) != 5:
        raise HTTPException(status_code=400, detail="Exact 5 buckets verplicht")

    table = _get_table(category)

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:

            # verwijder oude user override
            cur.execute(
                f"""
                DELETE FROM {table}
                WHERE indicator=%s
                  AND user_id=%s
                """,
                (indicator, user_id),
            )

            # insert nieuwe custom buckets
            for idx, (bmin, bmax) in enumerate(FIXED_BUCKETS):
                r = rules[idx]
                cur.execute(
                    f"""
                    INSERT INTO {table}
                    (indicator, range_min, range_max, score,
                     trend, interpretation, action,
                     score_mode, is_active, weight, user_id)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,'custom',true,%s,%s)
                    """,
                    (
                        indicator,
                        bmin,
                        bmax,
                        _clamp_score(r.get("score", 50)),
                        r.get("trend"),
                        r.get("interpretation"),
                        r.get("action"),
                        weight,
                        user_id,
                    ),
                )

        conn.commit()
        return {"ok": True}

    finally:
        conn.close()


# =========================================================
# ✅ 4) RESET → verwijder user override
# =========================================================
@router.post("/indicator_config/reset")
def reset_indicator_rules(
    payload: dict,
    current_user: dict = Depends(get_current_user),
):
    category = payload.get("category")
    indicator = payload.get("indicator")
    user_id = current_user["id"]

    table = _get_table(category)

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                DELETE FROM {table}
                WHERE indicator=%s
                  AND user_id=%s
                """,
                (indicator, user_id),
            )
        conn.commit()
        return {"ok": True}

    finally:
        conn.close()
