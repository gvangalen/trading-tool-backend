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
    # match je UI slider (0..3)
    if v < 0:
        v = 0.0
    if v > 3:
        v = 3.0
    return v


def _clamp_score(s) -> int:
    try:
        v = int(float(s))
    except Exception:
        v = 50
    # jij wil geen 0-scores
    if v < 10:
        v = 10
    if v > 100:
        v = 100
    return v


def _bucket_key(a: float, b: float):
    return (round(float(a), 4), round(float(b), 4))


def _rules_to_fixed_buckets(rows):
    """
    rows: tuples from DB
    row schema:
      range_min, range_max, score, trend, interpretation, action, score_mode, weight, is_active
    We return exactly 5 buckets in order.
    """
    # pak alleen active
    active = [r for r in rows if r[8] is not False]

    # indexeer op exact bucket match
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
            out.append(
                {
                    "range_min": float(bmin),
                    "range_max": float(bmax),
                    "score": _clamp_score(r[2]),
                    "trend": r[3],
                    "interpretation": r[4],
                    "action": r[5],
                }
            )
        else:
            # ontbrekende bucket → toon wel bucket met fallback score
            out.append(
                {
                    "range_min": float(bmin),
                    "range_max": float(bmax),
                    "score": 50,
                    "trend": None,
                    "interpretation": "Bucket ontbreekt in DB (fallback).",
                    "action": "Geen actie.",
                }
            )
    return out


# =========================================================
# ✅ 1) GET indicator config (mode + weight + rules)
#    🔒 returns ALWAYS 5 fixed buckets
# =========================================================
@router.get("/indicator_config/{category}/{indicator}")
def get_indicator_config(
    category: str,
    indicator: str,
    current_user: dict = Depends(get_current_user),
):
    table = _get_table(category)
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="DB niet beschikbaar")

    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                    range_min,
                    range_max,
                    score,
                    trend,
                    interpretation,
                    action,
                    score_mode,
                    weight,
                    is_active
                FROM {table}
                WHERE indicator=%s
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
                "rules": [
                    {
                        "range_min": bmin,
                        "range_max": bmax,
                        "score": 50,
                        "trend": None,
                        "interpretation": "Nog geen regels opgeslagen (fallback).",
                        "action": "Geen actie.",
                    }
                    for (bmin, bmax) in FIXED_BUCKETS
                ],
            }

        # score_mode/weight nemen we van de eerste row
        score_mode = (rows[0][6] or "standard").strip().lower()
        weight = _clamp_weight(rows[0][7] or 1)

        # 🔒 ALWAYS 5 buckets
        rules = _rules_to_fixed_buckets(rows)

        return {
            "indicator": indicator,
            "category": category,
            "score_mode": score_mode,
            "weight": weight,
            "rules": rules,
        }

    except Exception:
        logger.exception("❌ get_indicator_config error")
        raise HTTPException(status_code=500, detail="Indicator config ophalen mislukt")
    finally:
        conn.close()


# =========================================================
# ✅ 2) UPDATE mode + weight
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

    if not category or not indicator or not score_mode:
        raise HTTPException(status_code=400, detail="category, indicator, score_mode verplicht")

    if score_mode not in ("standard", "contrarian", "custom"):
        raise HTTPException(status_code=400, detail="Ongeldige score_mode")

    table = _get_table(category)

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="DB niet beschikbaar")

    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {table}
                SET score_mode=%s,
                    weight=%s
                WHERE indicator=%s
                """,
                (score_mode, weight, indicator),
            )
        conn.commit()
        return {"ok": True, "indicator": indicator, "score_mode": score_mode, "weight": weight}

    except Exception:
        conn.rollback()
        logger.exception("❌ update_indicator_settings error")
        raise HTTPException(status_code=500, detail="Settings opslaan mislukt")
    finally:
        conn.close()


# =========================================================
# ✅ 3) SAVE custom rules
#    🔒 Force EXACT 5 fixed buckets. Ignore incoming min/max.
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

    if not category or not indicator:
        raise HTTPException(status_code=400, detail="category en indicator verplicht")

    if not isinstance(rules, list) or len(rules) == 0:
        raise HTTPException(status_code=400, detail="rules must be a non-empty list")

    # 🔒 must be 5 rules (bucket scores)
    if len(rules) != 5:
        raise HTTPException(status_code=400, detail="Custom rules moeten exact 5 buckets hebben (0–20..80–100)")

    table = _get_table(category)

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="DB niet beschikbaar")

    try:
        with conn.cursor() as cur:
            # 1) bestaande rows deactiveren
            cur.execute(
                f"""
                UPDATE {table}
                SET is_active=false
                WHERE indicator=%s
                """,
                (indicator,),
            )

            # 2) insert EXACT fixed buckets
            # we nemen score/interpretation/action/trend van payload per index (0..4)
            for idx, (bmin, bmax) in enumerate(FIXED_BUCKETS):
                r = rules[idx] if idx < len(rules) else {}

                cur.execute(
                    f"""
                    INSERT INTO {table}
                    (indicator, range_min, range_max, score, trend, interpretation, action, score_mode, is_active, weight)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,'custom',true,%s)
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
                    ),
                )

            # 3) zet mode/weight op alle rows
            cur.execute(
                f"""
                UPDATE {table}
                SET score_mode='custom',
                    weight=%s
                WHERE indicator=%s
                """,
                (weight, indicator),
            )

        conn.commit()
        return {"ok": True, "indicator": indicator, "saved": True}

    except HTTPException:
        raise
    except Exception:
        conn.rollback()
        logger.exception("❌ save_custom_rules error")
        raise HTTPException(status_code=500, detail="Custom rules opslaan mislukt")
    finally:
        conn.close()


# =========================================================
# ✅ 4) RESET naar standard
# =========================================================
@router.post("/indicator_config/reset")
def reset_indicator_rules(
    payload: dict,
    current_user: dict = Depends(get_current_user),
):
    category = payload.get("category")
    indicator = payload.get("indicator")

    if not category or not indicator:
        raise HTTPException(status_code=400, detail="category en indicator verplicht")

    table = _get_table(category)

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="DB niet beschikbaar")

    try:
        with conn.cursor() as cur:
            # verwijder custom rows
            cur.execute(
                f"DELETE FROM {table} WHERE indicator=%s AND score_mode='custom'",
                (indicator,),
            )
            # active alles weer + standaard mode
            cur.execute(
                f"""
                UPDATE {table}
                SET is_active=true,
                    score_mode='standard'
                WHERE indicator=%s
                """,
                (indicator,),
            )

        conn.commit()
        return {"ok": True, "indicator": indicator, "reset": True}

    except Exception:
        conn.rollback()
        logger.exception("❌ reset_indicator_rules error")
        raise HTTPException(status_code=500, detail="Reset mislukt")
    finally:
        conn.close()
