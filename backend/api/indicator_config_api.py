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


def _get_table(category: str) -> str:
    table = TABLE_MAP.get(category)
    if not table:
        raise HTTPException(status_code=400, detail="Ongeldige category")
    return table


# =========================================================
# ✅ 1) GET indicator config (mode + weight + rules)
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
                "weight": 1,
                "rules": [],
            }

        # score_mode/weight nemen we van de eerste row (moet overal gelijk zijn)
        score_mode = rows[0][6] or "standard"
        weight = float(rows[0][7] or 1)

        # Alleen actieve rules tonen (maar als je alles wil tonen kan dat ook)
        rules = []
        for r in rows:
            if r[8] is False:
                continue
            rules.append(
                {
                    "range_min": float(r[0]),
                    "range_max": float(r[1]),
                    "score": int(r[2]),
                    "trend": r[3],
                    "interpretation": r[4],
                    "action": r[5],
                }
            )

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
# ✅ 2) UPDATE mode + weight (1 klik contrarian / standard)
# =========================================================
@router.put("/indicator_config/settings")
def update_indicator_settings(
    payload: dict,
    current_user: dict = Depends(get_current_user),
):
    category = payload.get("category")
    indicator = payload.get("indicator")
    score_mode = payload.get("score_mode")
    weight = payload.get("weight", 1)

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
# ✅ 3) SAVE custom rules (alleen als mode=custom)
# =========================================================
@router.post("/indicator_config/custom")
def save_custom_rules(
    payload: dict,
    current_user: dict = Depends(get_current_user),
):
    category = payload.get("category")
    indicator = payload.get("indicator")
    rules = payload.get("rules") or []
    weight = payload.get("weight", 1)

    if not category or not indicator:
        raise HTTPException(status_code=400, detail="category en indicator verplicht")

    if not isinstance(rules, list) or len(rules) == 0:
        raise HTTPException(status_code=400, detail="rules must be a non-empty list")

    table = _get_table(category)

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="DB niet beschikbaar")

    try:
        with conn.cursor() as cur:
            # 1) bestaande rows deactiveren (zodat custom echt de bron is)
            cur.execute(
                f"""
                UPDATE {table}
                SET is_active=false
                WHERE indicator=%s
                """,
                (indicator,),
            )

            # 2) custom rows toevoegen (active=true)
            for r in rules:
                cur.execute(
                    f"""
                    INSERT INTO {table}
                    (indicator, range_min, range_max, score, trend, interpretation, action, score_mode, is_active, weight)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,'custom',true,%
