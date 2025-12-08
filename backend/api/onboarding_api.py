import logging
from datetime import datetime, timezone
from typing import Dict, List

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel

from backend.utils.db import get_db_connection
from backend.api.auth_api import get_current_user

# =========================================================
# ⚙️ Router setup (jouw standaard stijl)
# =========================================================
router = APIRouter()
logger = logging.getLogger(__name__)
logger.info("🚀 onboarding_api.py geladen – onboarding-systeem actief.")

# =========================================================
# 📌 Config – flow + onboarding stappen
# =========================================================
DEFAULT_FLOW = "default"

DEFAULT_STEPS: List[str] = [
    "setup",
    "technical",
    "macro",
    "market",
    "strategy",
]

STEP_FLAG_MAP: Dict[str, str] = {
    "setup": "has_setup",
    "technical": "has_technical",
    "macro": "has_macro",
    "market": "has_market",
    "strategy": "has_strategy",
}


# =========================================================
# 📝 Pydantic models
# =========================================================
class StepRequest(BaseModel):
    step: str


# =========================================================
# 🔧 Helper: zorg dat alle onboarding-stappen bestaan
# =========================================================
def _ensure_steps_for_user(conn, user_id: int, flow: str = DEFAULT_FLOW):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT step_key
            FROM onboarding_steps
            WHERE user_id = %s AND flow = %s
            """,
            (user_id, flow),
        )
        existing = {row[0] for row in cur.fetchall()}

        missing = [s for s in DEFAULT_STEPS if s not in existing]

    if missing:
        now = datetime.now(timezone.utc)
        rows = [(user_id, flow, step, False, None) for step in missing]

        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO onboarding_steps (user_id, flow, step_key, completed, completed_at)
                VALUES (%s, %s, %s, %s, %s)
                """,
                rows,
            )
        conn.commit()

        logger.info(f"Onboarding: toegevoegde ontbrekende stappen {missing} voor user {user_id}")


# =========================================================
# 🔧 Helper: bouw status JSON voor frontend
# =========================================================
def _get_status_dict(conn, user_id: int, flow: str = DEFAULT_FLOW):
    _ensure_steps_for_user(conn, user_id, flow)

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT step_key, completed
            FROM onboarding_steps
            WHERE user_id = %s AND flow = %s
            """,
            (user_id, flow),
        )
        rows = cur.fetchall()

    status_dict = {
        "has_setup": False,
        "has_technical": False,
        "has_macro": False,
        "has_market": False,
        "has_strategy": False,
        "onboarding_complete": False,
        "steps": [],
    }

    for step_key, completed in rows:
        flag = STEP_FLAG_MAP.get(step_key)
        if flag:
            status_dict[flag] = bool(completed)

        status_dict["steps"].append({
            "step": step_key,
            "completed": bool(completed)
        })

    # Onboarding klaar?
    status_dict["onboarding_complete"] = all(
        status_dict[STEP_FLAG_MAP[s]] for s in DEFAULT_STEPS
    )

    return status_dict


# =========================================================
# 🔍 GET /onboarding/status
# =========================================================
@router.get("/onboarding/status")
def get_onboarding_status(
    conn=Depends(get_db_connection),
    current_user=Depends(get_current_user),
):
    user_id = current_user["id"] if isinstance(current_user, dict) else current_user.id
    return _get_status_dict(conn, user_id)


# =========================================================
# ✅ POST /onboarding/complete_step
# =========================================================
@router.post("/onboarding/complete_step")
def complete_onboarding_step(
    payload: StepRequest,
    conn=Depends(get_db_connection),
    current_user=Depends(get_current_user),
):
    step = payload.step.strip().lower()

    if step not in DEFAULT_STEPS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Ongeldige onboarding-stap: {step}",
        )

    user_id = current_user["id"] if isinstance(current_user, dict) else current_user.id

    _ensure_steps_for_user(conn, user_id)

    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE onboarding_steps
            SET completed = TRUE,
                completed_at = %s
            WHERE user_id = %s
              AND flow = %s
              AND step_key = %s
            """,
            (datetime.now(timezone.utc), user_id, DEFAULT_FLOW, step),
        )
    conn.commit()

    return _get_status_dict(conn, user_id)


# =========================================================
# 🏁 POST /onboarding/finish
# =========================================================
@router.post("/onboarding/finish")
def finish_onboarding(
    conn=Depends(get_db_connection),
    current_user=Depends(get_current_user),
):
    user_id = current_user["id"] if isinstance(current_user, dict) else current_user.id

    _ensure_steps_for_user(conn, user_id)

    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE onboarding_steps
            SET completed = TRUE,
                completed_at = %s
            WHERE user_id = %s AND flow = %s
            """,
            (datetime.now(timezone.utc), user_id, DEFAULT_FLOW),
        )
    conn.commit()

    return _get_status_dict(conn, user_id)


# =========================================================
# 🔄 POST /onboarding/reset (dev only)
# =========================================================
@router.post("/onboarding/reset")
def reset_onboarding(
    conn=Depends(get_db_connection),
    current_user=Depends(get_current_user),
):
    user_id = current_user["id"] if isinstance(current_user, dict) else current_user.id

    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE onboarding_steps
            SET completed = FALSE,
                completed_at = NULL
            WHERE user_id = %s AND flow = %s
            """,
            (user_id, DEFAULT_FLOW),
        )
    conn.commit()

    return _get_status_dict(conn, user_id)
