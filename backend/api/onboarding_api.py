import logging
from datetime import datetime, timezone
from typing import Dict, List

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel

from backend.utils.db import get_db_connection
from backend.utils.auth_utils import get_current_user

router = APIRouter()
logger = logging.getLogger("onboarding")

# ----------------------------------------------
# Onboarding flow definities
# ----------------------------------------------
DEFAULT_FLOW = "default"

DEFAULT_STEPS: List[str] = [
    "setup",
    "technical",
    "macro",
    "market",
    "strategy",
]

STEP_FLAG_MAP = {
    "setup": "has_setup",
    "technical": "has_technical",
    "macro": "has_macro",
    "market": "has_market",
    "strategy": "has_strategy",
}


class StepRequest(BaseModel):
    step: str


# ======================================================
# Zorg dat user alle onboarding stappen heeft in DB
# ======================================================
def _ensure_steps_for_user(conn, user_id: int):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT step_key FROM onboarding_steps
            WHERE user_id = %s AND flow = %s
        """, (user_id, DEFAULT_FLOW))

        existing = {row[0] for row in cur.fetchall()}

    missing = [s for s in DEFAULT_STEPS if s not in existing]

    if not missing:
        return

    rows = [(user_id, DEFAULT_FLOW, s, False, None, None) for s in missing]

    with conn.cursor() as cur:
        cur.executemany("""
            INSERT INTO onboarding_steps
                (user_id, flow, step_key, completed, completed_at, metadata)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, rows)

    conn.commit()


# ======================================================
# MARK STEP COMPLETED
# ======================================================
def mark_step_completed(conn, user_id: int, step_key: str):
    now = datetime.now(timezone.utc)

    with conn.cursor() as cur:
        cur.execute("""
            UPDATE onboarding_steps
            SET completed = TRUE,
                completed_at = %s
            WHERE user_id = %s AND flow = %s AND step_key = %s
        """, (now, user_id, DEFAULT_FLOW, step_key))

    conn.commit()


# ======================================================
# GET STATUS (alleen op basis van onboarding_steps)
# ======================================================
def _get_status_dict(conn, user_id: int) -> Dict[str, bool]:
    _ensure_steps_for_user(conn, user_id)

    with conn.cursor() as cur:
        cur.execute("""
            SELECT step_key, completed
            FROM onboarding_steps
            WHERE user_id = %s AND flow = %s
        """, (user_id, DEFAULT_FLOW))

        rows = cur.fetchall()

    flags = {step: done for step, done in rows}

    status = {
        STEP_FLAG_MAP[s]: flags.get(s, False)
        for s in DEFAULT_STEPS
    }

    status["onboarding_complete"] = all(
        status[STEP_FLAG_MAP[s]] for s in DEFAULT_STEPS
    )

    return status


# ======================================================
# ROUTES
# ======================================================
@router.get("/onboarding/status")
def get_onboarding_status(
    request: Request,
    conn=Depends(get_db_connection),
    current_user=Depends(get_current_user)
):
    return _get_status_dict(conn, current_user["id"])


@router.post("/onboarding/complete_step")
def complete_step(
    payload: StepRequest,
    conn=Depends(get_db_connection),
    current_user=Depends(get_current_user)
):
    mark_step_completed(conn, current_user["id"], payload.step)
    return _get_status_dict(conn, current_user["id"])


@router.post("/onboarding/finish")
def finish_onboarding(
    conn=Depends(get_db_connection),
    current_user=Depends(get_current_user)
):
    now = datetime.now(timezone.utc)
    uid = current_user["id"]

    with conn.cursor() as cur:
        cur.execute("""
            UPDATE onboarding_steps
            SET completed = TRUE, completed_at = %s
            WHERE user_id = %s AND flow = %s
        """, (now, uid, DEFAULT_FLOW))

    conn.commit()
    return _get_status_dict(conn, uid)


@router.post("/onboarding/reset")
def reset_onboarding(
    conn=Depends(get_db_connection),
    current_user=Depends(get_current_user)
):
    uid = current_user["id"]

    with conn.cursor() as cur:
        cur.execute("""
            UPDATE onboarding_steps
            SET completed = FALSE,
                completed_at = NULL
            WHERE user_id = %s AND flow = %s
        """, (uid, DEFAULT_FLOW))

    conn.commit()
    return _get_status_dict(conn, uid)
