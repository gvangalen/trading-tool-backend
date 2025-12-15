import logging
from datetime import datetime, timezone
from typing import Dict, List

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel

from backend.utils.db import get_db_connection
from backend.utils.auth_utils import get_current_user

router = APIRouter()
logger = logging.getLogger("onboarding")

# ======================================================
# Onboarding flow definities
# ======================================================
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
# 🔒 ENSURE STEPS BESTAAN
# ======================================================
def _ensure_steps_for_user(conn, user_id: int):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT step_key
            FROM onboarding_steps
            WHERE user_id = %s AND flow = %s
            """,
            (user_id, DEFAULT_FLOW),
        )
        existing = {r[0] for r in cur.fetchall()}

    missing = [s for s in DEFAULT_STEPS if s not in existing]
    if not missing:
        return

    rows = [
        (user_id, DEFAULT_FLOW, s, False, None, None, False)
        for s in missing
    ]

    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO onboarding_steps
                (user_id, flow, step_key, completed, completed_at, metadata, pipeline_started)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            rows,
        )

    conn.commit()


# ======================================================
# 🧠 STATUS BEREKENEN
# ======================================================
def _get_status_dict(conn, user_id: int) -> Dict[str, bool]:
    _ensure_steps_for_user(conn, user_id)

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT step_key, completed
            FROM onboarding_steps
            WHERE user_id = %s AND flow = %s
            """,
            (user_id, DEFAULT_FLOW),
        )
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
# 🔥 PIPELINE START (DB-GEDREVEN, EXACT 1x)
# ======================================================
def _kickstart_user_pipeline(conn, user_id: int):
    """
    Start onboarding Celery pipeline EXACT één keer.
    Volledig DB-gedreven en race-condition safe.
    """
    from backend.celery_task.onboarding_task import run_onboarding_pipeline

    with conn.cursor() as cur:
        # 🔒 Lock alle onboarding rows voor deze user
        cur.execute(
            """
            SELECT completed, pipeline_started
            FROM onboarding_steps
            WHERE user_id = %s AND flow = %s
            FOR UPDATE
            """,
            (user_id, DEFAULT_FLOW),
        )
        rows = cur.fetchall()

        if not rows:
            logger.warning(f"⚠️ Geen onboarding_steps voor user_id={user_id}")
            return

        all_completed = all(r[0] for r in rows)
        already_started = all(r[1] for r in rows)

        logger.info(
            f"🧪 onboarding check user={user_id} "
            f"completed={all_completed} pipeline_started={already_started}"
        )

        if not all_completed or already_started:
            return

        # 🔥 Markeer pipeline gestart (ONOMKEERBAAR)
        cur.execute(
            """
            UPDATE onboarding_steps
            SET pipeline_started = TRUE
            WHERE user_id = %s AND flow = %s
            """,
            (user_id, DEFAULT_FLOW),
        )

    conn.commit()

    # 🚀 Celery task NA commit
    run_onboarding_pipeline.delay(user_id)
    logger.info(f"🚀 Onboarding pipeline gestart voor user_id={user_id}")


# ======================================================
# MARK STEP COMPLETED
# ======================================================
def mark_step_completed(conn, user_id: int, step_key: str):
    now = datetime.now(timezone.utc)

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
            (now, user_id, DEFAULT_FLOW, step_key),
        )

    conn.commit()


# ======================================================
# ROUTES
# ======================================================
@router.get("/onboarding/status")
def get_onboarding_status(
    request: Request,
    conn=Depends(get_db_connection),
    current_user=Depends(get_current_user),
):
    return _get_status_dict(conn, current_user["id"])


@router.post("/onboarding/complete_step")
def complete_step(
    payload: StepRequest,
    conn=Depends(get_db_connection),
    current_user=Depends(get_current_user),
):
    uid = current_user["id"]
    step = payload.step

    mark_step_completed(conn, uid, step)
    status = _get_status_dict(conn, uid)

    # ✅ GEEN gokwerk: altijd veilig checken
    _kickstart_user_pipeline(conn, uid)

    return status


@router.post("/onboarding/finish")
def finish_onboarding(
    conn=Depends(get_db_connection),
    current_user=Depends(get_current_user),
):
    uid = current_user["id"]
    now = datetime.now(timezone.utc)

    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE onboarding_steps
            SET completed = TRUE,
                completed_at = %s
            WHERE user_id = %s AND flow = %s
            """,
            (now, uid, DEFAULT_FLOW),
        )

    conn.commit()

    # 🔥 Altijd veilig pipeline checken
    _kickstart_user_pipeline(conn, uid)

    return _get_status_dict(conn, uid)


@router.post("/onboarding/reset")
def reset_onboarding(
    conn=Depends(get_db_connection),
    current_user=Depends(get_current_user),
):
    uid = current_user["id"]

    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE onboarding_steps
            SET completed = FALSE,
                completed_at = NULL,
                pipeline_started = FALSE
            WHERE user_id = %s AND flow = %s
            """,
            (uid, DEFAULT_FLOW),
        )

    conn.commit()
    return _get_status_dict(conn, uid)
