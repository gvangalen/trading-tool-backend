import logging
from datetime import datetime, timezone
from typing import Dict, List

from fastapi import APIRouter, Depends, Request, HTTPException
from pydantic import BaseModel

from backend.utils.db import get_db_connection
from backend.utils.auth_utils import get_current_user

router = APIRouter()
logger = logging.getLogger("onboarding")

# =====================================================
# Onboarding flow definities
# =====================================================

DEFAULT_FLOW = "default"

DEFAULT_STEPS: List[str] = [
    "market",
    "macro",
    "technical",
    "setup",
    "strategy",
]

PIPELINE_STEP = "strategy"

STEP_FLAG_MAP = {
    "market": "has_market",
    "macro": "has_macro",
    "technical": "has_technical",
    "setup": "has_setup",
    "strategy": "has_strategy",
}


class StepRequest(BaseModel):
    step: str


# =====================================================
# Ensure steps bestaan voor user
# =====================================================

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
    logger.info(f"[Onboarding] Steps aangemaakt voor user_id={user_id}: {missing}")


# =====================================================
# Status berekenen
# =====================================================

def _get_status_dict(conn, user_id: int) -> Dict[str, bool]:
    _ensure_steps_for_user(conn, user_id)

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT step_key, completed, pipeline_started
            FROM onboarding_steps
            WHERE user_id = %s AND flow = %s
            """,
            (user_id, DEFAULT_FLOW),
        )
        rows = cur.fetchall()

    completed = {r[0]: r[1] for r in rows}
    pipeline_started = any(
        r[2] for r in rows if r[0] == PIPELINE_STEP
    )

    status = {
        STEP_FLAG_MAP[s]: completed.get(s, False)
        for s in DEFAULT_STEPS
    }

    status["onboarding_complete"] = all(status.values())
    status["pipeline_started"] = pipeline_started

    logger.info(
        f"[Onboarding] Status user_id={user_id} "
        f"completed={completed} pipeline_started={pipeline_started}"
    )

    return status


# =====================================================
# Pipeline starten (exact 1x)
# =====================================================

def _kickstart_user_pipeline(conn, user_id: int):
    from backend.celery_task.onboarding_task import run_onboarding_pipeline

    _ensure_steps_for_user(conn, user_id)

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT completed, pipeline_started
            FROM onboarding_steps
            WHERE user_id = %s
              AND flow = %s
              AND step_key = %s
            FOR UPDATE
            """,
            (user_id, DEFAULT_FLOW, PIPELINE_STEP),
        )
        row = cur.fetchone()

        if not row:
            logger.warning(f"[Onboarding] Geen strategy-step voor user_id={user_id}")
            return

        strategy_completed, pipeline_started = row

        cur.execute(
            """
            SELECT step_key, completed
            FROM onboarding_steps
            WHERE user_id = %s AND flow = %s
            """,
            (user_id, DEFAULT_FLOW),
        )
        all_rows = cur.fetchall()
        completed_map = {k: v for k, v in all_rows}
        all_completed = all(completed_map.get(s, False) for s in DEFAULT_STEPS)

        logger.info(
            f"[Onboarding] Pipeline check user_id={user_id} "
            f"strategy_completed={strategy_completed} "
            f"all_completed={all_completed} "
            f"pipeline_started={pipeline_started}"
        )

        if not all_completed or not strategy_completed or pipeline_started:
            return

        cur.execute(
            """
            UPDATE onboarding_steps
            SET pipeline_started = TRUE
            WHERE user_id = %s AND flow = %s AND step_key = %s
            """,
            (user_id, DEFAULT_FLOW, PIPELINE_STEP),
        )

    conn.commit()

    run_onboarding_pipeline.delay(user_id)
    logger.info(f"[Onboarding] Pipeline gestart voor user_id={user_id}")


# =====================================================
# Step afronden
# =====================================================

def mark_step_completed(conn, user_id: int, step_key: str):
    if step_key not in DEFAULT_STEPS:
        raise ValueError(f"Ongeldige step: {step_key}")

    now = datetime.now(timezone.utc)

    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE onboarding_steps
            SET completed = TRUE,
                completed_at = %s
            WHERE user_id = %s AND flow = %s AND step_key = %s
            """,
            (now, user_id, DEFAULT_FLOW, step_key),
        )

    conn.commit()
    logger.info(f"[Onboarding] Step '{step_key}' voltooid voor user_id={user_id}")


# =====================================================
# Routes
# =====================================================

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

    if step not in DEFAULT_STEPS:
        raise HTTPException(status_code=400, detail=f"Invalid step: {step}")

    mark_step_completed(conn, uid, step)
    _kickstart_user_pipeline(conn, uid)

    return _get_status_dict(conn, uid)


@router.post("/onboarding/finish")
def finish_onboarding(
    conn=Depends(get_db_connection),
    current_user=Depends(get_current_user),
):
    uid = current_user["id"]
    now = datetime.now(timezone.utc)

    _ensure_steps_for_user(conn, uid)

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

    _kickstart_user_pipeline(conn, uid)
    return _get_status_dict(conn, uid)


@router.post("/onboarding/reset")
def reset_onboarding(
    conn=Depends(get_db_connection),
    current_user=Depends(get_current_user),
):
    uid = current_user["id"]

    _ensure_steps_for_user(conn, uid)

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

    logger.info(f"[Onboarding] Reset uitgevoerd voor user_id={uid}")
    return _get_status_dict(conn, uid)
