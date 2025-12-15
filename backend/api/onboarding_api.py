import logging
from datetime import datetime, timezone
from typing import Dict, List

from fastapi import APIRouter, Depends, Request, HTTPException
from pydantic import BaseModel

from backend.utils.db import get_db_connection
from backend.utils.auth_utils import get_current_user

router = APIRouter()
logger = logging.getLogger("onboarding")

# ======================================================
# Onboarding flow definities
# ======================================================
DEFAULT_FLOW = "default"

# Let op: volgorde in UI kan anders zijn, maar backend accepteert deze set keys
DEFAULT_STEPS: List[str] = [
    "setup",
    "technical",
    "macro",
    "market",
    "strategy",
]

PIPELINE_STEP = "strategy"  # 🔥 ENIGE BRON VAN WAARHEID voor pipeline_started

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

    rows = [(user_id, DEFAULT_FLOW, s, False, None, None, False) for s in missing]

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
            SELECT step_key, completed, pipeline_started
            FROM onboarding_steps
            WHERE user_id = %s AND flow = %s
            """,
            (user_id, DEFAULT_FLOW),
        )
        rows = cur.fetchall()

    completed = {r[0]: r[1] for r in rows}

    # pipeline_started is alleen relevant op de PIPELINE_STEP row
    pipeline_started = any(
        r[2] for r in rows if r[0] == PIPELINE_STEP
    )

    status = {STEP_FLAG_MAP[s]: completed.get(s, False) for s in DEFAULT_STEPS}

    status["onboarding_complete"] = all(status[STEP_FLAG_MAP[s]] for s in DEFAULT_STEPS)
    status["pipeline_started"] = pipeline_started

    return status


# ======================================================
# 🔥 PIPELINE START — EXACT 1x (DB LOCK SAFE)
# ======================================================
def _kickstart_user_pipeline(conn, user_id: int):
    """
    Start de pipeline exact 1x.
    We locken alleen de strategy-row (PIPELINE_STEP).
    """
    _ensure_steps_for_user(conn, user_id)

    from backend.celery_task.onboarding_task import run_onboarding_pipeline

    with conn.cursor() as cur:
        # 1) lock de strategy row
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
            logger.warning(f"⚠️ Geen {PIPELINE_STEP}-step voor user_id={user_id}")
            return

        strategy_completed, pipeline_started = row

        # 2) extra safety: check of ALLES completed is
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
            f"🧪 onboarding pipeline check user={user_id} "
            f"strategy_completed={strategy_completed} all_completed={all_completed} "
            f"pipeline_started={pipeline_started}"
        )

        if not all_completed or not strategy_completed or pipeline_started:
            return

        # 3) markeer pipeline gestart (on the strategy row only)
        cur.execute(
            """
            UPDATE onboarding_steps
            SET pipeline_started = TRUE
            WHERE user_id = %s
              AND flow = %s
              AND step_key = %s
            """,
            (user_id, DEFAULT_FLOW, PIPELINE_STEP),
        )

    conn.commit()

    # Celery task NA commit
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

    if step not in DEFAULT_STEPS:
        raise HTTPException(status_code=400, detail=f"Invalid step: {step}")

    mark_step_completed(conn, uid, step)

    # pipeline check is altijd safe (start alleen als alles klopt)
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
    return _get_status_dict(conn, uid)
