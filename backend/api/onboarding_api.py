import logging
from datetime import datetime, timezone
from typing import Dict, List

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from backend.utils.db import get_db_connection
from backend.utils.auth_utils import get_current_user

router = APIRouter()
logger = logging.getLogger(__name__)
logger.info("🚀 onboarding_api.py geladen – onboarding actief.")

# ================================================
# CONFIG
# ================================================
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

class StepRequest(BaseModel):
    step: str


# ================================================
# HELPERS
# ================================================
def _ensure_steps_for_user(conn, user_id: int):
    """
    Zorgt dat alle benodigde onboarding_steps bestaan voor deze user.
    Gebaseerd op JOUW echte tabelstructuur.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT step_key
            FROM onboarding_steps
            WHERE user_id = %s AND flow = %s
        """, (user_id, DEFAULT_FLOW))

        existing = {row[0] for row in cur.fetchall()}

    missing = [s for s in DEFAULT_STEPS if s not in existing]

    if not missing:
        return

    rows = [
        (user_id, DEFAULT_FLOW, s, False, None, None)
        for s in missing
    ]

    with conn.cursor() as cur:
        cur.executemany("""
            INSERT INTO onboarding_steps
                (user_id, flow, step_key, completed, completed_at, metadata)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, rows)

    conn.commit()


def mark_step_completed(conn, user_id: int, step_key: str):
    if step_key not in STEP_FLAG_MAP:
        raise HTTPException(400, f"Step '{step_key}' bestaat niet")

    now = datetime.now(timezone.utc)

    with conn.cursor() as cur:
        cur.execute("""
            UPDATE onboarding_steps
            SET completed = TRUE,
                completed_at = %s
            WHERE user_id = %s AND flow = %s AND step_key = %s
        """, (now, user_id, DEFAULT_FLOW, step_key))

    conn.commit()


def _get_data_presence(conn, user_id: int) -> Dict[str, bool]:
    """
    Controleert of de user al data heeft toegevoegd die onboarding automatisch zou kunnen voltooien.
    """

    presence = {s: False for s in DEFAULT_STEPS}

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM setups WHERE user_id = %s", (user_id,))
        presence["setup"] = cur.fetchone()[0] > 0

        cur.execute("SELECT COUNT(*) FROM technical_indicators WHERE user_id = %s", (user_id,))
        presence["technical"] = cur.fetchone()[0] > 0

        cur.execute("SELECT COUNT(*) FROM macro_data WHERE user_id = %s", (user_id,))
        presence["macro"] = cur.fetchone()[0] > 0

        cur.execute("SELECT COUNT(*) FROM market_data WHERE user_id = %s", (user_id,))
        presence["market"] = cur.fetchone()[0] > 0

        cur.execute("SELECT COUNT(*) FROM strategies WHERE user_id = %s", (user_id,))
        presence["strategy"] = cur.fetchone()[0] > 0

    return presence


def _get_status_dict(conn, user_id: int):
    """
    Combineert database flags + presence en bouwt een nette status-structuur.
    """
    _ensure_steps_for_user(conn, user_id)

    # Load onboarding flags
    with conn.cursor() as cur:
        cur.execute("""
            SELECT step_key, completed
            FROM onboarding_steps
            WHERE user_id = %s AND flow = %s
        """, (user_id, DEFAULT_FLOW))
        fetched = cur.fetchall()

    step_flags = {key: done for key, done in fetched}
    presence = _get_data_presence(conn, user_id)

    status = {flag: False for flag in STEP_FLAG_MAP.values()}
    status["steps"] = []

    for step in DEFAULT_STEPS:
        flag = STEP_FLAG_MAP[step]

        # Final value = DB-completed OR user has relevant data
        final_val = step_flags.get(step, False) or presence.get(step, False)

        status[flag] = final_val

        status["steps"].append({
            "step": step,
            "completed": final_val,
            "from_flag": step_flags.get(step, False),
            "from_data": presence.get(step, False),
        })

    status["onboarding_complete"] = all(
        status[STEP_FLAG_MAP[s]] for s in DEFAULT_STEPS
    )

    return status


# ================================================
# ENDPOINTS
# ================================================
@router.get("/onboarding/status")
def get_onboarding_status(
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
    step = payload.step.lower()

    if step not in DEFAULT_STEPS:
        raise HTTPException(400, f"Ongeldige step '{step}'")

    mark_step_completed(conn, current_user["id"], step)

    return _get_status_dict(conn, current_user["id"])


@router.post("/onboarding/finish")
def finish_onboarding(
    conn=Depends(get_db_connection),
    current_user=Depends(get_current_user)
):
    uid = current_user["id"]
    now = datetime.now(timezone.utc)

    with conn.cursor() as cur:
        cur.execute("""
            UPDATE onboarding_steps
            SET completed = TRUE,
                completed_at = %s
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
