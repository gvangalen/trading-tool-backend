import logging
from datetime import datetime, timezone
from typing import Dict, List

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel

from backend.utils.db import get_db_connection
from backend.utils.auth_utils import get_current_user

router = APIRouter()
logger = logging.getLogger("onboarding")

logger.info("🚀 onboarding_api.py geladen – DEBUG MODE ACTIVATED")


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
# HELPERS — EXTRA LOGGING TOEGEVOEGD
# ================================================
def _ensure_steps_for_user(conn, user_id: int):
    logger.debug(f"🧩 _ensure_steps_for_user(user_id={user_id})")

    with conn.cursor() as cur:
        cur.execute("""
            SELECT step_key
            FROM onboarding_steps
            WHERE user_id = %s AND flow = %s
        """, (user_id, DEFAULT_FLOW))

        existing = {row[0] for row in cur.fetchall()}

    logger.debug(f"   → bestaande stappen: {existing}")

    missing = [s for s in DEFAULT_STEPS if s not in existing]
    logger.debug(f"   → missende stappen: {missing}")

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
    logger.info(f"   → Missende onboarding steps toegevoegd voor user {user_id}")


def mark_step_completed(conn, user_id: int, step_key: str):
    logger.info(f"✔️ Step completed aangevraagd: user={user_id}, step={step_key}")

    now = datetime.now(timezone.utc)

    with conn.cursor() as cur:
        cur.execute("""
            UPDATE onboarding_steps
            SET completed = TRUE,
                completed_at = %s
            WHERE user_id = %s AND flow = %s AND step_key = %s
        """, (now, user_id, DEFAULT_FLOW, step_key))

    conn.commit()
    logger.info(f"   → Step '{step_key}' gemarkeerd als voltooid.")


def _get_data_presence(conn, user_id: int) -> Dict[str, bool]:
    logger.debug(f"📊 _get_data_presence(user_id={user_id})")

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

    logger.debug(f"   → presence check: {presence}")
    return presence


def _get_status_dict(conn, user_id: int):
    logger.info(f"🔎 Onboarding status opvragen voor user_id={user_id}")

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
    logger.debug(f"   → DB flags: {step_flags}")

    presence = _get_data_presence(conn, user_id)

    status = {}

    # Map naar frontend structuur
    for step in DEFAULT_STEPS:
        key = STEP_FLAG_MAP[step]
        status[key] = step_flags.get(step, False) or presence.get(step, False)

    status["onboarding_complete"] = all(status[STEP_FLAG_MAP[s]] for s in DEFAULT_STEPS)

    logger.info(f"   → Status result = {status}")

    return status


# ================================================
# ENDPOINTS — NU MET EXTRA LOGGING
# ================================================
@router.get("/onboarding/status")
def get_onboarding_status(
    request: Request,
    conn=Depends(get_db_connection),
    current_user=Depends(get_current_user)
):
    logger.info(f"📥 GET /onboarding/status by user={current_user['id']}")
    logger.debug(f"   Cookies ontvangen: {request.cookies}")
    return _get_status_dict(conn, current_user["id"])


@router.post("/onboarding/complete_step")
def complete_step(
    payload: StepRequest,
    conn=Depends(get_db_connection),
    current_user=Depends(get_current_user)
):
    logger.info(f"📥 POST /onboarding/complete_step user={current_user['id']} step={payload.step}")
    mark_step_completed(conn, current_user["id"], payload.step)
    return _get_status_dict(conn, current_user["id"])


@router.post("/onboarding/finish")
def finish_onboarding(
    conn=Depends(get_db_connection),
    current_user=Depends(get_current_user)
):
    logger.info(f"📥 POST /onboarding/finish user={current_user['id']}")

    uid = current_user["id"]
    now = datetime.now(timezone.utc)

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
    logger.info(f"📥 POST /onboarding/reset user={current_user['id']}")
    uid = current_user["id"]

    with conn.cursor() as cur:
        cur.execute("""
            UPDATE onboarding_steps
            SET completed = FALSE, completed_at = NULL
            WHERE user_id = %s AND flow = %s
        """, (uid, DEFAULT_FLOW))

    conn.commit()

    return _get_status_dict(conn, uid)
