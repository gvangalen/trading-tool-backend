import logging
from datetime import datetime, timezone
from typing import Dict, List

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel

from backend.utils.db import get_db_connection
from backend.api.auth_api import get_current_user

# =========================================================
# ⚙️ Router setup
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
# 🔧 Helper — externe functie voor andere API’s
# =========================================================
def mark_step_completed(conn, user_id: int, step_key: str):
    if step_key not in STEP_FLAG_MAP:
        logger.warning(f"⚠️ mark_step_completed: ongeldige step '{step_key}'")
        return

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
    logger.info(f"✅ Onboarding stap '{step_key}' automatisch voltooid voor user {user_id}")


# =========================================================
# Helper: ensure steps exist for this user
# =========================================================
def _ensure_steps_for_user(conn, user_id: int, flow: str = DEFAULT_FLOW):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT step_key
            FROM onboarding_steps
            WHERE user_id = %s AND flow = %s
        """, (user_id, flow))
        existing = {row[0] for row in cur.fetchall()}

    missing = [s for s in DEFAULT_STEPS if s not in existing]

    if missing:
        now = datetime.now(timezone.utc)
        rows = [(user_id, flow, step, False, None, now) for step in missing]

        with conn.cursor() as cur:
            cur.executemany("""
                INSERT INTO onboarding_steps (
                    user_id, flow, step_key, completed, completed_at, created_at
                )
                VALUES (%s, %s, %s, %s, %s, %s)
            """, rows)

        conn.commit()
        logger.info(f"Onboarding: ontbrekende steps {missing} toegevoegd voor user {user_id}")


def _get_data_presence(conn, user_id: int) -> Dict[str, bool]:
    presence = {step: False for step in DEFAULT_STEPS}

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
    _ensure_steps_for_user(conn, user_id)

    with conn.cursor() as cur:
        cur.execute("""
            SELECT step_key, completed
            FROM onboarding_steps
            WHERE user_id = %s
        """, (user_id,))
        rows = cur.fetchall()

    step_flags = {step_key: bool(done) for step_key, done in rows}
    presence = _get_data_presence(conn, user_id)

    status_dict = {
        flag: False for flag in STEP_FLAG_MAP.values()
    }
    status_dict["onboarding_complete"] = False
    status_dict["steps"] = []

    steps_to_sync = []

    for step_key in DEFAULT_STEPS:
        flag_name = STEP_FLAG_MAP[step_key]
        from_flag = step_flags.get(step_key, False)
        from_data = presence.get(step_key, False)

        final_value = from_flag or from_data
        status_dict[flag_name] = final_value

        status_dict["steps"].append({
            "step": step_key,
            "completed": final_value,
            "from_flag": from_flag,
            "from_data": from_data,
        })

        if from_data and not from_flag:
            steps_to_sync.append(step_key)

    if steps_to_sync:
        now = datetime.now(timezone.utc)
        with conn.cursor() as cur:
            for key in steps_to_sync:
                cur.execute("""
                    UPDATE onboarding_steps
                    SET completed = TRUE, completed_at = %s
                    WHERE user_id = %s AND step_key = %s
                """, (now, user_id, key))
        conn.commit()

    status_dict["onboarding_complete"] = all(
        status_dict[STEP_FLAG_MAP[s]] for s in DEFAULT_STEPS
    )

    return status_dict


# =========================================================
# 🔍 GET /onboarding/status — NIET MEER PROTECTED ✔ FIX
# =========================================================
@router.get("/onboarding/status")
def get_onboarding_status_public(request: Request):
    token = request.cookies.get("session")

    # ❗ Geen session → user is (nog) niet ingelogd → geef neutrale status
    if not token:
        return {
            "has_setup": False,
            "has_technical": False,
            "has_macro": False,
            "has_market": False,
            "has_strategy": False,
            "onboarding_complete": False,
            "steps": [],
            "anonymous": True,
        }

    # Session bestaat → echte user ophalen
    conn = get_db_connection()
    if not conn:
        raise HTTPException(500, "Databaseverbinding mislukt")

    # Auth via originele helper
    user = get_current_user(request)
    user_id = user["id"]

    return _get_status_dict(conn, user_id)


# =========================================================
# POST /onboarding/complete_step
# =========================================================
@router.post("/onboarding/complete_step")
def complete_onboarding_step(
    payload: StepRequest,
    conn=Depends(get_db_connection),
    current_user=Depends(get_current_user),
):
    step = payload.step.strip().lower()

    if step not in DEFAULT_STEPS:
        raise HTTPException(400, f"Ongeldige onboarding-stap: {step}")

    user_id = current_user["id"]
    mark_step_completed(conn, user_id, step)

    return _get_status_dict(conn, user_id)


# =========================================================
# POST /onboarding/finish
# =========================================================
@router.post("/onboarding/finish")
def finish_onboarding(conn=Depends(get_db_connection), current_user=Depends(get_current_user)):
    user_id = current_user["id"]

    with conn.cursor() as cur:
        cur.execute("""
            UPDATE onboarding_steps
            SET completed = TRUE,
                completed_at = %s
            WHERE user_id = %s
        """, (datetime.now(timezone.utc), user_id))
    conn.commit()

    return _get_status_dict(conn, user_id)


# =========================================================
# POST /onboarding/reset — dev only
# =========================================================
@router.post("/onboarding/reset")
def reset_onboarding(conn=Depends(get_db_connection), current_user=Depends(get_current_user)):
    user_id = current_user["id"]

    with conn.cursor() as cur:
        cur.execute("""
            UPDATE onboarding_steps
            SET completed = FALSE,
                completed_at = NULL
            WHERE user_id = %s
        """, (user_id,))
    conn.commit()

    return _get_status_dict(conn, user_id)
