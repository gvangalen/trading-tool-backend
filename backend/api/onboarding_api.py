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

# Mapping van logische step → flagnaam in JSON
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
    """
    Zorg dat voor deze user & flow alle DEFAULT_STEPS in onboarding_steps staan.
    """
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
        rows = [(user_id, flow, step, False, None, now) for step in missing]

        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO onboarding_steps (
                    user_id,
                    flow,
                    step_key,
                    completed,
                    completed_at,
                    created_at
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                rows,
            )
        conn.commit()

        logger.info(
            "Onboarding: ontbrekende stappen %s toegevoegd voor user %s",
            missing,
            user_id,
        )


# =========================================================
# 🔧 Helper: check echte data in de kern-tabellen
#   → hybride model: flags + “data presence”
# =========================================================
def _get_data_presence(conn, user_id: int) -> Dict[str, bool]:
    """
    Check in de echte tabellen of er al data is voor deze user.
    Dit wordt gebruikt om onboarding-stappen automatisch als 'gedaan'
    te markeren als er al data bestaat.
    """
    presence = {
        "setup": False,
        "technical": False,
        "macro": False,
        "market": False,
        "strategy": False,
    }

    with conn.cursor() as cur:
        # 🟦 1. Setups
        cur.execute(
            "SELECT COUNT(*) FROM setups WHERE user_id = %s",
            (user_id,),
        )
        presence["setup"] = (cur.fetchone()[0] or 0) > 0

        # 🟦 2. Technische indicatoren
        cur.execute(
            "SELECT COUNT(*) FROM technical_indicators WHERE user_id = %s",
            (user_id,),
        )
        presence["technical"] = (cur.fetchone()[0] or 0) > 0

        # 🟦 3. Macro-data
        cur.execute(
            "SELECT COUNT(*) FROM macro_data WHERE user_id = %s",
            (user_id,),
        )
        presence["macro"] = (cur.fetchone()[0] or 0) > 0

        # 🟦 4. Market-data
        cur.execute(
            "SELECT COUNT(*) FROM market_data WHERE user_id = %s",
            (user_id,),
        )
        presence["market"] = (cur.fetchone()[0] or 0) > 0

        # 🟦 5. Strategieën
        cur.execute(
            "SELECT COUNT(*) FROM strategies WHERE user_id = %s",
            (user_id,),
        )
        presence["strategy"] = (cur.fetchone()[0] or 0) > 0

    return presence


# =========================================================
# 🔧 Helper: bouw status JSON voor frontend (optie 2 – hybride)
# =========================================================
def _get_status_dict(conn, user_id: int, flow: str = DEFAULT_FLOW):
    """
    Bouw de JSON-status voor de frontend.
    - combineert onboarding_steps flags
    - én presence in setups / macro / technical / market / strategies
    - synchroniseert onboarding_steps automatisch als er data is
    """
    _ensure_steps_for_user(conn, user_id, flow)

    # 1️⃣ Lees alle onboarding_steps
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

    # Maak dict van flags (zoals opgeslagen in onboarding_steps)
    step_flags = {step_key: bool(completed) for step_key, completed in rows}

    # 2️⃣ Lees dat-aanwezigheid in de echte tabellen
    presence = _get_data_presence(conn, user_id)

    # 3️⃣ Combineer flags + presence tot 'final' status
    status_dict = {
        "has_setup": False,
        "has_technical": False,
        "has_macro": False,
        "has_market": False,
        "has_strategy": False,
        "onboarding_complete": False,
        "steps": [],
    }

    # Om te detecteren of we onboarding_steps moeten bijwerken
    steps_to_autocomplete: List[str] = []

    for step_key in DEFAULT_STEPS:
        flag_name = STEP_FLAG_MAP.get(step_key)
        if not flag_name:
            continue

        from_flag = step_flags.get(step_key, False)
        from_data = presence.get(step_key, False)

        # Final waarde: als óf flag óf data aanwezig is → True
        final_value = bool(from_flag or from_data)
        status_dict[flag_name] = final_value

        # Bewaar voor steps[]
        status_dict["steps"].append(
            {
                "step": step_key,
                "completed": final_value,
                "from_flag": from_flag,
                "from_data": from_data,
            }
        )

        # Als er data is maar flag nog False → auto-complete in DB
        if from_data and not from_flag:
            steps_to_autocomplete.append(step_key)

    # 4️⃣ Onboarding compleet als alle stappen True zijn
    status_dict["onboarding_complete"] = all(
        status_dict[STEP_FLAG_MAP[s]] for s in DEFAULT_STEPS
    )

    # 5️⃣ Synchroniseer onboarding_steps → auto TRUE voor stappen met data
    if steps_to_autocomplete:
        now = datetime.now(timezone.utc)
        with conn.cursor() as cur:
            for step_key in steps_to_autocomplete:
                cur.execute(
                    """
                    UPDATE onboarding_steps
                    SET completed = TRUE,
                        completed_at = %s
                    WHERE user_id = %s AND flow = %s AND step_key = %s
                    """,
                    (now, user_id, flow, step_key),
                )
        conn.commit()
        logger.info(
            "Onboarding: auto-complete stappen %s voor user %s (data aanwezig).",
            steps_to_autocomplete,
            user_id,
        )

    return status_dict


# =========================================================
# 🔍 GET /api/onboarding/status
# =========================================================
@router.get("/onboarding/status")
def get_onboarding_status(
    conn=Depends(get_db_connection),
    current_user=Depends(get_current_user),
):
    """
    Haal de hybride onboarding-status op.
    Wordt gebruikt door:
      - onboarding pagina
      - middleware (toegang tot /dashboard)
    """
    user_id = current_user["id"] if isinstance(current_user, dict) else current_user.id
    return _get_status_dict(conn, user_id)


# =========================================================
# ✅ POST /api/onboarding/complete_step
#    (optioneel – handmatig markeren als done)
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

    logger.info(
        "Onboarding: step '%s' handmatig voltooid voor user %s",
        step,
        user_id,
    )

    return _get_status_dict(conn, user_id)


# =========================================================
# 🏁 POST /api/onboarding/finish
#    → zet ALLE stappen op completed = TRUE
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

    logger.info("Onboarding: finish_onboarding aangeroepen voor user %s", user_id)

    return _get_status_dict(conn, user_id)


# =========================================================
# 🔄 POST /api/onboarding/reset  (dev only)
# =========================================================
@router.post("/onboarding/reset")
def reset_onboarding(
    conn=Depends(get_db_connection),
    current_user=Depends(get_current_user),
):
    """
    Reset alle onboarding-flags naar False.
    LET OP: data in setups/indicators blijft bestaan,
    dus bij volgende status-call worden stappen weer auto-completed
    als er al data is.
    """
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

    logger.info("Onboarding: reset voor user %s", user_id)

    return _get_status_dict(conn, user_id)
