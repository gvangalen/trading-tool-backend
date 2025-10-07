# backend/api/onboarding_helpers.py

import logging

logger = logging.getLogger(__name__)

def mark_step_done(step: int, user_id: str) -> dict:
    step_mapping = {
        1: "setup_done",
        2: "technical_done",
        3: "macro_done",
        4: "dashboard_done"
    }

    step_key = step_mapping.get(step)
    if not step_key:
        logger.warning(f"Ongeldig stapnummer: {step}")
        return {}

    payload = {
        "step": step_key,
        "done": True,
        "user_id": user_id
    }

    logger.info(f"Stap {step} ({step_key}) voltooid voor gebruiker {user_id}.")
    return payload
