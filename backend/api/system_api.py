import os
import logging
from dotenv import load_dotenv
from fastapi import APIRouter, HTTPException, Depends

from backend.utils.auth_utils import get_current_user
from backend.celery_task.bootstrap_agents_task import bootstrap_agents_task

logger = logging.getLogger(__name__)
router = APIRouter()

dotenv_path = os.path.join(os.path.dirname(__file__), "..", ".env")
load_dotenv(dotenv_path=dotenv_path)

logger.info("⚙️ system_api.py geladen – System endpoints.")


# =====================================================
# 🚀 BOOTSTRAP AGENTS (na onboarding)
# =====================================================
@router.post("/system/bootstrap-agents")
def bootstrap_agents(current_user=Depends(get_current_user)):

    try:

        user_id = current_user["id"]

        logger.info(f"🚀 Bootstrap agents gestart voor user {user_id}")

        bootstrap_agents_task.delay(user_id)

        return {
            "status": "started",
            "message": "AI agents worden geïnitialiseerd",
            "user_id": user_id,
        }

    except Exception as e:

        logger.exception("❌ Bootstrap agents mislukt")

        raise HTTPException(
            status_code=500,
            detail="Bootstrap agents starten mislukt",
        )
