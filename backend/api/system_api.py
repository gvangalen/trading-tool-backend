from fastapi import APIRouter, Depends
from backend.auth.dependencies import get_current_user
from backend.tasks.bootstrap_agents_task import bootstrap_agents_task

router = APIRouter()


@router.post("/system/bootstrap-agents")
def bootstrap_agents(current_user=Depends(get_current_user)):

    bootstrap_agents_task.delay(current_user.id)

    return {"status": "bootstrap_started"}
