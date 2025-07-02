from fastapi import APIRouter, HTTPException
from ai_tasks.validation_task import validate_setups_task

router = APIRouter(prefix="/setups")

# ✅ Setup-validatie starten via Celery
@router.post("/validate")
async def trigger_setup_validation():
    try:
        task = validate_setups_task.delay()
        return {
            "message": "Setup-validatie gestart.",
            "task_id": task.id
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fout bij starten van validatie: {e}")
