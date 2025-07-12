from fastapi import APIRouter

router = APIRouter()

@router.get("/status")
def ai_status():
    return {
        "status": "actief",
        "strategie": "DCA + Swing",
        "laatste_update": "2025-07-12T13:00:00"
    }
