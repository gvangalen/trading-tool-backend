from fastapi import APIRouter
from backend.utils.db import get_db_connection
import logging

router = APIRouter()
logger = logging.getLogger(__name__)

@router.get("/trades/active")
def get_active_trades():
    """
    Geeft een lijst van actieve trades terug.
    Voor nu een lege placeholder, later koppelbaar aan strategieën of setups.
    """
    logger.info("🔄 API-call: /api/trades/active")
    return {
        "status": "ok",
        "active_trades": [],
        "message": "Nog geen actieve trades"
    }
