from fastapi import APIRouter, HTTPException
from backend.utils.db import get_db_connection
import logging

router = APIRouter(prefix="/sidebar")
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# ✅ Actieve trades ophalen (mockversie – later koppelen aan db)
@router.get("/active-trades")
async def get_active_trades():
    try:
        # Hier kun je later je eigen query zetten
        return [
            {"id": 1, "symbol": "BTC/USDT", "status": "Open"},
            {"id": 2, "symbol": "SOL/USDT", "status": "In Progress"},
        ]
    except Exception as e:
        logger.error(f"❌ SB01: Fout bij ophalen trades: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ✅ AI bot status ophalen (mockversie – later koppelen aan echte botstatus)
@router.get("/ai-bot-status")
async def get_ai_bot_status():
    try:
        return {
            "state": "Actief",
            "strategy": "Breakout & Volume Surge",
            "updated": "2025-06-23 10:30"
        }
    except Exception as e:
        logger.error(f"❌ SB02: Fout bij ophalen botstatus: {e}")
        raise HTTPException(status_code=500, detail=str(e))
