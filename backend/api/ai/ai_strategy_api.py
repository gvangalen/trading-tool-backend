# ✅ ai_strategy_api.py — FastAPI-versie
from fastapi import APIRouter, HTTPException, Request
from utils.ai_strategy_utils import generate_strategy_from_setup
import logging

router = APIRouter(prefix="/ai/strategy")
logger = logging.getLogger(__name__)

@router.post("/ai/strategy")
async def generate_strategy(request: Request):
    try:
        setup = await request.json()
        if not setup:
            raise HTTPException(status_code=400, detail="Geen setup ontvangen")

        strategy = generate_strategy_from_setup(setup)
        if not strategy:
            raise HTTPException(status_code=500, detail="Strategie-generatie mislukt")

        return strategy

    except Exception as e:
        logger.error(f"❌ Fout bij AI-strategiegeneratie: {e}")
        raise HTTPException(status_code=500, detail=str(e))
