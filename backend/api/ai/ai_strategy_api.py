from fastapi import APIRouter, HTTPException, Request
from backend.utils.ai_strategy_utils import generate_strategy_from_setup
import logging

router = APIRouter()
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

@router.post("/strategies/generate")
async def generate_strategy(request: Request):
    """
    Genereert een AI-tradingstrategie op basis van een meegegeven setup.
    """
    try:
        setup = await request.json()

        if not setup or not isinstance(setup, dict):
            raise HTTPException(status_code=400, detail="❌ Ongeldige of ontbrekende setup-data.")

        strategy = generate_strategy_from_setup(setup)
        if not strategy:
            raise HTTPException(status_code=500, detail="❌ Strategie-generatie mislukt.")

        logger.info(f"✅ Strategie gegenereerd voor setup: {setup.get('name', 'onbekend')}")
        return strategy

    except Exception as e:
        logger.error(f"❌ AI-strategiegeneratie mislukt: {e}")
        raise HTTPException(status_code=500, detail=f"Interne fout: {e}")
