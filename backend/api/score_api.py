import logging
import json
from fastapi import APIRouter, HTTPException
from utils.scoring_utils import load_config, generate_scores

router = APIRouter()
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# ✅ Tijdelijke dummydata (tot live data via DB/API komt)
dummy_data = {
    "fear_greed_index": 72,
    "btc_dominance": 52.4,
    "dxy": 103.8,
    "rsi": 61,
    "volume": 7900000000,
    "ma_200": 69000,
    "price": 71000,
    "change_24h": 2.4
}

# ✅ Macro score ophalen
@router.get("/score/macro")
async def macro_score():
    try:
        config = load_config("config/macro_indicators_config.json")
        result = generate_scores(dummy_data, config)
        return result
    except Exception as e:
        logger.error(f"❌ Macro-score fout: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ✅ Technische score ophalen
@router.get("/score/technical")
async def technical_score():
    try:
        config = load_config("config/technical_indicators_config.json")
        result = generate_scores(dummy_data, config)
        return result
    except Exception as e:
        logger.error(f"❌ Technische score fout: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ✅ Markt score ophalen
@router.get("/score/market")
async def market_score():
    try:
        config = load_config("config/market_data_config.json")
        result = generate_scores(dummy_data, config)
        return result
    except Exception as e:
        logger.error(f"❌ Market-score fout: {e}")
        raise HTTPException(status_code=500, detail=str(e))
