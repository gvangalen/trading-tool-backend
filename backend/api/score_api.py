import logging
import json
from fastapi.responses import JSONResponse
from fastapi import APIRouter, HTTPException
from backend.utils.scoring_utils import load_config, generate_scores
from backend.utils.scoring_utils import get_scores_for_symbol

router = APIRouter()
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# ✅ Tijdelijke dummydata (wordt later vervangen door live data via DB/API)
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
    """
    Retourneert de macro-economische score en interpretatie op basis van dummydata.
    Configuratie wordt geladen uit `macro_indicators_config.json`.
    """
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
    """
    Retourneert de technische analyse score en interpretatie op basis van dummydata.
    Configuratie wordt geladen uit `technical_indicators_config.json`.
    """
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
    """
    Retourneert de marktscore en interpretatie op basis van dummydata.
    Configuratie wordt geladen uit `market_data_config.json`.
    """
    try:
        config = load_config("config/market_data_config.json")
        result = generate_scores(dummy_data, config)
        return result
    except Exception as e:
        logger.error(f"❌ Market-score fout: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ✅ Dagelijkse gecombineerde score ophalen
@router.get("/scores/daily")
async def get_daily_scores():
    """
    ➤ Haalt de meest actuele scores + interpretaties + top contributors op uit de database.
    Wordt gebruikt in dashboard en rapport. Gebaseerd op de 'daily_scores' tabel.
    """
    try:
        scores = get_scores_for_symbol()  # Haalt laatste entry op (meestal BTC)
        if not scores:
            logger.warning("⚠️ Geen scores gevonden in database")
            return JSONResponse(
                status_code=404,
                content={"detail": "Geen scores gevonden in database"}
            )

        # ✅ Maak een nette response met expliciete velden
        response = {
            "macro": {
                "score": scores.get("macro_score"),
                "interpretation": scores.get("macro_interpretation"),
                "top_contributors": json.loads(scores.get("macro_top_contributors", "[]"))
            },
            "technical": {
                "score": scores.get("technical_score"),
                "interpretation": scores.get("technical_interpretation"),
                "top_contributors": json.loads(scores.get("technical_top_contributors", "[]"))
            },
            "setup": {
                "score": scores.get("setup_score"),
                "interpretation": scores.get("setup_interpretation"),
                "top_contributors": json.loads(scores.get("setup_top_contributors", "[]"))
            },
            "market": {
                "score": scores.get("market_score"),
                "interpretation": None,  # Marktdata heeft (voor nu) geen uitleg
                "top_contributors": []
            }
        }

        return response

    except Exception as e:
        logger.error(f"❌ Fout in /api/scores/daily: {e}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"detail": "Interne fout bij ophalen scores"}
        )
