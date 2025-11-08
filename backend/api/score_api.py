import logging
from fastapi import APIRouter, HTTPException
from backend.utils.db import get_db_connection
from backend.utils.scoring_utils import (
    generate_scores_db,
    get_scores_for_symbol,
    get_active_setups_with_info,
)

router = APIRouter()
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


# =========================================================
# ✅ Macro score (uit DB-regels)
# =========================================================
@router.get("/score/macro")
async def get_macro_score():
    """Haalt macro-score direct uit database via scoreregels."""
    try:
        result = generate_scores_db("macro")
        return result
    except Exception as e:
        logger.error(f"❌ Fout in /score/macro: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Fout bij ophalen macro-score")


# =========================================================
# ✅ Technische score (uit DB-regels)
# =========================================================
@router.get("/score/technical")
async def get_technical_score():
    """Haalt technische score direct uit database via scoreregels."""
    try:
        result = generate_scores_db("technical")
        return result
    except Exception as e:
        logger.error(f"❌ Fout in /score/technical: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Fout bij ophalen technische score")


# =========================================================
# ✅ Markt score (uit DB-regels)
# =========================================================
@router.get("/score/market")
async def get_market_score():
    """Haalt markt-score direct uit database via scoreregels."""
    try:
        result = generate_scores_db("market")
        return result
    except Exception as e:
        logger.error(f"❌ Fout in /score/market: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Fout bij ophalen market score")


# =========================================================
# ✅ Dagelijkse gecombineerde score (voor dashboard)
# =========================================================
@router.get("/scores/daily")
async def get_daily_scores():
    """
    ➤ Haalt actuele gecombineerde macro-, technische-, setup- en marktscores op uit de database.
    Inclusief actieve setups voor het dashboard en dagrapport.
    """
    try:
        scores = get_scores_for_symbol(include_metadata=True)
        if not scores:
            logger.warning("⚠️ Geen scores gevonden in database – fallback naar nulwaarden.")
            scores = {
                "macro_score": 0,
                "technical_score": 0,
                "setup_score": 0,
                "market_score": 0,
                "macro_interpretation": "Geen data beschikbaar",
                "technical_interpretation": "Geen data beschikbaar",
                "setup_interpretation": "Geen data beschikbaar",
                "macro_top_contributors": [],
                "technical_top_contributors": [],
                "setup_top_contributors": [],
            }

        # 🔍 Actieve setups ophalen
        conn = get_db_connection()
        active_setups = get_active_setups_with_info(conn)
        conn.close()
        logger.info(f"📦 Actieve setups gevonden: {len(active_setups)}")

        # ✅ Structureren voor dashboard
        response = {
            "macro": {
                "score": float(scores.get("macro_score", 0)),
                "interpretation": scores.get("macro_interpretation", "Geen uitleg beschikbaar"),
                "top_contributors": scores.get("macro_top_contributors", []),
            },
            "technical": {
                "score": float(scores.get("technical_score", 0)),
                "interpretation": scores.get("technical_interpretation", "Geen uitleg beschikbaar"),
                "top_contributors": scores.get("technical_top_contributors", []),
            },
            "setup": {
                "score": float(scores.get("setup_score", 0)),
                "interpretation": scores.get("setup_interpretation", "Geen actieve setups"),
                "top_contributors": [s["name"] for s in active_setups] if active_setups else [],
                "active_setups": active_setups,
            },
            "market": {
                "score": float(scores.get("market_score", 0)),
                "interpretation": scores.get("market_interpretation", "Geen uitleg beschikbaar"),
                "top_contributors": scores.get("market_top_contributors", []),
            },
        }

        logger.info("✅ Dagelijkse scores succesvol opgehaald uit database")
        return response

    except Exception as e:
        logger.error(f"❌ Fout in /api/scores/daily: {e}", exc_info=True)
        return {
            "macro": {"score": 0, "interpretation": "Geen data"},
            "technical": {"score": 0, "interpretation": "Geen data"},
            "setup": {"score": 0, "interpretation": "Geen data", "active_setups": []},
            "market": {"score": 0, "interpretation": "Geen data"},
        }
