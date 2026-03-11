import logging
from fastapi import APIRouter, Depends

from backend.utils.auth_utils import get_current_user
from backend.engine.bot_brain import run_bot_brain


# =========================================================
# Router
# =========================================================
router = APIRouter()
logger = logging.getLogger(__name__)


# =========================================================
# MARKET INTELLIGENCE
# =========================================================
@router.get("/market/intelligence")
async def get_market_intelligence(current_user: dict = Depends(get_current_user)):

    try:

        brain = run_bot_brain()

        return {
            "cycle": brain.get("cycle"),
            "temperature": brain.get("temperature"),

            "trend": {
                "short": brain.get("short_trend"),
                "mid": brain.get("mid_trend"),
                "long": brain.get("long_trend"),
            },

            "metrics": {
                "market_pressure": brain.get("market_pressure"),
                "transition_risk": brain.get("transition_risk"),
                "setup_quality": brain.get("setup_quality"),
                "volatility": brain.get("volatility"),
                "trend_strength": brain.get("trend_strength"),
                "position_size": brain.get("position_size"),
            }
        }

    except Exception:
        logger.exception("❌ market intelligence error")
        raise
