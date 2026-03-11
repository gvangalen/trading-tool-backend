import logging
from datetime import date
from fastapi import APIRouter, Depends

from backend.utils.auth_utils import get_current_user
from backend.utils.db import get_db_connection
from backend.engine.bot_brain import run_bot_brain

logger = logging.getLogger(__name__)

# =========================================================
# Router
# =========================================================
router = APIRouter()


# =========================================================
# Helpers
# =========================================================

def _get_daily_scores(conn, user_id: int):
    """
    Haalt de gecombineerde scores op uit daily_scores.
    """

    with conn.cursor() as cur:

        cur.execute(
            """
            SELECT macro_score, technical_score, market_score, setup_score
            FROM daily_scores
            WHERE user_id=%s
            ORDER BY report_date DESC
            LIMIT 1
            """,
            (user_id,),
        )

        row = cur.fetchone()

    if not row:
        return {
            "macro": 10,
            "technical": 10,
            "market": 10,
            "setup": 10,
        }

    macro, technical, market, setup = row

    return {
        "macro": float(macro or 10),
        "technical": float(technical or 10),
        "market": float(market or 10),
        "setup": float(setup or 10),
    }


# =========================================================
# MARKET INTELLIGENCE
# =========================================================

@router.get("/market/intelligence")
async def get_market_intelligence(
    current_user: dict = Depends(get_current_user)
):

    user_id = current_user["id"]

    conn = get_db_connection()

    try:

        # -------------------------------------------------
        # Scores ophalen
        # -------------------------------------------------

        scores = _get_daily_scores(conn, user_id)

        # dummy setup voor brain (alleen nodig voor sizing logic)
        setup = {}

        # -------------------------------------------------
        # Bot brain uitvoeren
        # -------------------------------------------------

        brain = run_bot_brain(
            user_id=user_id,
            setup=setup,
            scores=scores,
        )

        # -------------------------------------------------
        # Response naar frontend
        # -------------------------------------------------

        return {

            "cycle": brain.get("cycle"),
            "temperature": brain.get("temperature"),

            "trend": {
                "short": brain.get("short_trend"),
                "mid": brain.get("mid_trend"),
                "long": brain.get("long_trend"),
            },

            # 👇 metrics direct uit bot brain
            "metrics": brain.get("metrics", {}),

            # extra informatie voor andere panels
            "state": {
                "risk_environment": brain.get("risk_environment"),
                "risk_state": brain.get("risk_state"),
                "structure_bias": brain.get("structure_bias"),
                "volatility_state": brain.get("volatility_state"),
            },

            "generated_at": date.today().isoformat(),
        }

    except Exception:

        logger.exception("❌ market intelligence error")
        raise

    finally:

        if conn:
            conn.close()
