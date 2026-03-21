from fastapi import APIRouter, Depends
from datetime import date
import logging

from backend.utils.auth_utils import get_current_user
from backend.utils.db import get_db_connection

from backend.engine.market_intelligence_engine import get_market_intelligence

logger = logging.getLogger(__name__)

router = APIRouter()


# =========================================================
# Helper: daily scores
# =========================================================
def _get_daily_scores(conn, user_id: int):
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
# API: Market Intelligence
# =========================================================
@router.get("/market/intelligence")
async def get_market_intelligence_api(
    current_user: dict = Depends(get_current_user)
):
    user_id = current_user["id"]
    conn = get_db_connection()

    try:
        scores = _get_daily_scores(conn, user_id)

        # 🔥 SINGLE SOURCE OF TRUTH (ENGINE)
        intelligence = get_market_intelligence(
            user_id=user_id,
            scores=scores,
        )

        # 🔥 GEEN extra berekeningen
        return intelligence

    except Exception:
        logger.exception("❌ market intelligence error")
        raise

    finally:
        if conn:
            conn.close()
