from fastapi import APIRouter, Depends
from datetime import date
import logging

from backend.utils.auth_utils import get_current_user
from backend.utils.db import get_db_connection

from backend.engine.market_intelligence_engine import get_market_intelligence
from backend.engine.market_pressure_engine import get_market_pressure
from backend.engine.transition_detector import compute_transition_detector

logger = logging.getLogger(__name__)

router = APIRouter()


# 🔥 FIX 1: helper toevoegen (ontbrak)
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


@router.get("/market/intelligence")
async def get_market_intelligence_api(
    current_user: dict = Depends(get_current_user)
):
    user_id = current_user["id"]
    conn = get_db_connection()

    try:
        scores = _get_daily_scores(conn, user_id)

        intelligence = get_market_intelligence(
            user_id=user_id,
            scores=scores,
        )

        market_pressure = get_market_pressure(
            user_id=user_id,
            scores=scores,
        )

        transition = compute_transition_detector(user_id)

        transition_risk = float(
            (transition or {}).get("normalized_risk", 0.5)
        )

        return {
            "cycle": intelligence.get("cycle"),
            "temperature": intelligence.get("temperature"),

            "trend": intelligence.get("trend", {}),

            # 🔥 FIX 2: GEEN *100
            "metrics": {
                "market_pressure": round(market_pressure, 3),
                "transition_risk": round(transition_risk, 3),
                "setup_quality": 50,
                "volatility": 50,
                "trend_strength": 50,
            },

            "state": intelligence.get("state", {}),

            "generated_at": date.today().isoformat(),
        }

    except Exception:
        logger.exception("❌ market intelligence error")
        raise

    finally:
        if conn:
            conn.close()
