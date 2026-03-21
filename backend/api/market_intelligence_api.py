from backend.engine.market_intelligence_engine import get_market_intelligence
from backend.engine.market_pressure_engine import get_market_pressure
from backend.engine.transition_detector import compute_transition_detector


@router.get("/market/intelligence")
async def get_market_intelligence_api(
    current_user: dict = Depends(get_current_user)
):
    user_id = current_user["id"]
    conn = get_db_connection()

    try:
        scores = _get_daily_scores(conn, user_id)

        # ✅ engines direct gebruiken
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

        # -------------------------------------------------
        # Response (zelfde structuur houden)
        # -------------------------------------------------

        return {
            "cycle": intelligence.get("cycle"),
            "temperature": intelligence.get("temperature"),

            "trend": intelligence.get("trend", {}),

            "metrics": {
                "market_pressure": round(market_pressure * 100, 1),
                "transition_risk": round(transition_risk * 100, 1),
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
