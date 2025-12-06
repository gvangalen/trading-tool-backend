import logging
from datetime import date

from fastapi import APIRouter, HTTPException, Depends
import psycopg2.extras

from backend.utils.db import get_db_connection
from backend.utils.scoring_utils import (
    generate_scores_db,
    get_scores_for_symbol,
)
from backend.utils.auth_utils import get_current_user   # ✅ juiste huidige import

router = APIRouter()
logger = logging.getLogger(__name__)


# =========================================================
# Helpers – nu user-specifiek
# =========================================================
def fetch_active_setups(user_id: int):
    """
    Haalt alle setups op voor deze user + score uit daily_setup_scores van vandaag.
    """
    conn = get_db_connection()
    if not conn:
        logger.warning("⚠️ fetch_active_setups: geen DB-verbinding.")
        return []

    today = date.today()

    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT DISTINCT ON (s.id)
                       s.id,
                       s.name,
                       COALESCE(s.symbol, 'BTC') AS symbol,
                       COALESCE(s.timeframe, '1D') AS timeframe,
                       COALESCE(s.explanation, '') AS explanation,
                       s.created_at AS timestamp,
                       COALESCE(ds.score, 0) AS score,
                       COALESCE(ds.active, false) AS is_active,
                       COALESCE(ds.breakdown, '{}'::jsonb) AS breakdown
                FROM setups s
                LEFT JOIN daily_setup_scores ds
                    ON s.id = ds.setup_id 
                    AND ds.report_date = %s
                    AND ds.user_id = %s
                WHERE s.user_id = %s
                ORDER BY s.id, ds.report_date DESC
                LIMIT 100
            """, (today, user_id, user_id))
            return [dict(r) for r in cur.fetchall()]

    except Exception as e:
        logger.warning(f"⚠️ fetch_active_setups error: {e}")
        return []

    finally:
        conn.close()


# =========================================================
# Macro Score
# =========================================================
@router.get("/score/macro")
async def get_macro_score(current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]

    try:
        return generate_scores_db("macro", user_id=user_id)
    except Exception as e:
        logger.error(f"❌ /score/macro: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Fout bij ophalen macro-score")


# =========================================================
# Technical Score
# =========================================================
@router.get("/score/technical")
async def get_technical_score(current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]

    try:
        return generate_scores_db("technical", user_id=user_id)
    except Exception as e:
        logger.error(f"❌ /score/technical: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Fout bij ophalen technische score")


# =========================================================
# Market Score
# =========================================================
@router.get("/score/market")
async def get_market_score(current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]

    try:
        return generate_scores_db("market", user_id=user_id)
    except Exception as e:
        logger.error(f"❌ /score/market: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Fout bij ophalen market-score")


# =========================================================
# Daily Combined Score (Dashboard)
# =========================================================
@router.get("/scores/daily")
async def get_daily_scores(current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]

    try:
        # Score engine → per user
        try:
            scores = get_scores_for_symbol(user_id=user_id, include_metadata=True) or {}
        except TypeError:
            scores = get_scores_for_symbol(include_metadata=True)

        active_setups = fetch_active_setups(user_id)

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
            "market": {
                "score": float(scores.get("market_score", 0)),
                "interpretation": scores.get("market_interpretation", "Geen uitleg beschikbaar"),
                "top_contributors": scores.get("market_top_contributors", []),
            },
            "setup": {
                "score": float(scores.get("setup_score", 0)),
                "interpretation": "Actieve setups" if active_setups else "Geen actieve setups",
                "top_contributors": [s["name"] for s in active_setups if s.get("is_active")],
                "active_setups": active_setups,
            },
        }

        return response

    except Exception as e:
        logger.error(f"❌ /scores/daily: {e}", exc_info=True)
        return {
            "macro": {"score": 0, "interpretation": "Geen data", "top_contributors": []},
            "technical": {"score": 0, "interpretation": "Geen data", "top_contributors": []},
            "market": {"score": 0, "interpretation": "Geen data", "top_contributors": []},
            "setup": {"score": 0, "interpretation": "Geen data", "top_contributors": [], "active_setups": []},
        }


# =========================================================
# AI Master Score (user-specifiek)
# =========================================================
@router.get("/ai/master_score")
def get_ai_master_score(current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]

    conn = get_db_connection()
    if not conn:
        return {"error": "Geen DB-verbinding"}

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    master_score, 
                    master_trend, 
                    master_bias, 
                    master_risk,
                    alignment_score,
                    outlook,
                    weights,
                    data_warnings,
                    domains,
                    summary,
                    date
                FROM ai_master_score_view
                WHERE user_id = %s
                ORDER BY date DESC
                LIMIT 1;
            """, (user_id,))

            row = cur.fetchone()

            if not row:
                return {"message": "Nog geen AI-master-score beschikbaar"}

            columns = [desc[0] for desc in cur.description]
            return dict(zip(columns, row))

    except Exception as e:
        logger.error(f"❌ Fout bij ophalen AI Master Score: {e}")
        return {"error": str(e)}

    finally:
        conn.close()
