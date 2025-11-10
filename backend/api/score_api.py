import logging
from fastapi import APIRouter, HTTPException
import psycopg2.extras
from datetime import date

from backend.utils.db import get_db_connection
from backend.utils.scoring_utils import (
    generate_scores_db,
    get_scores_for_symbol,
)

router = APIRouter()
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


# ---------------------------
# Helpers
# ---------------------------
def fetch_active_setups():
    """
    Haalt actieve setups op, inclusief hun laatste score van vandaag uit daily_setup_scores.
    """
    conn = get_db_connection()
    if not conn:
        logger.warning("⚠️ fetch_active_setups: geen DB-verbinding.")
        return []

    today = date.today()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT DISTINCT ON (s.name)
                       s.id,
                       s.name,
                       COALESCE(s.symbol, 'BTC') AS symbol,
                       COALESCE(s.timeframe, '1D') AS timeframe,
                       COALESCE(s.explanation, '') AS explanation,
                       s.created_at AS timestamp,
                       COALESCE(ds.score, 0) AS score,          -- ✅ score uit daily_setup_scores
                       COALESCE(ds.active, false) AS is_active,  -- ✅ juiste kolomnaam uit DB
                       ds.report_date,
                       ds.created_at AS updated_at
                FROM setups s
                LEFT JOIN daily_setup_scores ds
                       ON s.id = ds.setup_id AND ds.report_date = %s
                WHERE s.active = TRUE
                ORDER BY s.name, s.created_at DESC
                LIMIT 50
            """, (today,))
            rows = cur.fetchall()
            return [dict(r) for r in rows]

    except Exception as e:
        logger.warning(f"⚠️ fetch_active_setups error: {e}")
        return []
    finally:
        conn.close()


# =========================================================
# ✅ Macro score (DB-regels)
# =========================================================
@router.get("/score/macro")
async def get_macro_score():
    """Haalt macro-score direct uit database via scoreregels."""
    try:
        result = generate_scores_db("macro")
        return result
    except Exception as e:
        logger.error(f"❌ /score/macro: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Fout bij ophalen macro-score")


# =========================================================
# ✅ Technische score (DB-regels)
# =========================================================
@router.get("/score/technical")
async def get_technical_score():
    """Haalt technische score direct uit database via scoreregels."""
    try:
        result = generate_scores_db("technical")
        return result
    except Exception as e:
        logger.error(f"❌ /score/technical: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Fout bij ophalen technische score")


# =========================================================
# ✅ Markt score (DB-regels; gebruikt technical rules)
# =========================================================
@router.get("/score/market")
async def get_market_score():
    """Haalt markt-score direct uit database via scoreregels."""
    try:
        result = generate_scores_db("market")
        return result
    except Exception as e:
        logger.error(f"❌ /score/market: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Fout bij ophalen market score")


# =========================================================
# ✅ Dagelijkse gecombineerde score (dashboard)
# =========================================================
@router.get("/scores/daily")
async def get_daily_scores():
    """
    ➤ Haalt actuele gecombineerde macro-, technische-, setup- en marktscores op uit de database.
    Inclusief actieve setups en hun actuele score (uit daily_setup_scores).
    """
    try:
        # Combineert macro/technical/market en levert top_contributors + interpretations
        scores = get_scores_for_symbol(include_metadata=True) or {}

        # Actieve setups inclusief score
        active_setups = fetch_active_setups()
        logger.info(f"📦 Actieve setups: {len(active_setups)}")

        # Structuur voor frontend/dashboard
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
                "top_contributors": [s["name"] for s in active_setups if s.get("is_active")] if active_setups else [],
                "active_setups": active_setups,
            },
            "market": {
                "score": float(scores.get("market_score", 0)),
                "interpretation": scores.get("market_interpretation", "Geen uitleg beschikbaar"),
                "top_contributors": scores.get("market_top_contributors", []),
            },
        }

        logger.info("✅ /scores/daily OK")
        return response

    except Exception as e:
        logger.error(f"❌ /scores/daily: {e}", exc_info=True)
        # Fallback met shape die je frontend verwacht
        return {
            "macro": {"score": 0, "interpretation": "Geen data", "top_contributors": []},
            "technical": {"score": 0, "interpretation": "Geen data", "top_contributors": []},
            "setup": {"score": 0, "interpretation": "Geen data", "top_contributors": [], "active_setups": []},
            "market": {"score": 0, "interpretation": "Geen data", "top_contributors": []},
        }
