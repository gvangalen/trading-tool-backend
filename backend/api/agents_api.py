import logging
from datetime import datetime
from typing import List, Dict, Any

from fastapi import APIRouter, HTTPException, Query

from backend.utils.db import get_db_connection

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

router = APIRouter()
logger.info("🚀 agents_api.py geladen – v2 met ai_category_insights + ai_reflections")


# ==========================================
# 🔧 Helper: DB connectie
# ==========================================
def get_conn_cursor():
    conn = get_db_connection()
    if not conn:
        logger.error("❌ [agents] Geen databaseverbinding.")
        raise HTTPException(status_code=500, detail="Geen databaseverbinding.")
    return conn, conn.cursor()


# ==========================================
# 🧠 Categorie-insight ophalen
# ==========================================
@router.get("/agents/insights")
async def get_agent_insight(category: str = Query(..., description="Categorie: macro, market, technical, setup, ...")):
    """
    Haal de laatste AI-categorie-insight op uit `ai_category_insights`.

    Frontend verwacht:
    {
      "insight": {
        "category": "technical",
        "score": 73.5,
        "trend": "bullish",
        "bias": "risk-on",
        "risk": "gemiddeld",
        "summary": "Korte samenvatting...",
        "top_signals": [...],
        "date": "2025-12-01",
        "created_at": "2025-12-01T07:30:00"
      }
    }
    """
    logger.info(f"📡 [agents] Insight ophalen voor categorie={category}")

    conn, cur = get_conn_cursor()
    try:
        cur.execute(
            """
            SELECT category,
                   avg_score,
                   trend,
                   bias,
                   risk,
                   summary,
                   top_signals,
                   date,
                   created_at
            FROM ai_category_insights
            WHERE category = %s
            ORDER BY date DESC, created_at DESC
            LIMIT 1;
            """,
            (category,),
        )
        row = cur.fetchone()

        if not row:
            logger.warning(f"⚠️ [agents] Geen ai_category_insights gevonden voor category={category}")
            return {"insight": None}

        (
            cat,
            avg_score,
            trend,
            bias,
            risk,
            summary,
            top_signals,
            d,
            created_at,
        ) = row

        insight = {
            "category": cat,
            "score": float(avg_score) if avg_score is not None else None,
            "trend": trend,
            "bias": bias,
            "risk": risk,
            "summary": summary,
            "top_signals": top_signals,  # jsonb → dict/list in Python
            "date": d.isoformat() if d else None,
            "created_at": created_at.isoformat() if isinstance(created_at, datetime) else None,
        }

        return {"insight": insight}

    except Exception as e:
        logger.error(f"❌ [agents/insights] Databasefout: {e}")
        raise HTTPException(status_code=500, detail="Fout bij ophalen AI-insight.")
    finally:
        conn.close()


# ==========================================
# 🪞 Reflecties per categorie
# ==========================================
@router.get("/agents/reflections")
async def get_agent_reflections(
    category: str = Query(..., description="Categorie: macro, market, technical, setup, ..."),
    limit: int = Query(5, ge=1, le=50, description="Max aantal reflecties"),
):
    """
    Haal AI-reflecties op uit `ai_reflections`.

    Frontend verwacht:
    {
      "reflections": [
        {
          "indicator": "RSI",
          "raw_score": null,
          "ai_score": 72,
          "compliance": 80,
          "comment": "Korte observatie...",
          "recommendation": "Korte aanbeveling...",
          "date": "2025-12-01",
          "timestamp": "2025-12-01T07:31:00"
        },
        ...
      ]
    }
    """
    logger.info(f"📡 [agents] Reflections ophalen voor categorie={category} limit={limit}")

    conn, cur = get_conn_cursor()
    try:
        cur.execute(
            """
            SELECT indicator,
                   raw_score,
                   ai_score,
                   compliance,
                   comment,
                   recommendation,
                   date,
                   timestamp
            FROM ai_reflections
            WHERE category = %s
            ORDER BY date DESC, timestamp DESC
            LIMIT %s;
            """,
            (category, limit),
        )
        rows = cur.fetchall()

        reflections: List[Dict[str, Any]] = []
        for (
            indicator,
            raw_score,
            ai_score,
            compliance,
            comment,
            recommendation,
            d,
            ts,
        ) in rows:
            reflections.append({
                "indicator": indicator,
                "raw_score": float(raw_score) if raw_score is not None else None,
                "ai_score": float(ai_score) if ai_score is not None else None,
                "compliance": float(compliance) if compliance is not None else None,
                "comment": comment,
                "recommendation": recommendation,
                "date": d.isoformat() if d else None,
                "timestamp": ts.isoformat() if isinstance(ts, datetime) else None,
            })

        return {"reflections": reflections}

    except Exception as e:
        logger.error(f"❌ [agents/reflections] Fout: {e}")
        raise HTTPException(status_code=500, detail="Fout bij ophalen AI-reflecties.")
    finally:
        conn.close()
