# backend/agents_api.py

import logging
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query

from backend.utils.db import get_db_connection

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/agents",
    tags=["AI Agents"],
)

# ---------------------------------------------------------
# 🧠 1) CATEGORY INSIGHT
# GET /api/agents/insights?category=macro
# ---------------------------------------------------------
@router.get("/insights")
def get_agent_insight(
    category: str = Query(..., description="Categorie, bv. macro|market|technical|setup")
):
    """
    Haal de meest recente AI-inzichttekst op uit ai_category_insights
    voor een bepaalde categorie.
    """

    logger.info(f"[agents_api] Fetching insight for category={category}")

    with get_db_connection() as conn:
      with conn.cursor() as cur:
        # Pas kolomnamen aan als jouw schema anders heet
        cur.execute(
            """
            SELECT id, category, insight, created_at
            FROM ai_category_insights
            WHERE category = %s
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (category,),
        )
        row = cur.fetchone()

    if not row:
        # Frontend lib/agent.js gaat hier gewoon null van maken
        logger.info(f"[agents_api] No insight found for category={category}")
        return {"category": category, "insight": None}

    insight = {
        "id": row[0],
        "category": row[1],
        "text": row[2],
        "created_at": row[3].isoformat() if row[3] else None,
    }

    # ⚠️ Frontend lib/agent.js doet: data?.insight || null
    # Daarom geven we hier "insight" terug.
    return {
        "category": category,
        "insight": insight["text"],
        "meta": insight,
    }


# ---------------------------------------------------------
# 🧠 2) REFLECTIONS PER CATEGORY
# GET /api/agents/reflections?category=macro
# ---------------------------------------------------------
@router.get("/reflections")
def get_agent_reflections(
    category: str = Query(..., description="Categorie, bv. macro|market|technical|setup"),
    limit: int = Query(5, ge=1, le=50, description="Max aantal regels")
):
    """
    Haal laatste AI-reflecties op uit ai_reflections voor een categorie.
    Worden gebruikt als optionele extra context in AgentInsightPanel.
    """

    logger.info(f"[agents_api] Fetching reflections for category={category}, limit={limit}")

    with get_db_connection() as conn:
      with conn.cursor() as cur:
        # Pas kolomnamen aan aan je echte schema
        cur.execute(
            """
            SELECT id, category, title, reflection, weight, created_at
            FROM ai_reflections
            WHERE category = %s
            ORDER BY created_at DESC
            LIMIT %s
            """,
            (category, limit),
        )
        rows = cur.fetchall()

    reflections: List[dict] = []
    for row in rows:
        reflections.append(
            {
                "id": row[0],
                "category": row[1],
                "title": row[2],
                "text": row[3],
                "weight": row[4],
                "created_at": row[5].isoformat() if row[5] else None,
            }
        )

    # lib/agent.js doet: data?.reflections || []
    return {
        "category": category,
        "count": len(reflections),
        "reflections": reflections,
    }
