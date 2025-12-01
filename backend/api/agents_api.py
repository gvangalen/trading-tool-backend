import os
import logging
from datetime import datetime
from fastapi import APIRouter, HTTPException, Query
from dotenv import load_dotenv

from backend.utils.db import get_db_connection

# =========================================
# 🔧 ENV + Logging
# =========================================
dotenv_path = os.path.join(os.path.dirname(__file__), "..", ".env")
load_dotenv(dotenv_path=dotenv_path)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

router = APIRouter()
logger.info("🚀 agents_api.py geladen – AI Agents backend actief.")


# =========================================
# 🔧 Helper: DB cursor
# =========================================
def get_db_cursor():
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="❌ [DB01] Geen databaseverbinding.")
    return conn, conn.cursor()


# =========================================
# 🧠 GET /api/agents/insights?category=macro
# =========================================
@router.get("/agents/insights")
async def get_agent_insight(category: str = Query(...)):
    logger.info(f"📡 [agents] Insight ophalen voor categorie={category}")

    conn, cur = get_db_cursor()

    try:
        cur.execute("""
            SELECT id, category, insight, created_at
            FROM ai_category_insights
            WHERE category = %s
            ORDER BY created_at DESC
            LIMIT 1;
        """, (category,))

        row = cur.fetchone()

        if not row:
            logger.warning(f"⚠️ Geen insight gevonden voor {category}")
            return {"category": category, "insight": None}

        insight_obj = {
            "id": row[0],
            "category": row[1],
            "text": row[2],
            "created_at": row[3].isoformat() if row[3] else None,
        }

        return {
            "category": category,
            "insight": insight_obj["text"],
            "meta": insight_obj,
        }

    except Exception as e:
        logger.error(f"❌ [agents/insights] Databasefout: {e}")
        raise HTTPException(status_code=500, detail="Fout bij ophalen AI insight.")
    finally:
        conn.close()


# =========================================
# 🧠 GET /api/agents/reflections?category=macro
# =========================================
@router.get("/agents/reflections")
async def get_agent_reflections(
    category: str = Query(...),
    limit: int = Query(5, ge=1, le=50),
):
    logger.info(f"📡 [agents] Reflections ophalen voor categorie={category} limit={limit}")

    conn, cur = get_db_cursor()

    try:
        cur.execute("""
            SELECT id, category, title, reflection, weight, created_at
            FROM ai_reflections
            WHERE category = %s
            ORDER BY created_at DESC
            LIMIT %s;
        """, (category, limit))

        rows = cur.fetchall()

        reflections = [
            {
                "id": r[0],
                "category": r[1],
                "title": r[2],
                "text": r[3],
                "weight": r[4],
                "created_at": r[5].isoformat() if r[5] else None
            }
            for r in rows
        ]

        return {
            "category": category,
            "count": len(reflections),
            "reflections": reflections,
        }

    except Exception as e:
        logger.error(f"❌ [agents/reflections] Fout: {e}")
        raise HTTPException(status_code=500, detail="Fout bij ophalen AI reflections.")
    finally:
        conn.close()
