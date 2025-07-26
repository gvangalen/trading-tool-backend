# backend/api/daily_report_api.py

from fastapi import APIRouter, HTTPException
from backend.utils.db import get_db_connection
import logging

router = APIRouter()
logger = logging.getLogger(__name__)

@router.get("/daily_report/summary")
async def get_daily_report_summary():
    """
    ✅ Haalt de aanbevelingssamenvatting op uit het meest recente dagrapport.
    """
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT recommendations
                FROM daily_reports
                ORDER BY report_date DESC
                LIMIT 1
            """)
            row = cur.fetchone()
            if not row:
                return {"summary": "Geen dagrapport gevonden."}
            return {"summary": row[0]}
    except Exception as e:
        logger.error(f"❌ DR-SUMMARY: Fout bij ophalen dagrapport-samenvatting: {e}")
        raise HTTPException(status_code=500, detail="Kan samenvatting niet ophalen.")
