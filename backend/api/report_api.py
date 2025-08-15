import logging
import io
from datetime import datetime
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse

from backend.utils.db import get_db_connection
from backend.utils.pdf_generator import generate_pdf_report
from backend.celery_task.daily_report_task import generate_daily_report
from backend.celery_task.weekly_report_task import generate_weekly_report
from backend.celery_task.monthly_report_task import generate_monthly_report
from backend.celery_task.quarterly_report_task import generate_quarterly_report

router = APIRouter()
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ✅ Dagelijks rapport ophalen
@router.get("/report/daily")
async def get_daily_report():
    logger.info("[get_daily_report] Dagelijks rapport ophalen")
    conn = get_db_connection()
    if not conn:
        logger.error("[get_daily_report] Geen databaseverbinding")
        raise HTTPException(status_code=500, detail="Databaseverbinding mislukt")
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM daily_reports ORDER BY report_date DESC LIMIT 1")
            row = cur.fetchone()
            if not row:
                logger.warning("[get_daily_report] Geen rapport gevonden")
                return {}
            return dict(zip([desc[0] for desc in cur.description], row))
    except Exception as e:
        logger.error(f"❌ [get_daily_report] Fout bij ophalen: {e}")
        raise HTTPException(status_code=500, detail="Fout bij ophalen dagelijks rapport")
    finally:
        conn.close()

# ✅ Wekelijks rapport ophalen
@router.get("/report/weekly")
async def get_weekly_report():
    logger.info("[get_weekly_report] Wekelijks rapport ophalen")
    conn = get_db_connection()
    if not conn:
        logger.error("[get_weekly_report] Geen databaseverbinding")
        raise HTTPException(status_code=500, detail="Databaseverbinding mislukt")
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM weekly_reports ORDER BY report_date DESC LIMIT 1")
            row = cur.fetchone()
            if not row:
                logger.warning("[get_weekly_report] Geen rapport gevonden")
                return {}
            return dict(zip([desc[0] for desc in cur.description], row))
    except Exception as e:
        logger.error(f"❌ [get_weekly_report] Fout bij ophalen: {e}")
        raise HTTPException(status_code=500, detail="Fout bij ophalen wekelijks rapport")
    finally:
        conn.close()

# ✅ Maandelijks rapport ophalen
@router.get("/report/monthly")
async def get_monthly_report():
    logger.info("[get_monthly_report] Maandelijks rapport ophalen")
    conn = get_db_connection()
    if not conn:
        logger.error("[get_monthly_report] Geen databaseverbinding")
        raise HTTPException(status_code=500, detail="Databaseverbinding mislukt")
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM monthly_reports ORDER BY report_date DESC LIMIT 1")
            row = cur.fetchone()
            if not row:
                logger.warning("[get_monthly_report] Geen rapport gevonden")
                return {}
            return dict(zip([desc[0] for desc in cur.description], row))
    except Exception as e:
        logger.error(f"❌ [get_monthly_report] Fout bij ophalen: {e}")
        raise HTTPException(status_code=500, detail="Fout bij ophalen maandelijks rapport")
    finally:
        conn.close()

# ✅ Kwartaalrapport ophalen
@router.get("/report/quarterly")
async def get_quarterly_report():
    logger.info("[get_quarterly_report] Kwartaalrapport ophalen")
    conn = get_db_connection()
    if not conn:
        logger.error("[get_quarterly_report] Geen databaseverbinding")
        raise HTTPException(status_code=500, detail="Databaseverbinding mislukt")
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM quarterly_reports ORDER BY report_date DESC LIMIT 1")
            row = cur.fetchone()
            if not row:
                logger.warning("[get_quarterly_report] Geen rapport gevonden")
                return {}
            return dict(zip([desc[0] for desc in cur.description], row))
    except Exception as e:
        logger.error(f"❌ [get_quarterly_report] Fout bij ophalen: {e}")
        raise HTTPException(status_code=500, detail="Fout bij ophalen kwartaalrapport")
    finally:
        conn.close()
