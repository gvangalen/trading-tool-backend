# backend/api/report_api.py

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

# ==========================
# HELPERS
# ==========================

def get_table_name(report_type: str):
    mapping = {
        "daily": "daily_reports",
        "weekly": "weekly_reports",
        "monthly": "monthly_reports",
        "quarterly": "quarterly_reports"
    }
    if report_type not in mapping:
        raise HTTPException(status_code=400, detail=f"Ongeldig report type: {report_type}")
    return mapping[report_type]

def get_generate_task(report_type: str):
    mapping = {
        "daily": generate_daily_report,
        "weekly": generate_weekly_report,
        "monthly": generate_monthly_report,
        "quarterly": generate_quarterly_report
    }
    if report_type not in mapping:
        raise HTTPException(status_code=400, detail=f"Ongeldig report type: {report_type}")
    return mapping[report_type]

def fetch_report(table: str, date: str = None):
    conn = get_db_connection()
    if not conn:
        logger.error(f"[fetch_report] Geen databaseverbinding voor {table}")
        raise HTTPException(status_code=500, detail="Geen databaseverbinding")
    try:
        with conn.cursor() as cur:
            if date:
                cur.execute(f"SELECT * FROM {table} WHERE report_date = %s", (date,))
            else:
                cur.execute(f"SELECT * FROM {table} ORDER BY report_date DESC LIMIT 1")
            row = cur.fetchone()
            if not row:
                logger.warning(f"[fetch_report] Geen rapport gevonden in {table} voor {date or 'latest'}")
                return {}
            return dict(zip([desc[0] for desc in cur.description], row))
    except Exception as e:
        logger.exception(f"[fetch_report] Fout bij ophalen uit {table}: {e}")
        raise HTTPException(status_code=500, detail="Fout bij ophalen rapport")
    finally:
        conn.close()

def fetch_report_history(table: str):
    conn = get_db_connection()
    if not conn:
        logger.error(f"[fetch_report_history] Geen databaseverbinding voor {table}")
        raise HTTPException(status_code=500, detail="Geen databaseverbinding")
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT report_date FROM {table} ORDER BY report_date DESC LIMIT 30")
            rows = cur.fetchall()
            return [r[0] for r in rows]
    except Exception as e:
        logger.exception(f"[fetch_report_history] Fout bij ophalen history uit {table}: {e}")
        raise HTTPException(status_code=500, detail="Fout bij ophalen rapportgeschiedenis")
    finally:
        conn.close()

# ==========================
# 1. RAPPORT PER TYPE
# ==========================

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

# ==========================
# 2. HISTORY, GENERATE, PDF
# ==========================

@router.get("/report/{report_type}/history")
async def get_report_history(report_type: str):
    table = get_table_name(report_type)
    logger.info(f"[get_report_history] Geschiedenis ophalen uit {table}")
    return fetch_report_history(table)

@router.post("/report/{report_type}/generate")
async def generate_report(report_type: str):
    task = get_generate_task(report_type)
    try:
        celery_task = task.delay()
        logger.info(f"[generate_report] Celery taak gestart: {celery_task.id}")
        return {"message": "Taak gestart", "task_id": celery_task.id}
    except Exception as e:
        logger.error(f"[generate_report] Fout bij starten taak: {e}")
        raise HTTPException(status_code=500, detail="Fout bij starten Celery taak")

@router.get("/report/{report_type}/export/pdf")
async def export_report_pdf(report_type: str, date: str = Query(...)):
    table = get_table_name(report_type)
    logger.info(f"[export_report_pdf] PDF genereren voor {report_type} op {date}")
    try:
        report = fetch_report(table, date)
        if not report:
            raise HTTPException(status_code=404, detail="Rapport niet gevonden")
        pdf_bytes = generate_pdf_report(report)
        return StreamingResponse(io.BytesIO(pdf_bytes), media_type="application/pdf")
    except Exception as e:
        logger.error(f"[export_report_pdf] Fout bij exporteren: {e}")
        raise HTTPException(status_code=500, detail="Fout bij PDF-export")
