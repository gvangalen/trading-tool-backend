# backend/api/report_api.py

import logging
import os
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import FileResponse

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
# HELPER FUNCTIES
# ==========================

def fetch_report(table: str, date: str | None = None):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Geen databaseverbinding")
    try:
        with conn.cursor() as cur:
            if date:
                logger.info(f"[fetch_report] 📅 Ophalen {table}: {date}")
                cur.execute(f"SELECT * FROM {table} WHERE report_date::text = %s AND symbol=%s", (date, "BTC"))
            else:
                logger.info(f"[fetch_report] 📄 Ophalen laatste rapport uit {table}")
                cur.execute(f"SELECT * FROM {table} WHERE symbol=%s ORDER BY report_date DESC LIMIT 1", ("BTC",))
            row = cur.fetchone()
            if not row:
                logger.warning(f"[fetch_report] ⚠️ Geen rapport gevonden in {table}")
                return {}
            data = dict(zip([d[0] for d in cur.description], row))
            logger.info(f"[fetch_report] ✅ Rapport gevonden: {data.get('report_date')} ({data.get('symbol')})")
            return data
    except Exception as e:
        logger.exception(f"[fetch_report] ❌ Fout: {e}")
        raise HTTPException(status_code=500, detail="Databasefout")
    finally:
        conn.close()

def fetch_history(table: str):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Geen databaseverbinding")
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT report_date FROM {table} ORDER BY report_date DESC LIMIT 30")
            return [r[0] for r in cur.fetchall()]
    finally:
        conn.close()

def export_pdf(report_type: str, report: dict, date: str):
    pdf_dir = f"backend/static/reports/{report_type}"
    os.makedirs(pdf_dir, exist_ok=True)
    pdf_path = os.path.join(pdf_dir, f"{report_type}_report_{date}.pdf")

    pdf_buffer = generate_pdf_report(report)
    with open(pdf_path, "wb") as f:
        f.write(pdf_buffer.getbuffer())

    logger.info(f"[export_pdf] ✅ PDF opgeslagen: {pdf_path}")
    return FileResponse(pdf_path, media_type="application/pdf", filename=os.path.basename(pdf_path))


# ==========================
# DAGRAPPORT
# ==========================

@router.get("/report/daily/latest")
async def get_daily_latest():
    data = fetch_report("daily_reports")
    if not data:
        raise HTTPException(status_code=404, detail="Geen dagelijks rapport gevonden")
    return data

@router.get("/report/daily/by-date")
async def get_daily_by_date(date: str = Query(...)):
    data = fetch_report("daily_reports", date)
    if not data:
        raise HTTPException(status_code=404, detail="Geen dagelijks rapport op deze datum")
    return data

@router.get("/report/daily/history")
async def get_daily_history():
    return fetch_history("daily_reports")

@router.post("/report/daily/generate")
async def generate_daily():
    task = generate_daily_report.delay()
    return {"message": "Dagrapport taak gestart", "task_id": task.id}

@router.get("/report/daily/export/pdf")
async def export_daily_pdf(date: str = Query(...)):
    report = fetch_report("daily_reports", date)
    if not report:
        raise HTTPException(status_code=404, detail="Geen dagelijks rapport gevonden")
    return export_pdf("daily", report, date)


# ==========================
# WEEKRAPPORT
# ==========================

@router.get("/report/weekly/latest")
async def get_weekly_latest():
    data = fetch_report("weekly_reports")
    if not data:
        raise HTTPException(status_code=404, detail="Geen weekrapport gevonden")
    return data

@router.get("/report/weekly/by-date")
async def get_weekly_by_date(date: str = Query(...)):
    data = fetch_report("weekly_reports", date)
    if not data:
        raise HTTPException(status_code=404, detail="Geen weekrapport op deze datum")
    return data

@router.get("/report/weekly/history")
async def get_weekly_history():
    return fetch_history("weekly_reports")

@router.post("/report/weekly/generate")
async def generate_weekly():
    task = generate_weekly_report.delay()
    return {"message": "Weekrapport taak gestart", "task_id": task.id}

@router.get("/report/weekly/export/pdf")
async def export_weekly_pdf(date: str = Query(...)):
    report = fetch_report("weekly_reports", date)
    if not report:
        raise HTTPException(status_code=404, detail="Geen weekrapport gevonden")
    return export_pdf("weekly", report, date)


# ==========================
# MAANDRAPPORT
# ==========================

@router.get("/report/monthly/latest")
async def get_monthly_latest():
    data = fetch_report("monthly_reports")
    if not data:
        raise HTTPException(status_code=404, detail="Geen maandrapport gevonden")
    return data

@router.get("/report/monthly/by-date")
async def get_monthly_by_date(date: str = Query(...)):
    data = fetch_report("monthly_reports", date)
    if not data:
        raise HTTPException(status_code=404, detail="Geen maandrapport op deze datum")
    return data

@router.get("/report/monthly/history")
async def get_monthly_history():
    return fetch_history("monthly_reports")

@router.post("/report/monthly/generate")
async def generate_monthly():
    task = generate_monthly_report.delay()
    return {"message": "Maandrapport taak gestart", "task_id": task.id}

@router.get("/report/monthly/export/pdf")
async def export_monthly_pdf(date: str = Query(...)):
    report = fetch_report("monthly_reports", date)
    if not report:
        raise HTTPException(status_code=404, detail="Geen maandrapport gevonden")
    return export_pdf("monthly", report, date)


# ==========================
# KWARTAALRAPPORT
# ==========================

@router.get("/report/quarterly/latest")
async def get_quarterly_latest():
    data = fetch_report("quarterly_reports")
    if not data:
        raise HTTPException(status_code=404, detail="Geen kwartaalrapport gevonden")
    return data

@router.get("/report/quarterly/by-date")
async def get_quarterly_by_date(date: str = Query(...)):
    data = fetch_report("quarterly_reports", date)
    if not data:
        raise HTTPException(status_code=404, detail="Geen kwartaalrapport op deze datum")
    return data

@router.get("/report/quarterly/history")
async def get_quarterly_history():
    return fetch_history("quarterly_reports")

@router.post("/report/quarterly/generate")
async def generate_quarterly():
    task = generate_quarterly_report.delay()
    return {"message": "Kwartaalrapport taak gestart", "task_id": task.id}

@router.get("/report/quarterly/export/pdf")
async def export_quarterly_pdf(date: str = Query(...)):
    report = fetch_report("quarterly_reports", date)
    if not report:
        raise HTTPException(status_code=404, detail="Geen kwartaalrapport gevonden")
    return export_pdf("quarterly", report, date)
