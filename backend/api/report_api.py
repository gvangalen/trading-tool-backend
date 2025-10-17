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
logger = logging.getLogger("backend.api.report_api")

# =========================================================
# 🧩 HELPER: PDF export
# =========================================================
def export_pdf_generic(report_type: str, report: dict, date: str):
    pdf_dir = f"backend/static/reports/{report_type}"
    os.makedirs(pdf_dir, exist_ok=True)
    pdf_path = os.path.join(pdf_dir, f"{report_type}_report_{date}.pdf")

    pdf_buffer = generate_pdf_report(report)
    with open(pdf_path, "wb") as f:
        f.write(pdf_buffer.getbuffer())

    logger.info(f"[export_pdf] ✅ PDF opgeslagen: {pdf_path}")
    return FileResponse(pdf_path, media_type="application/pdf", filename=os.path.basename(pdf_path))


# =========================================================
# 📅 DAGRAPPORT
# =========================================================
@router.get("/report/daily/latest")
async def get_daily_latest():
    logger.info("[get_daily_latest] 🚀 Ophalen laatste dagrapport")
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            query = """SELECT * FROM daily_reports WHERE symbol='BTC' ORDER BY report_date DESC LIMIT 1"""
            logger.info(f"[get_daily_latest] ▶️ Query:\n{query}")
            cur.execute(query)
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Geen dagelijks rapport gevonden")
            cols = [d[0] for d in cur.description]
            data = dict(zip(cols, row))
            logger.info(f"[get_daily_latest] ✅ Rapport: {data.get('report_date')}")
            return data
    finally:
        conn.close()


@router.get("/report/daily/by-date")
async def get_daily_by_date(date: str = Query(...)):
    logger.info(f"[get_daily_by_date] 🚀 Rapport voor {date}")
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            query = """SELECT * FROM daily_reports WHERE report_date::date=%s::date AND symbol='BTC' LIMIT 1"""
            logger.info(f"[get_daily_by_date] ▶️ Query:\n{query}")
            cur.execute(query, (date,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail=f"Geen rapport voor {date}")
            cols = [d[0] for d in cur.description]
            data = dict(zip(cols, row))
            logger.info(f"[get_daily_by_date] ✅ Rapport: {data.get('report_date')}")
            return data
    finally:
        conn.close()


@router.get("/report/daily/history")
async def get_daily_history():
    logger.info("[get_daily_history] 🚀 Ophalen geschiedenis")
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            query = """SELECT report_date FROM daily_reports WHERE symbol='BTC' ORDER BY report_date DESC LIMIT 30"""
            logger.info(f"[get_daily_history] ▶️ Query:\n{query}")
            cur.execute(query)
            result = [r[0] for r in cur.fetchall()]
            logger.info(f"[get_daily_history] ✅ {len(result)} datums gevonden")
            return result
    finally:
        conn.close()


@router.post("/report/daily/generate")
async def generate_daily():
    logger.info("[generate_daily] 🚀 Celery taak starten")
    task = generate_daily_report.delay()
    return {"message": "Dagrapport taak gestart", "task_id": task.id}


@router.get("/report/daily/export/pdf")
async def export_daily_pdf(date: str = Query(...)):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM daily_reports WHERE report_date::date=%s::date AND symbol='BTC'", (date,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Geen dagelijks rapport gevonden")
            cols = [d[0] for d in cur.description]
            report = dict(zip(cols, row))
            return export_pdf_generic("daily", report, date)
    finally:
        conn.close()


# =========================================================
# 📈 WEEKRAPPORT
# =========================================================
@router.get("/report/weekly/latest")
async def get_weekly_latest():
    logger.info("[get_weekly_latest] 🚀 Ophalen laatste weekrapport")
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            query = """SELECT * FROM weekly_reports WHERE symbol='BTC' ORDER BY report_date DESC LIMIT 1"""
            cur.execute(query)
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Geen weekrapport gevonden")
            cols = [d[0] for d in cur.description]
            return dict(zip(cols, row))
    finally:
        conn.close()


@router.get("/report/weekly/by-date")
async def get_weekly_by_date(date: str = Query(...)):
    logger.info(f"[get_weekly_by_date] 🚀 Rapport voor {date}")
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM weekly_reports WHERE report_date::date=%s::date AND symbol='BTC'", (date,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail=f"Geen weekrapport voor {date}")
            cols = [d[0] for d in cur.description]
            return dict(zip(cols, row))
    finally:
        conn.close()


@router.get("/report/weekly/history")
async def get_weekly_history():
    logger.info("[get_weekly_history] 🚀 Ophalen geschiedenis weekrapporten")
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT report_date FROM weekly_reports WHERE symbol='BTC' ORDER BY report_date DESC LIMIT 30")
            return [r[0] for r in cur.fetchall()]
    finally:
        conn.close()


@router.post("/report/weekly/generate")
async def generate_weekly():
    task = generate_weekly_report.delay()
    return {"message": "Weekrapport taak gestart", "task_id": task.id}


@router.get("/report/weekly/export/pdf")
async def export_weekly_pdf(date: str = Query(...)):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM weekly_reports WHERE report_date::date=%s::date AND symbol='BTC'", (date,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Geen weekrapport gevonden")
            cols = [d[0] for d in cur.description]
            report = dict(zip(cols, row))
            return export_pdf_generic("weekly", report, date)
    finally:
        conn.close()


# =========================================================
# 📊 MAANDRAPPORT
# =========================================================
@router.get("/report/monthly/latest")
async def get_monthly_latest():
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM monthly_reports WHERE symbol='BTC' ORDER BY report_date DESC LIMIT 1")
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Geen maandrapport gevonden")
            cols = [d[0] for d in cur.description]
            return dict(zip(cols, row))
    finally:
        conn.close()


@router.get("/report/monthly/by-date")
async def get_monthly_by_date(date: str = Query(...)):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM monthly_reports WHERE report_date::date=%s::date AND symbol='BTC'", (date,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail=f"Geen maandrapport voor {date}")
            cols = [d[0] for d in cur.description]
            return dict(zip(cols, row))
    finally:
        conn.close()


@router.get("/report/monthly/history")
async def get_monthly_history():
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT report_date FROM monthly_reports WHERE symbol='BTC' ORDER BY report_date DESC LIMIT 30")
            return [r[0] for r in cur.fetchall()]
    finally:
        conn.close()


@router.post("/report/monthly/generate")
async def generate_monthly():
    task = generate_monthly_report.delay()
    return {"message": "Maandrapport taak gestart", "task_id": task.id}


@router.get("/report/monthly/export/pdf")
async def export_monthly_pdf(date: str = Query(...)):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM monthly_reports WHERE report_date::date=%s::date AND symbol='BTC'", (date,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Geen maandrapport gevonden")
            cols = [d[0] for d in cur.description]
            report = dict(zip(cols, row))
            return export_pdf_generic("monthly", report, date)
    finally:
        conn.close()


# =========================================================
# 📉 KWARTAALRAPPORT
# =========================================================
@router.get("/report/quarterly/latest")
async def get_quarterly_latest():
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM quarterly_reports WHERE symbol='BTC' ORDER BY report_date DESC LIMIT 1")
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Geen kwartaalrapport gevonden")
            cols = [d[0] for d in cur.description]
            return dict(zip(cols, row))
    finally:
        conn.close()


@router.get("/report/quarterly/by-date")
async def get_quarterly_by_date(date: str = Query(...)):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM quarterly_reports WHERE report_date::date=%s::date AND symbol='BTC'", (date,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail=f"Geen kwartaalrapport voor {date}")
            cols = [d[0] for d in cur.description]
            return dict(zip(cols, row))
    finally:
        conn.close()


@router.get("/report/quarterly/history")
async def get_quarterly_history():
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT report_date FROM quarterly_reports WHERE symbol='BTC' ORDER BY report_date DESC LIMIT 30")
            return [r[0] for r in cur.fetchall()]
    finally:
        conn.close()


@router.post("/report/quarterly/generate")
async def generate_quarterly():
    task = generate_quarterly_report.delay()
    return {"message": "Kwartaalrapport taak gestart", "task_id": task.id}


@router.get("/report/quarterly/export/pdf")
async def export_quarterly_pdf(date: str = Query(...)):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM quarterly_reports WHERE report_date::date=%s::date AND symbol='BTC'", (date,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Geen kwartaalrapport gevonden")
            cols = [d[0] for d in cur.description]
            report = dict(zip(cols, row))
            return export_pdf_generic("quarterly", report, date)
    finally:
        conn.close()
