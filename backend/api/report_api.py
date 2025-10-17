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
logger = logging.getLogger("backend.api.report_api")


# ==========================
# 🧩 HELPER FUNCTIES
# ==========================

def fetch_report(table: str, date: str | None = None):
    """Haalt één rapport op uit de database."""
    conn = get_db_connection()
    if not conn:
        logger.error(f"[fetch_report] ❌ Geen databaseverbinding ({table})")
        raise HTTPException(status_code=500, detail="Geen databaseverbinding")

    try:
        with conn.cursor() as cur:
            if date:
                logger.info(f"[fetch_report] 📅 Ophalen {table}: report_date={date} (BTC)")
                cur.execute(f"SELECT * FROM {table} WHERE report_date = %s AND symbol=%s", (date, "BTC"))
            else:
                logger.info(f"[fetch_report] 📄 Ophalen laatste rapport uit {table} (BTC)")
                cur.execute(f"SELECT * FROM {table} WHERE symbol=%s ORDER BY report_date DESC LIMIT 1", ("BTC",))

            row = cur.fetchone()
            if not row:
                logger.warning(f"[fetch_report] ⚠️ Geen rapport gevonden in {table} ({date or 'latest'})")
                return {}

            cols = [desc[0] for desc in cur.description]
            data = dict(zip(cols, row))
            logger.info(f"[fetch_report] ✅ Rapport gevonden: {data.get('report_date')} ({data.get('symbol')})")
            return data

    except Exception as e:
        logger.exception(f"[fetch_report] ❌ Fout bij query ({table}): {e}")
        raise HTTPException(status_code=500, detail=f"Databasefout: {e}")

    finally:
        conn.close()


def fetch_history(table: str):
    """Haalt laatste 30 rapportdatums op."""
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Geen databaseverbinding")

    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT report_date FROM {table} ORDER BY report_date DESC LIMIT 30")
            result = [r[0] for r in cur.fetchall()]
            logger.info(f"[fetch_history] ✅ {len(result)} datums uit {table}: {result[:5]}...")
            return result
    except Exception as e:
        logger.exception(f"[fetch_history] ❌ Fout bij ophalen uit {table}: {e}")
        raise HTTPException(status_code=500, detail=f"Databasefout: {e}")
    finally:
        conn.close()


def export_pdf(report_type: str, report: dict, date: str):
    """Genereert of serveert PDF-bestand."""
    pdf_dir = f"backend/static/reports/{report_type}"
    os.makedirs(pdf_dir, exist_ok=True)
    pdf_path = os.path.join(pdf_dir, f"{report_type}_report_{date}.pdf")

    pdf_buffer = generate_pdf_report(report)
    with open(pdf_path, "wb") as f:
        f.write(pdf_buffer.getbuffer())

    logger.info(f"[export_pdf] ✅ PDF opgeslagen: {pdf_path}")
    return FileResponse(pdf_path, media_type="application/pdf", filename=os.path.basename(pdf_path))


# ==========================
# 📅 DAGRAPPORT
# ==========================

@router.get("/report/daily/latest")
async def get_daily_latest():
    logger.info("[get_daily_latest] 🚀 Request ontvangen")
    data = fetch_report("daily_reports")
    if not data:
        raise HTTPException(status_code=404, detail="Geen dagelijks rapport gevonden")
    return data


@router.get("/report/daily/by-date")
async def get_daily_by_date(date: str = Query(...)):
    logger.info(f"[get_daily_by_date] 🚀 Request ontvangen (date={date})")
    data = fetch_report("daily_reports", date)
    if not data:
        raise HTTPException(status_code=404, detail=f"Geen dagelijks rapport gevonden voor {date}")
    return data


@router.get("/report/daily/history")
async def get_daily_history():
    logger.info("[get_daily_history] 🚀 Request ontvangen")
    return fetch_history("daily_reports")


@router.post("/report/daily/generate")
async def generate_daily():
    logger.info("[generate_daily] 🚀 Celery taak starten")
    task = generate_daily_report.delay()
    return {"message": "Dagrapport taak gestart", "task_id": task.id}


@router.get("/report/daily/export/pdf")
async def export_daily_pdf(date: str = Query(...)):
    logger.info(f"[export_daily_pdf] 🚀 PDF export voor {date}")
    report = fetch_report("daily_reports", date)
    if not report:
        raise HTTPException(status_code=404, detail="Geen dagelijks rapport gevonden")
    return export_pdf("daily", report, date)


# ==========================
# 📈 WEEKRAPPORT
# ==========================

@router.get("/report/weekly/latest")
async def get_weekly_latest():
    logger.info("[get_weekly_latest] 🚀 Request ontvangen")
    data = fetch_report("weekly_reports")
    if not data:
        raise HTTPException(status_code=404, detail="Geen weekrapport gevonden")
    return data


@router.get("/report/weekly/by-date")
async def get_weekly_by_date(date: str = Query(...)):
    logger.info(f"[get_weekly_by_date] 🚀 Request ontvangen (date={date})")
    data = fetch_report("weekly_reports", date)
    if not data:
        raise HTTPException(status_code=404, detail=f"Geen weekrapport gevonden voor {date}")
    return data


@router.get("/report/weekly/history")
async def get_weekly_history():
    logger.info("[get_weekly_history] 🚀 Request ontvangen")
    return fetch_history("weekly_reports")


@router.post("/report/weekly/generate")
async def generate_weekly():
    logger.info("[generate_weekly] 🚀 Celery taak starten")
    task = generate_weekly_report.delay()
    return {"message": "Weekrapport taak gestart", "task_id": task.id}


@router.get("/report/weekly/export/pdf")
async def export_weekly_pdf(date: str = Query(...)):
    logger.info(f"[export_weekly_pdf] 🚀 PDF export voor {date}")
    report = fetch_report("weekly_reports", date)
    if not report:
        raise HTTPException(status_code=404, detail="Geen weekrapport gevonden")
    return export_pdf("weekly", report, date)


# ==========================
# 📊 MAANDRAPPORT
# ==========================

@router.get("/report/monthly/latest")
async def get_monthly_latest():
    logger.info("[get_monthly_latest] 🚀 Request ontvangen")
    data = fetch_report("monthly_reports")
    if not data:
        raise HTTPException(status_code=404, detail="Geen maandrapport gevonden")
    return data


@router.get("/report/monthly/by-date")
async def get_monthly_by_date(date: str = Query(...)):
    logger.info(f"[get_monthly_by_date] 🚀 Request ontvangen (date={date})")
    data = fetch_report("monthly_reports", date)
    if not data:
        raise HTTPException(status_code=404, detail=f"Geen maandrapport gevonden voor {date}")
    return data


@router.get("/report/monthly/history")
async def get_monthly_history():
    logger.info("[get_monthly_history] 🚀 Request ontvangen")
    return fetch_history("monthly_reports")


@router.post("/report/monthly/generate")
async def generate_monthly():
    logger.info("[generate_monthly] 🚀 Celery taak starten")
    task = generate_monthly_report.delay()
    return {"message": "Maandrapport taak gestart", "task_id": task.id}


@router.get("/report/monthly/export/pdf")
async def export_monthly_pdf(date: str = Query(...)):
    logger.info(f"[export_monthly_pdf] 🚀 PDF export voor {date}")
    report = fetch_report("monthly_reports", date)
    if not report:
        raise HTTPException(status_code=404, detail="Geen maandrapport gevonden")
    return export_pdf("monthly", report, date)


# ==========================
# 📉 KWARTAALRAPPORT
# ==========================

@router.get("/report/quarterly/latest")
async def get_quarterly_latest():
    logger.info("[get_quarterly_latest] 🚀 Request ontvangen")
    data = fetch_report("quarterly_reports")
    if not data:
        raise HTTPException(status_code=404, detail="Geen kwartaalrapport gevonden")
    return data


@router.get("/report/quarterly/by-date")
async def get_quarterly_by_date(date: str = Query(...)):
    logger.info(f"[get_quarterly_by_date] 🚀 Request ontvangen (date={date})")
    data = fetch_report("quarterly_reports", date)
    if not data:
        raise HTTPException(status_code=404, detail=f"Geen kwartaalrapport gevonden voor {date}")
    return data


@router.get("/report/quarterly/history")
async def get_quarterly_history():
    logger.info("[get_quarterly_history] 🚀 Request ontvangen")
    return fetch_history("quarterly_reports")


@router.post("/report/quarterly/generate")
async def generate_quarterly():
    logger.info("[generate_quarterly] 🚀 Celery taak starten")
    task = generate_quarterly_report.delay()
    return {"message": "Kwartaalrapport taak gestart", "task_id": task.id}


@router.get("/report/quarterly/export/pdf")
async def export_quarterly_pdf(date: str = Query(...)):
    logger.info(f"[export_quarterly_pdf] 🚀 PDF export voor {date}")
    report = fetch_report("quarterly_reports", date)
    if not report:
        raise HTTPException(status_code=404, detail="Geen kwartaalrapport gevonden")
    return export_pdf("quarterly", report, date)
