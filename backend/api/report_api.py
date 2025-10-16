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

# ==========================
# CONFIGURATIE
# ==========================

router = APIRouter()
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ==========================
# HELPERS
# ==========================

def get_table_name(report_type: str):
    """Koppel report_type aan de juiste databasetabel."""
    mapping = {
        "daily": "daily_reports",
        "weekly": "weekly_reports",
        "monthly": "monthly_reports",
        "quarterly": "quarterly_reports",
    }
    if report_type not in mapping:
        logger.error(f"[get_table_name] ❌ Ongeldig report_type: {report_type}")
        raise HTTPException(status_code=400, detail=f"Ongeldig report type: {report_type}")
    return mapping[report_type]


def get_generate_task(report_type: str):
    """Koppel report_type aan de juiste Celery-taak."""
    mapping = {
        "daily": generate_daily_report,
        "weekly": generate_weekly_report,
        "monthly": generate_monthly_report,
        "quarterly": generate_quarterly_report,
    }
    if report_type not in mapping:
        logger.error(f"[get_generate_task] ❌ Ongeldig report_type: {report_type}")
        raise HTTPException(status_code=400, detail=f"Ongeldig report type: {report_type}")
    return mapping[report_type]


def fetch_report(table: str, date: str | None = None):
    """📄 Haalt één rapport op uit de database (optioneel met datum)."""
    conn = get_db_connection()
    if not conn:
        logger.error(f"[fetch_report] ❌ Geen databaseverbinding voor {table}")
        raise HTTPException(status_code=500, detail="Geen databaseverbinding")

    try:
        with conn.cursor() as cur:
            if date:
                logger.info(f"[fetch_report] 📅 Ophalen {table}: report_date={date}, symbol=BTC")
                # 🔧 cast naar tekst voorkomt typeproblemen bij vergelijking
                cur.execute(
                    f"SELECT * FROM {table} WHERE report_date::text = %s AND symbol = %s",
                    (date, "BTC"),
                )
            else:
                logger.info(f"[fetch_report] 📄 Ophalen laatste rapport uit {table} (symbol=BTC)")
                cur.execute(
                    f"SELECT * FROM {table} WHERE symbol = %s ORDER BY report_date DESC LIMIT 1",
                    ("BTC",),
                )

            row = cur.fetchone()
            if not row:
                logger.warning(f"[fetch_report] ⚠️ Geen rapport gevonden in {table} ({date or 'latest'})")
                return {}

            columns = [d[0] for d in cur.description]
            data = dict(zip(columns, row))
            logger.info(f"[fetch_report] ✅ Rapport gevonden: {data.get('report_date')} ({data.get('symbol')})")
            return data

    except Exception as e:
        logger.exception(f"[fetch_report] ❌ Fout bij ophalen uit {table}: {e}")
        raise HTTPException(status_code=500, detail="Fout bij ophalen rapport")
    finally:
        conn.close()


def fetch_report_history(table: str):
    """📜 Haal de laatste 30 rapportdatums op uit de database."""
    conn = get_db_connection()
    if not conn:
        logger.error(f"[fetch_report_history] ❌ Geen databaseverbinding voor {table}")
        raise HTTPException(status_code=500, detail="Geen databaseverbinding")

    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT report_date FROM {table} ORDER BY report_date DESC LIMIT 30")
            rows = [r[0] for r in cur.fetchall()]
            logger.info(f"[fetch_report_history] ✅ {len(rows)} datums gevonden in {table}")
            return rows
    except Exception as e:
        logger.exception(f"[fetch_report_history] ❌ Fout bij ophalen history uit {table}: {e}")
        raise HTTPException(status_code=500, detail="Fout bij ophalen rapportgeschiedenis")
    finally:
        conn.close()

# ==========================
# 1. FRONTEND-COMPATIBELE ROUTES
# ==========================

@router.get("/report/{report_type}/latest")
async def get_latest_report(report_type: str):
    """✅ Haal het laatste rapport op (bijv. /api/report/daily/latest)."""
    table = get_table_name(report_type)
    logger.info(f"[get_latest_report] Route aangeroepen voor {report_type}")
    try:
        report = fetch_report(table)
        if not report:
            logger.warning(f"[get_latest_report] ⚠️ Geen rapport gevonden in {table}")
            raise HTTPException(status_code=404, detail="Geen rapport gevonden")
        return report
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"[get_latest_report] ❌ Fout: {e}")
        raise HTTPException(status_code=500, detail="Fout bij ophalen laatste rapport")


@router.get("/report/{report_type}/by-date")
async def get_report_by_date(report_type: str, date: str = Query(...)):
    """✅ Haal rapport op voor specifieke datum (bijv. /api/report/daily/by-date?date=2025-10-14)."""
    table = get_table_name(report_type)
    logger.info(f"[get_report_by_date] Route aangeroepen: type={report_type}, date={date}")

    try:
        report = fetch_report(table, date)
        if not report:
            logger.warning(f"[get_report_by_date] ⚠️ Geen rapport gevonden in {table} voor {date}")
            raise HTTPException(status_code=404, detail=f"Geen rapport gevonden voor {date}")
        return report
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"[get_report_by_date] ❌ Fout: {e}")
        raise HTTPException(status_code=500, detail="Fout bij ophalen rapport op datum")


@router.get("/report/{report_type}/history")
async def get_report_history(report_type: str):
    """📜 Geef een lijst met de laatste 30 rapportdatums terug."""
    table = get_table_name(report_type)
    logger.info(f"[get_report_history] Route aangeroepen voor {report_type}")
    return fetch_report_history(table)

# ==========================
# 2. GENEREREN EN EXPORTEREN
# ==========================

@router.post("/report/{report_type}/generate")
async def generate_report(report_type: str):
    """🧠 Start Celery-taak om rapport te genereren."""
    task = get_generate_task(report_type)
    logger.info(f"[generate_report] Route aangeroepen voor {report_type}")

    try:
        celery_task = task.delay()
        logger.info(f"[generate_report] ✅ Celery-taak gestart: {celery_task.id}")
        return {"message": "Taak gestart", "task_id": celery_task.id}
    except Exception as e:
        logger.exception(f"[generate_report] ❌ Starten taak mislukt: {e}")
        raise HTTPException(status_code=500, detail="Fout bij starten Celery-taak")


@router.get("/report/{report_type}/export/pdf")
async def export_report_pdf(report_type: str, date: str = Query(...)):
    """📄 Exporteer of genereer PDF voor rapport."""
    table = get_table_name(report_type)
    logger.info(f"[export_report_pdf] Route aangeroepen: type={report_type}, date={date}")

    pdf_dir = f"backend/static/reports/{report_type}"
    pdf_path = os.path.join(pdf_dir, f"{report_type}_report_{date}.pdf")

    # ✅ Cache check
    if os.path.exists(pdf_path):
        logger.info(f"[export_report_pdf] 📄 Bestaande PDF teruggegeven: {pdf_path}")
        return FileResponse(pdf_path, media_type="application/pdf", filename=os.path.basename(pdf_path))

    # 🚀 Anders genereren
    report = fetch_report(table, date)
    if not report:
        logger.warning(f"[export_report_pdf] ⚠️ Geen rapport gevonden voor {report_type} op {date}")
        raise HTTPException(status_code=404, detail="Rapport niet gevonden")

    pdf_buffer = generate_pdf_report(report)
    os.makedirs(pdf_dir, exist_ok=True)

    with open(pdf_path, "wb") as f:
        f.write(pdf_buffer.getbuffer())

    logger.info(f"[export_report_pdf] ✅ PDF succesvol opgeslagen: {pdf_path}")
    return FileResponse(pdf_path, media_type="application/pdf", filename=os.path.basename(pdf_path))
