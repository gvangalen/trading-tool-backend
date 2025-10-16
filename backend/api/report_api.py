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
# HELPERS
# ==========================

def get_table_name(report_type: str):
    mapping = {
        "daily": "daily_reports",
        "weekly": "weekly_reports",
        "monthly": "monthly_reports",
        "quarterly": "quarterly_reports",
    }
    if report_type not in mapping:
        raise HTTPException(status_code=400, detail=f"Ongeldig report type: {report_type}")
    return mapping[report_type]

def get_generate_task(report_type: str):
    mapping = {
        "daily": generate_daily_report,
        "weekly": generate_weekly_report,
        "monthly": generate_monthly_report,
        "quarterly": generate_quarterly_report,
    }
    if report_type not in mapping:
        raise HTTPException(status_code=400, detail=f"Ongeldig report type: {report_type}")
    return mapping[report_type]

def fetch_report(table: str, date: str | None = None):
    """
    Haal één rapport op.
    - Met date: exact die dag + symbol='BTC'
    - Zonder date: laatste rapport voor symbol='BTC'
    """
    conn = get_db_connection()
    if not conn:
        logger.error(f"[fetch_report] ❌ Geen databaseverbinding voor {table}")
        raise HTTPException(status_code=500, detail="Geen databaseverbinding")

    try:
        with conn.cursor() as cur:
            if date:
                logger.info(f"[fetch_report] 📅 {table}: report_date={date}, symbol=BTC")
                cur.execute(
                    f"SELECT * FROM {table} WHERE report_date = %s AND symbol = %s",
                    (date, "BTC"),
                )
            else:
                logger.info(f"[fetch_report] 📄 {table}: laatste rapport (symbol=BTC)")
                cur.execute(
                    f"SELECT * FROM {table} WHERE symbol = %s ORDER BY report_date DESC LIMIT 1",
                    ("BTC",),
                )

            row = cur.fetchone()
            if not row:
                logger.warning(f"[fetch_report] ⚠️ Geen rapport in {table} voor {date or 'latest'} (symbol=BTC)")
                return {}

            data = dict(zip([d[0] for d in cur.description], row))
            logger.info(f"[fetch_report] ✅ Gevonden: {data.get('report_date')} ({data.get('symbol')})")
            return data
    except Exception as e:
        logger.exception(f"[fetch_report] ❌ Fout bij ophalen uit {table}: {e}")
        raise HTTPException(status_code=500, detail="Fout bij ophalen rapport")
    finally:
        conn.close()

def fetch_report_history(table: str):
    conn = get_db_connection()
    if not conn:
        logger.error(f"[fetch_report_history] ❌ Geen databaseverbinding voor {table}")
        raise HTTPException(status_code=500, detail="Geen databaseverbinding")
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT report_date FROM {table} ORDER BY report_date DESC LIMIT 30")
            rows = cur.fetchall()
            return [r[0] for r in rows]
    except Exception as e:
        logger.exception(f"[fetch_report_history] ❌ Fout bij ophalen history uit {table}: {e}")
        raise HTTPException(status_code=500, detail="Fout bij ophalen rapportgeschiedenis")
    finally:
        conn.close()

# ==========================
# 1. SPECIFIEKE ENDPOINTS PER TYPE (backwards compatible)
# ==========================

@router.get("/report/daily")
async def get_daily_report():
    logger.info("[get_daily_report] Dagelijks rapport ophalen (BTC)")
    conn = get_db_connection()
    if not conn:
        logger.error("[get_daily_report] ❌ Geen databaseverbinding")
        raise HTTPException(status_code=500, detail="Databaseverbinding mislukt")
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM daily_reports WHERE symbol = %s ORDER BY report_date DESC LIMIT 1",
                ("BTC",),
            )
            row = cur.fetchone()
            if not row:
                logger.warning("[get_daily_report] ⚠️ Geen BTC-rapport gevonden")
                return {}
            return dict(zip([d[0] for d in cur.description], row))
    except Exception as e:
        logger.error(f"[get_daily_report] ❌ Fout: {e}")
        raise HTTPException(status_code=500, detail="Fout bij ophalen dagelijks rapport")
    finally:
        conn.close()

@router.get("/report/weekly")
async def get_weekly_report():
    logger.info("[get_weekly_report] Wekelijks rapport ophalen (BTC)")
    conn = get_db_connection()
    if not conn:
        logger.error("[get_weekly_report] ❌ Geen databaseverbinding")
        raise HTTPException(status_code=500, detail="Databaseverbinding mislukt")
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM weekly_reports WHERE symbol = %s ORDER BY report_date DESC LIMIT 1",
                ("BTC",),
            )
            row = cur.fetchone()
            if not row:
                logger.warning("[get_weekly_report] ⚠️ Geen rapport gevonden")
                return {}
            return dict(zip([d[0] for d in cur.description], row))
    except Exception as e:
        logger.error(f"[get_weekly_report] ❌ Fout: {e}")
        raise HTTPException(status_code=500, detail="Fout bij ophalen wekelijks rapport")
    finally:
        conn.close()

@router.get("/report/monthly")
async def get_monthly_report():
    logger.info("[get_monthly_report] Maandelijks rapport ophalen (BTC)")
    conn = get_db_connection()
    if not conn:
        logger.error("[get_monthly_report] ❌ Geen databaseverbinding")
        raise HTTPException(status_code=500, detail="Databaseverbinding mislukt")
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM monthly_reports WHERE symbol = %s ORDER BY report_date DESC LIMIT 1",
                ("BTC",),
            )
            row = cur.fetchone()
            if not row:
                logger.warning("[get_monthly_report] ⚠️ Geen rapport gevonden")
                return {}
            return dict(zip([d[0] for d in cur.description], row))
    except Exception as e:
        logger.error(f"[get_monthly_report] ❌ Fout: {e}")
        raise HTTPException(status_code=500, detail="Fout bij ophalen maandelijks rapport")
    finally:
        conn.close()

@router.get("/report/quarterly")
async def get_quarterly_report():
    logger.info("[get_quarterly_report] Kwartaalrapport ophalen (BTC)")
    conn = get_db_connection()
    if not conn:
        logger.error("[get_quarterly_report] ❌ Geen databaseverbinding")
        raise HTTPException(status_code=500, detail="Databaseverbinding mislukt")
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM quarterly_reports WHERE symbol = %s ORDER BY report_date DESC LIMIT 1",
                ("BTC",),
            )
            row = cur.fetchone()
            if not row:
                logger.warning("[get_quarterly_report] ⚠️ Geen rapport gevonden")
                return {}
            return dict(zip([d[0] for d in cur.description], row))
    except Exception as e:
        logger.error(f"[get_quarterly_report] ❌ Fout: {e}")
        raise HTTPException(status_code=500, detail="Fout bij ophalen kwartaalrapport")
    finally:
        conn.close()

# ==========================
# 2. GENERIEKE ENDPOINTS (met datum)
# ==========================

@router.get("/report/{report_type}")
async def get_report_by_type(report_type: str, date: str | None = Query(None)):
    """
    Haal rapport op per type ('daily','weekly','monthly','quarterly').
    Optioneel ?date=YYYY-MM-DD voor een specifieke dag.
    """
    table = get_table_name(report_type)
    logger.info(f"[get_report_by_type] {report_type} voor {date or 'latest'}")
    try:
        report = fetch_report(table, date)
        if not report:
            logger.warning(f"[get_report_by_type] ⚠️ Geen rapport voor {report_type} ({date or 'latest'})")
            return {}
        return report
    except Exception as e:
        logger.error(f"[get_report_by_type] ❌ Fout ({report_type}): {e}")
        raise HTTPException(status_code=500, detail="Fout bij ophalen rapport")

@router.get("/report/{report_type}/history")
async def get_report_history(report_type: str):
    table = get_table_name(report_type)
    logger.info(f"[get_report_history] Geschiedenis uit {table}")
    return fetch_report_history(table)

@router.post("/report/{report_type}/generate")
async def generate_report(report_type: str):
    task = get_generate_task(report_type)
    try:
        celery_task = task.delay()
        logger.info(f"[generate_report] Celery taak gestart: {celery_task.id}")
        return {"message": "Taak gestart", "task_id": celery_task.id}
    except Exception as e:
        logger.error(f"[generate_report] ❌ Starten taak mislukt: {e}")
        raise HTTPException(status_code=500, detail="Fout bij starten Celery taak")

@router.get("/report/{report_type}/export/pdf")
async def export_report_pdf(report_type: str, date: str = Query(...)):
    """
    Download PDF voor een bestaand rapport.
    - We proberen eerst een cached PDF op schijf.
    - Bestaat die niet: rapport ophalen -> PDF genereren -> opslaan -> terugsturen.
    """
    table = get_table_name(report_type)
    logger.info(f"[export_report_pdf] PDF {report_type} op {date}")

    pdf_dir = f"backend/static/reports/{report_type}"
    pdf_path = os.path.join(pdf_dir, f"{report_type}_report_{date}.pdf")

    # 1) Cache hit
    if os.path.exists(pdf_path):
        logger.info(f"[export_report_pdf] 📄 Bestaande PDF: {pdf_path}")
        return FileResponse(pdf_path, media_type="application/pdf", filename=os.path.basename(pdf_path))

    # 2) Geen cache: haal rapport + maak PDF
    report = fetch_report(table, date)
    if not report:
        raise HTTPException(status_code=404, detail="Rapport niet gevonden")

    pdf_buffer = generate_pdf_report(report)  # -> BytesIO
    os.makedirs(pdf_dir, exist_ok=True)
    with open(pdf_path, "wb") as f:
        f.write(pdf_buffer.getbuffer())

    logger.info(f"[export_report_pdf] ✅ PDF opgeslagen: {pdf_path}")
    return FileResponse(pdf_path, media_type="application/pdf", filename=os.path.basename(pdf_path))
