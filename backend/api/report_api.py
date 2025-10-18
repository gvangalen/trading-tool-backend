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

# =====================================================
# 🔧 Setup
# =====================================================
router = APIRouter()
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("backend.api.report_api")


# =====================================================
# 🧾 PDF EXPORT HELPER
# =====================================================
def export_pdf(report_type: str, report: dict, date: str):
    """Genereert en serveert een PDF-bestand."""
    pdf_dir = f"backend/static/reports/{report_type}"
    os.makedirs(pdf_dir, exist_ok=True)
    pdf_path = os.path.join(pdf_dir, f"{report_type}_report_{date}.pdf")

    pdf_buffer = generate_pdf_report(report)
    with open(pdf_path, "wb") as f:
        f.write(pdf_buffer.getbuffer())

    logger.info(f"[export_pdf] ✅ PDF opgeslagen: {pdf_path}")
    return FileResponse(pdf_path, media_type="application/pdf", filename=os.path.basename(pdf_path))


# =====================================================
# 📅 DAGRAPPORT
# =====================================================
@router.get("/report/daily/latest")
async def get_daily_latest():
    logger.info("[get_daily_latest] 🚀 Request ontvangen")
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT * FROM daily_reports
                    ORDER BY report_date DESC, created_at DESC
                    LIMIT 1;
                """)
                row = cur.fetchone()
                if not row:
                    logger.warning("[get_daily_latest] ⚠️ Geen dagelijks rapport gevonden")
                    raise HTTPException(status_code=404, detail="Geen dagelijks rapport gevonden")
                data = dict(zip([desc[0] for desc in cur.description], row))
                logger.info(f"[get_daily_latest] ✅ Rapport gevonden: {data.get('report_date')}")
                return data
    except Exception as e:
        logger.exception(f"[get_daily_latest] ❌ Databasefout: {e}")
        raise HTTPException(status_code=500, detail="Databasefout")


@router.get("/report/daily/by-date")
async def get_daily_by_date(date: str = Query(...)):
    logger.info(f"[get_daily_by_date] 🚀 Request ontvangen (date={date})")
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT * FROM daily_reports
                    WHERE report_date = %s
                    ORDER BY created_at DESC
                    LIMIT 1;
                """, (date,))
                row = cur.fetchone()
                if not row:
                    logger.warning(f"[get_daily_by_date] ⚠️ Geen rapport gevonden voor {date}")
                    raise HTTPException(status_code=404, detail=f"Geen dagelijks rapport gevonden voor {date}")
                return dict(zip([desc[0] for desc in cur.description], row))
    except Exception as e:
        logger.exception(f"[get_daily_by_date] ❌ Databasefout: {e}")
        raise HTTPException(status_code=500, detail="Databasefout")


@router.get("/report/daily/history")
async def get_daily_history():
    logger.info("[get_daily_history] 🚀 Request ontvangen")
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT report_date FROM daily_reports ORDER BY report_date DESC LIMIT 30;")
                result = [r[0] for r in cur.fetchall()]
                logger.info(f"[get_daily_history] ✅ {len(result)} datums opgehaald")
                return result
    except Exception as e:
        logger.exception(f"[get_daily_history] ❌ Databasefout: {e}")
        raise HTTPException(status_code=500, detail="Databasefout")


@router.post("/report/daily/generate")
async def generate_daily():
    logger.info("[generate_daily] 🚀 Celery taak starten")
    task = generate_daily_report.delay()
    return {"message": "Dagrapport taak gestart", "task_id": task.id}


@router.get("/report/daily/export/pdf")
async def export_daily_pdf(date: str = Query(...)):
    logger.info(f"[export_daily_pdf] 🚀 PDF export voor {date}")
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT * FROM daily_reports
                    WHERE report_date = %s
                    ORDER BY created_at DESC
                    LIMIT 1;
                """, (date,))
                row = cur.fetchone()
                if not row:
                    logger.warning(f"[export_daily_pdf] ⚠️ Geen rapport gevonden voor {date}")
                    raise HTTPException(status_code=404, detail="Geen dagelijks rapport gevonden")
                report = dict(zip([desc[0] for desc in cur.description], row))
                return export_pdf("daily", report, date)
    except Exception as e:
        logger.exception(f"[export_daily_pdf] ❌ Fout bij export: {e}")
        raise HTTPException(status_code=500, detail="Fout bij PDF-export")


# =====================================================
# 📈 WEEKRAPPORT
# =====================================================
@router.get("/report/weekly/latest")
async def get_weekly_latest():
    logger.info("[get_weekly_latest] 🚀 Request ontvangen")
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT * FROM weekly_reports
                    ORDER BY report_date DESC, created_at DESC
                    LIMIT 1;
                """)
                row = cur.fetchone()
                if not row:
                    logger.warning("[get_weekly_latest] ⚠️ Geen weekrapport gevonden")
                    raise HTTPException(status_code=404, detail="Geen weekrapport gevonden")
                return dict(zip([desc[0] for desc in cur.description], row))
    except Exception as e:
        logger.exception(f"[get_weekly_latest] ❌ Databasefout: {e}")
        raise HTTPException(status_code=500, detail="Databasefout")


@router.get("/report/weekly/by-date")
async def get_weekly_by_date(date: str = Query(...)):
    logger.info(f"[get_weekly_by_date] 🚀 Request ontvangen (date={date})")
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT * FROM weekly_reports
                    WHERE report_date = %s
                    ORDER BY created_at DESC
                    LIMIT 1;
                """, (date,))
                row = cur.fetchone()
                if not row:
                    logger.warning(f"[get_weekly_by_date] ⚠️ Geen weekrapport gevonden voor {date}")
                    raise HTTPException(status_code=404, detail=f"Geen weekrapport gevonden voor {date}")
                return dict(zip([desc[0] for desc in cur.description], row))
    except Exception as e:
        logger.exception(f"[get_weekly_by_date] ❌ Databasefout: {e}")
        raise HTTPException(status_code=500, detail="Databasefout")


@router.get("/report/weekly/history")
async def get_weekly_history():
    logger.info("[get_weekly_history] 🚀 Request ontvangen")
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT report_date FROM weekly_reports ORDER BY report_date DESC LIMIT 30;")
                return [r[0] for r in cur.fetchall()]
    except Exception as e:
        logger.exception(f"[get_weekly_history] ❌ Databasefout: {e}")
        raise HTTPException(status_code=500, detail="Databasefout")


@router.post("/report/weekly/generate")
async def generate_weekly():
    logger.info("[generate_weekly] 🚀 Celery taak starten")
    task = generate_weekly_report.delay()
    return {"message": "Weekrapport taak gestart", "task_id": task.id}


@router.get("/report/weekly/export/pdf")
async def export_weekly_pdf(date: str = Query(...)):
    logger.info(f"[export_weekly_pdf] 🚀 PDF export voor {date}")
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT * FROM weekly_reports
                    WHERE report_date = %s
                    ORDER BY created_at DESC
                    LIMIT 1;
                """, (date,))
                row = cur.fetchone()
                if not row:
                    logger.warning(f"[export_weekly_pdf] ⚠️ Geen weekrapport gevonden voor {date}")
                    raise HTTPException(status_code=404, detail="Geen weekrapport gevonden")
                report = dict(zip([desc[0] for desc in cur.description], row))
                return export_pdf("weekly", report, date)
    except Exception as e:
        logger.exception(f"[export_weekly_pdf] ❌ Fout bij export: {e}")
        raise HTTPException(status_code=500, detail="Fout bij PDF-export")


# =====================================================
# 📊 MAANDRAPPORT
# =====================================================
@router.get("/report/monthly/latest")
async def get_monthly_latest():
    logger.info("[get_monthly_latest] 🚀 Request ontvangen")
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT * FROM monthly_reports
                    ORDER BY report_date DESC, created_at DESC
                    LIMIT 1;
                """)
                row = cur.fetchone()
                if not row:
                    logger.warning("[get_monthly_latest] ⚠️ Geen maandrapport gevonden")
                    raise HTTPException(status_code=404, detail="Geen maandrapport gevonden")
                return dict(zip([desc[0] for desc in cur.description], row))
    except Exception as e:
        logger.exception(f"[get_monthly_latest] ❌ Databasefout: {e}")
        raise HTTPException(status_code=500, detail="Databasefout")


@router.get("/report/monthly/by-date")
async def get_monthly_by_date(date: str = Query(...)):
    logger.info(f"[get_monthly_by_date] 🚀 Request ontvangen (date={date})")
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT * FROM monthly_reports
                    WHERE report_date = %s
                    ORDER BY created_at DESC
                    LIMIT 1;
                """, (date,))
                row = cur.fetchone()
                if not row:
                    logger.warning(f"[get_monthly_by_date] ⚠️ Geen maandrapport gevonden voor {date}")
                    raise HTTPException(status_code=404, detail=f"Geen maandrapport gevonden voor {date}")
                return dict(zip([desc[0] for desc in cur.description], row))
    except Exception as e:
        logger.exception(f"[get_monthly_by_date] ❌ Databasefout: {e}")
        raise HTTPException(status_code=500, detail="Databasefout")


@router.get("/report/monthly/history")
async def get_monthly_history():
    logger.info("[get_monthly_history] 🚀 Request ontvangen")
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT report_date FROM monthly_reports ORDER BY report_date DESC LIMIT 30;")
                return [r[0] for r in cur.fetchall()]
    except Exception as e:
        logger.exception(f"[get_monthly_history] ❌ Databasefout: {e}")
        raise HTTPException(status_code=500, detail="Databasefout")


@router.post("/report/monthly/generate")
async def generate_monthly():
    logger.info("[generate_monthly] 🚀 Celery taak starten")
    task = generate_monthly_report.delay()
    return {"message": "Maandrapport taak gestart", "task_id": task.id}


@router.get("/report/monthly/export/pdf")
async def export_monthly_pdf(date: str = Query(...)):
    logger.info(f"[export_monthly_pdf] 🚀 PDF export voor {date}")
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT * FROM monthly_reports
                    WHERE report_date = %s
                    ORDER BY created_at DESC
                    LIMIT 1;
                """, (date,))
                row = cur.fetchone()
                if not row:
                    logger.warning(f"[export_monthly_pdf] ⚠️ Geen maandrapport gevonden voor {date}")
                    raise HTTPException(status_code=404, detail="Geen maandrapport gevonden")
                report = dict(zip([desc[0] for desc in cur.description], row))
                return export_pdf("monthly", report, date)
    except Exception as e:
        logger.exception(f"[export_monthly_pdf] ❌ Fout bij export: {e}")
        raise HTTPException(status_code=500, detail="Fout bij PDF-export")


# =====================================================
# 📉 KWARTAALRAPPORT
# =====================================================
@router.get("/report/quarterly/latest")
async def get_quarterly_latest():
    logger.info("[get_quarterly_latest] 🚀 Request ontvangen")
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT * FROM quarterly_reports
                    ORDER BY report_date DESC, created_at DESC
                    LIMIT 1;
                """)
                row = cur.fetchone()
                if not row:
                    logger.warning("[get_quarterly_latest] ⚠️ Geen kwartaalrapport gevonden")
                    raise HTTPException(status_code=404, detail="Geen kwartaalrapport gevonden")
                return dict(zip([desc[0] for desc in cur.description], row))
    except Exception as e:
        logger.exception(f"[get_quarterly_latest] ❌ Databasefout: {e}")
        raise HTTPException(status_code=500, detail="Databasefout")


@router.get("/report/quarterly/by-date")
async def get_quarterly_by_date(date: str = Query(...)):
    logger.info(f"[get_quarterly_by_date] 🚀 Request ontvangen (date={date})")
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT * FROM quarterly_reports
                    WHERE report_date = %s
                    ORDER BY created_at DESC
                    LIMIT 1;
                """, (date,))
                row = cur.fetchone()
                if not row:
                    logger.warning(f"[get_quarterly_by_date] ⚠️ Geen kwartaalrapport gevonden voor {date}")
                    raise HTTPException(status_code=404, detail=f"Geen kwartaalrapport gevonden voor {date}")
                return dict(zip([desc[0] for desc in cur.description], row))
    except Exception as e:
        logger.exception(f"[get_quarterly_by_date] ❌ Databasefout: {e}")
        raise HTTPException(status_code=500, detail="Databasefout")


@router.get("/report/quarterly/history")
async def get_quarterly_history():
    logger.info("[get_quarterly_history] 🚀 Request ontvangen")
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT report_date FROM quarterly_reports ORDER BY report_date DESC LIMIT 30;")
                return [r[0] for r in cur.fetchall()]
    except Exception as e:
        logger.exception(f"[get_quarterly_history] ❌ Databasefout: {e}")
        raise HTTPException(status_code=500, detail="Databasefout")


@router.post("/report/quarterly/generate")
async def generate_quarterly():
    logger.info("[generate_quarterly] 🚀 Celery taak starten")
    task = generate_quarterly_report.delay()
    return {"message": "Kwartaalrapport taak gestart", "task_id": task.id}


@router.get("/report/quarterly/export/pdf")
async def export_quarterly_pdf(date: str = Query(...)):
    logger.info(f"[export_quarterly_pdf] 🚀 PDF export voor {date}")
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT * FROM quarterly_reports
                    WHERE report_date = %s
                    ORDER BY created_at DESC
                    LIMIT 1;
                """, (date,))
                row = cur.fetchone()
                if not row:
                    logger.warning(f"[export_quarterly_pdf] ⚠️ Geen kwartaalrapport gevonden voor {date}")
                    raise HTTPException(status_code=404, detail="Geen kwartaalrapport gevonden")
                report = dict(zip([desc[0] for desc in cur.description], row))
                return export_pdf("quarterly", report, date)
    except Exception as e:
        logger.exception(f"[export_quarterly_pdf] ❌ Fout bij export: {e}")
        raise HTTPException(status_code=500, detail="Fout bij PDF-export")
