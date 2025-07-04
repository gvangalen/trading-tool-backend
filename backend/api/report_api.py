import logging
import os
import io
import traceback
from datetime import datetime

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse

from utils.db import get_db_connection
from utils.pdf_generator import generate_pdf_report
from celery_task.daily_report_task import generate_daily_report  # ✅ Celery-task import

# ✅ Router met prefix
router = APIRouter(prefix="/daily_report")

# ✅ Logging instellen
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# ✅ Laatste rapport ophalen
@router.get("/latest")
async def get_latest_daily_report():
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Geen databaseverbinding")

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM daily_reports ORDER BY report_date DESC LIMIT 1")
            row = cur.fetchone()
            if not row:
                return {}
            columns = [desc[0] for desc in cur.description]
            return dict(zip(columns, row))
    except Exception as e:
        logger.error(f"❌ RAP01: Fout bij ophalen laatste rapport: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


# ✅ Rapportgeschiedenis ophalen (alleen datums)
@router.get("/history")
async def get_daily_report_history(limit: int = 7):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Geen databaseverbinding")
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT report_date FROM daily_reports ORDER BY report_date DESC LIMIT %s", (limit,))
            rows = cur.fetchall()
            return [row[0].isoformat() for row in rows]
    except Exception as e:
        logger.error(f"❌ RAP02: Fout bij ophalen rapportgeschiedenis: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


# ✅ Rapport op specifieke datum ophalen
@router.get("/{date}")
async def get_daily_report_by_date(date: str):
    try:
        datetime.strptime(date, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400, detail="Ongeldige datum. Gebruik formaat YYYY-MM-DD")

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Geen databaseverbinding")

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM daily_reports WHERE report_date = %s", (date,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Geen rapport gevonden voor deze datum")
            columns = [desc[0] for desc in cur.description]
            return dict(zip(columns, row))
    except Exception as e:
        logger.error(f"❌ RAP03: Fout bij ophalen rapport van {date}: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


# ✅ Samenvatting ophalen (voor dashboard)
@router.get("/summary")
async def get_daily_report_summary():
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Geen databaseverbinding")

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT summary FROM daily_reports ORDER BY report_date DESC LIMIT 1")
            row = cur.fetchone()
            if not row:
                return {"summary": "Geen samenvatting beschikbaar"}
            return {"summary": row[0]}
    except Exception as e:
        logger.error(f"❌ RAP05: Fout bij ophalen samenvatting: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


# ✅ PDF-export van rapport (laatste of specifieke datum)
@router.get("/export/pdf")
async def export_daily_report_pdf(date: str = Query(default=None)):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Geen databaseverbinding")

    try:
        with conn.cursor() as cur:
            if date:
                try:
                    datetime.strptime(date, "%Y-%m-%d")
                except ValueError:
                    raise HTTPException(status_code=400, detail="Ongeldige datum. Gebruik formaat YYYY-MM-DD")
                cur.execute("SELECT * FROM daily_reports WHERE report_date = %s", (date,))
            else:
                cur.execute("SELECT * FROM daily_reports ORDER BY report_date DESC LIMIT 1")

            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Geen rapport beschikbaar")

            columns = [desc[0] for desc in cur.description]
            report = dict(zip(columns, row))

            # ✅ Structuur mappen voor PDF
            structured = {
                "summary": report.get("btc_summary", ""),
                "macro": report.get("macro_summary", ""),
                "technical": report.get("setup_checklist", ""),
                "setups": report.get("priorities", ""),
                "strategy": report.get("wyckoff_analysis", ""),
                "recommendation": report.get("recommendations", ""),
                "conclusion": report.get("conclusion", ""),
                "outlook": report.get("outlook", ""),
            }

            pdf_output = generate_pdf_report(structured)

            return StreamingResponse(
                pdf_output,
                media_type="application/pdf",
                headers={"Content-Disposition": f"attachment; filename=dagrapport_{report['report_date']}.pdf"}
            )
    except Exception as e:
        logger.error(f"❌ RAP04: Fout bij PDF-export: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


# ✅ Handmatig rapport genereren via Celery
@router.post("/generate")
async def trigger_generate_daily_report():
    try:
        task = generate_daily_report.delay()
        return {"message": "Dagrapport wordt gegenereerd", "task_id": task.id}
    except Exception as e:
        logger.error(f"❌ RAP06: Fout bij starten Celery-task: {e}")
        raise HTTPException(status_code=500, detail=str(e))
