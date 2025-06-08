# report_api.py
from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from utils.db import get_db_connection   # correct
from datetime import datetime
import logging
import io
from fpdf import FPDF

router = APIRouter()
logger = logging.getLogger(__name__)

# ✅ Laatste rapport ophalen
@router.get("/daily_report/latest")
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
        logger.error(f"❌ Fout bij ophalen laatste rapport: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

# ✅ Rapportgeschiedenis ophalen
@router.get("/daily_report/history")
async def get_daily_report_history(limit: int = 7):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Geen databaseverbinding")

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM daily_reports ORDER BY report_date DESC LIMIT %s", (limit,))
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            return [dict(zip(columns, row)) for row in rows]
    except Exception as e:
        logger.error(f"❌ Fout bij ophalen rapportgeschiedenis: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

# ✅ Rapport van specifieke dag ophalen
@router.get("/daily_report/{date}")
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
        logger.error(f"❌ Fout bij ophalen rapport van datum {date}: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

# ✅ Rapport exporteren als PDF
@router.get("/daily_report/export/pdf")
async def export_daily_report_pdf():
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Geen databaseverbinding")

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM daily_reports ORDER BY report_date DESC LIMIT 1")
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Geen rapport beschikbaar")
            columns = [desc[0] for desc in cur.description]
            report = dict(zip(columns, row))

            pdf = FPDF()
            pdf.add_page()
            pdf.set_font("Arial", size=12)

            pdf.cell(200, 10, txt=f"Dagrapport {report['report_date']}", ln=True, align="C")
            for key, value in report.items():
                if key != "report_date":
                    pdf.multi_cell(0, 10, txt=f"{key.replace('_', ' ').capitalize()}:\n{value}\n")

            pdf_output = io.BytesIO()
            pdf.output(pdf_output)
            pdf_output.seek(0)

            return StreamingResponse(
                pdf_output,
                media_type="application/pdf",
                headers={"Content-Disposition": "attachment; filename=dagrapport.pdf"}
            )
    except Exception as e:
        logger.error(f"❌ Fout bij exporteren rapport naar PDF: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()
