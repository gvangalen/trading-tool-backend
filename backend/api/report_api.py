from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from utils.db import get_db_connection
from datetime import datetime
import logging
import io
from fpdf import FPDF

router = APIRouter(prefix="/report")
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ✅ Laatste rapport ophalen
@router.get("/daily/latest")
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
@router.get("/daily/history")
async def get_daily_report_history(limit: int = 7):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Geen databaseverbinding")

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT report_date FROM daily_reports ORDER BY report_date DESC LIMIT %s", (limit,))
            rows = cur.fetchall()
            return [row[0].isoformat() for row in rows]  # Alleen datum als string
    except Exception as e:
        logger.error(f"❌ RAP02: Fout bij ophalen rapportgeschiedenis: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

# ✅ Rapport van specifieke datum ophalen
@router.get("/daily/{date}")
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

# ✅ Samenvatting ophalen voor de zijbalk
@router.get("/daily/summary")
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

# ✅ Rapport exporteren als PDF (laatste of specifieke datum)
@router.get("/daily/export/pdf")
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

            # 📄 PDF genereren
            pdf = FPDF()
            pdf.add_page()
            pdf.set_font("Arial", size=12)
            pdf.cell(200, 10, txt=f"Dagrapport {report['report_date']}", ln=True, align="C")
            pdf.ln(10)

            for key, value in report.items():
                if key != "report_date":
                    text = f"{key.replace('_', ' ').capitalize()}:\n{value}\n"
                    pdf.multi_cell(0, 10, txt=text)

            pdf_output = io.BytesIO()
            pdf.output(pdf_output)
            pdf_output.seek(0)

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
