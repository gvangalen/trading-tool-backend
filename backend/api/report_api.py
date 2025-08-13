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

# ⬇️ Helper voor rapporttype
def fetch_report(table: str, date: str = None, limit: int = 1):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Geen databaseverbinding")
    try:
        cur = conn.cursor()
        if date:
            cur.execute(f"SELECT * FROM {table} WHERE report_date = %s", (date,))
        else:
            cur.execute(f"SELECT * FROM {table} ORDER BY report_date DESC LIMIT %s", (limit,))
        rows = cur.fetchall()
        if limit == 1:
            return dict(zip([desc[0] for desc in cur.description], rows[0])) if rows else None
        else:
            return [row[0].isoformat() for row in rows]
    except Exception as e:
        logger.error(f"❌ RAP_FETCH: Fout bij ophalen {table}: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


# ⬇️ Helper voor PDF
def export_report_pdf(report: dict):
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
    return generate_pdf_report(structured)


# ✅ Endpoints per rapporttype (daily, weekly, monthly, quarterly)
for report_type, table, celery_task in [
    ("daily",     "daily_reports",     generate_daily_report),
    ("weekly",    "weekly_reports",    generate_weekly_report),
    ("monthly",   "monthly_reports",   generate_monthly_report),
    ("quarterly", "quarterly_reports", generate_quarterly_report),
]:

    # 🟢 LAATSTE
    @router.get(f"/{report_type}_report/latest")
    async def get_latest_report(rt=report_type, tbl=table):
        report = fetch_report(tbl)
        if not report:
            return {}
        return report

    # 🟢 SAMENVATTING
    @router.get(f"/{report_type}_report/summary")
    async def get_summary(rt=report_type, tbl=table):
        report = fetch_report(tbl)
        if not report:
            raise HTTPException(status_code=404, detail="Geen samenvatting beschikbaar")

        def safe(val):
            return val if val is not None else "Niet beschikbaar"

        return {
            "report_date": report.get("report_date", "onbekend").isoformat() if report.get("report_date") else "onbekend",
            "btc_summary": safe(report.get("btc_summary")),
            "macro_summary": safe(report.get("macro_summary")),
            "setup_checklist": safe(report.get("setup_checklist")),
            "priorities": safe(report.get("priorities")),
            "wyckoff_analysis": safe(report.get("wyckoff_analysis")),
            "recommendations": safe(report.get("recommendations")),
            "conclusion": safe(report.get("conclusion")),
            "outlook": safe(report.get("outlook")),
        }

    # 🟢 HISTORIE
    @router.get(f"/{report_type}_report/history")
    async def get_history(limit: int = 7, tbl=table):
        return fetch_report(tbl, limit=limit)

    # 🟢 OP DATUM
    @router.get(f"/{report_type}_report/{{date}}")
    async def get_by_date(date: str, tbl=table):
        try:
            datetime.strptime(date, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(status_code=400, detail="Ongeldige datum. Gebruik formaat YYYY-MM-DD")
        report = fetch_report(tbl, date=date)
        if not report:
            raise HTTPException(status_code=404, detail="Geen rapport gevonden")
        return report

    # 🟢 EXPORT PDF
    @router.get(f"/{report_type}_report/export/pdf")
    async def export_pdf(date: str = Query(default=None), tbl=table):
        if date:
            try:
                datetime.strptime(date, "%Y-%m-%d")
            except ValueError:
                raise HTTPException(status_code=400, detail="Ongeldige datum. Gebruik formaat YYYY-MM-DD")
        report = fetch_report(tbl, date=date)
        if not report:
            raise HTTPException(status_code=404, detail="Geen rapport beschikbaar")
        pdf_output = export_report_pdf(report)
        filename = f"{rt}rapport_{report['report_date']}.pdf"
        return StreamingResponse(pdf_output, media_type="application/pdf", headers={
            "Content-Disposition": f"attachment; filename={filename}"
        })

    # 🟢 GENEREREN
    @router.post(f"/{report_type}_report/generate")
    async def generate_report(task=celery_task):
        try:
            t = task.delay()
            return {"message": f"{report_type.capitalize()}rapport wordt gegenereerd", "task_id": t.id}
        except Exception as e:
            logger.error(f"❌ RAP06: Fout bij starten Celery-task ({report_type}): {e}")
            raise HTTPException(status_code=500, detail=str(e))
