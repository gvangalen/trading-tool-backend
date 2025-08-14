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


def fetch_report(table: str, date: str = None, limit: int = 1, as_options=False):
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

        if as_options:
            return [{"label": r[0].isoformat(), "value": r[0].isoformat()} for r in rows]

        if limit == 1:
            return dict(zip([desc[0] for desc in cur.description], rows[0])) if rows else None
        else:
            return [r[0].isoformat() for r in rows]
    except Exception as e:
        logger.error(f"\u274c RAP_FETCH: Fout bij ophalen {table}: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


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


# Helper om routes aan te maken

def create_report_routes(report_type: str, table: str, celery_task):
    @router.get(f"/{report_type}_report/latest")
    async def get_latest():
        report = fetch_report(table)
        return report or {}

    @router.get(f"/{report_type}_report/summary")
    async def get_summary():
        report = fetch_report(table)
        if not report:
            raise HTTPException(status_code=404, detail="Geen samenvatting beschikbaar")

        def safe(val): return val or "Niet beschikbaar"

        return {
            "report_date": report["report_date"].isoformat() if report.get("report_date") else "onbekend",
            "btc_summary": safe(report.get("btc_summary")),
            "macro_summary": safe(report.get("macro_summary")),
            "setup_checklist": safe(report.get("setup_checklist")),
            "priorities": safe(report.get("priorities")),
            "wyckoff_analysis": safe(report.get("wyckoff_analysis")),
            "recommendations": safe(report.get("recommendations")),
            "conclusion": safe(report.get("conclusion")),
            "outlook": safe(report.get("outlook")),
        }

    @router.get(f"/{report_type}_report/history")
    async def get_history(limit: int = 10):
        return fetch_report(table, limit=limit, as_options=True)

    @router.get(f"/{report_type}_report/{{date}}")
    async def get_by_date(date: str):
        try:
            datetime.strptime(date, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(status_code=400, detail="Ongeldige datum. Gebruik formaat YYYY-MM-DD")
        report = fetch_report(table, date=date)
        if not report:
            raise HTTPException(status_code=404, detail="Geen rapport gevonden")
        return report

    @router.get(f"/{report_type}_report/export/pdf")
    async def export_pdf(date: str = Query(default=None)):
        if date:
            try:
                datetime.strptime(date, "%Y-%m-%d")
            except ValueError:
                raise HTTPException(status_code=400, detail="Ongeldige datum. Gebruik formaat YYYY-MM-DD")
        report = fetch_report(table, date=date)
        if not report:
            raise HTTPException(status_code=404, detail="Geen rapport beschikbaar")
        pdf_output = export_report_pdf(report)
        filename = f"{report_type}_report_{report['report_date']}.pdf"
        return StreamingResponse(pdf_output, media_type="application/pdf", headers={
            "Content-Disposition": f"attachment; filename={filename}"
        })

    @router.post(f"/{report_type}_report/generate")
    async def generate_report():
        try:
            t = celery_task.delay()
            return {"message": f"{report_type.capitalize()}rapport wordt gegenereerd", "task_id": t.id}
        except Exception as e:
            logger.error(f"\u274c RAP_GEN: Fout bij starten Celery-task ({report_type}): {e}")
            raise HTTPException(status_code=500, detail=str(e))


# 🔁 Voeg routes toe voor elk type
create_report_routes("daily", "daily_reports", generate_daily_report)
create_report_routes("weekly", "weekly_reports", generate_weekly_report)
create_report_routes("monthly", "monthly_reports", generate_monthly_report)
create_report_routes("quarterly", "quarterly_reports", generate_quarterly_report)
