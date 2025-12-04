print("🟢 report_api wordt geladen ✅")

import logging
import os
from datetime import datetime
from fastapi import APIRouter, HTTPException, Query, Depends
from fastapi.responses import FileResponse

from backend.utils.db import get_db_connection
from backend.utils.pdf_generator import generate_pdf_report
from backend.ai_agents.report_ai_agent import generate_daily_report_sections
from backend.celery_task.daily_report_task import generate_daily_report
from backend.celery_task.weekly_report_task import generate_weekly_report
from backend.celery_task.monthly_report_task import generate_monthly_report
from backend.celery_task.quarterly_report_task import generate_quarterly_report
from backend.utils.auth_utils import get_current_user  # 🔐 user uit token

router = APIRouter()
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


# ======================================================
# 📄 PDF export helper — nu user-specifiek
# ======================================================
def export_pdf(report_type: str, report: dict, date: str, user_id: int):
    """
    Maak een PDF-bestand aan in static/pdf/<type>/ en geef FileResponse terug.
    Bestandsnaam bevat nu ook user_id zodat meerdere users
    niet elkaars rapport-bestanden overschrijven.
    """
    # 📁 Folder per report-type
    pdf_dir = os.path.join("static", "pdf", report_type)
    os.makedirs(pdf_dir, exist_ok=True)

    # 📄 Bestandsnaam met user_id
    filename = f"{report_type}_report_user_{user_id}_{date}.pdf"
    pdf_path = os.path.join(pdf_dir, filename)

    # 🛠️ PDF genereren (in memory) en opslaan
    pdf_buffer = generate_pdf_report(report, report_type=report_type, save_to_disk=False)
    with open(pdf_path, "wb") as f:
        f.write(pdf_buffer.getbuffer())

    logger.info(f"[export_pdf] ✅ PDF opgeslagen op: {pdf_path}")

    return FileResponse(
        pdf_path,
        media_type="application/pdf",
        filename=filename
    )


# ======================================================
# 🟢 DAILY REPORTS
# ======================================================

@router.get("/report/daily/latest")
async def get_daily_latest(current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT *
                FROM daily_reports
                WHERE user_id = %s
                ORDER BY report_date DESC
                LIMIT 1;
                """,
                (user_id,)
            )
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Geen dagelijks rapport gevonden")
            cols = [desc[0] for desc in cur.description]
            return dict(zip(cols, row))
    finally:
        conn.close()


@router.get("/report/daily/by-date")
async def get_daily_by_date(
    date: str = Query(...),
    current_user: dict = Depends(get_current_user)
):
    user_id = current_user["id"]
    try:
        parsed_date = datetime.strptime(date, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(status_code=400, detail="Ongeldig datumformaat. Gebruik YYYY-MM-DD.")

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT *
                FROM daily_reports
                WHERE report_date = %s AND user_id = %s
                LIMIT 1;
                """,
                (parsed_date, user_id)
            )
            row = cur.fetchone()
            if not row:
                raise HTTPException(
                    status_code=404,
                    detail=f"Geen dagelijks rapport gevonden voor {parsed_date}"
                )
            cols = [desc[0] for desc in cur.description]
            return dict(zip(cols, row))
    finally:
        conn.close()


@router.get("/report/daily/history")
async def get_daily_report_history(
    current_user: dict = Depends(get_current_user)
):
    logger.info("🚀 Daily report history endpoint aangeroepen")
    user_id = current_user["id"]
    conn = get_db_connection()

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT report_date
                FROM daily_reports
                WHERE user_id = %s
                ORDER BY report_date DESC
                LIMIT 10;
                """,
                (user_id,)
            )
            rows = cur.fetchall()
            logger.info(f"🧪 Gevonden {len(rows)} daily reports voor user {user_id}: {rows}")
            if not rows:
                raise HTTPException(status_code=404, detail="Geen daily reports gevonden.")
            return [r[0].isoformat() for r in rows]
    finally:
        conn.close()


@router.post("/report/daily/generate")
async def generate_daily(current_user: dict = Depends(get_current_user)):
    """
    Genereert een daily report *preview* via de AI-agent (zonder in DB op te slaan).
    Voor het echte opslaan gebruik je de Celery-task `generate_daily_report` elders.
    """
    user_id = current_user["id"]
    try:
        # 💡 Zorg dat generate_daily_report_sections(user_id=...) ondersteunt
        report = generate_daily_report_sections(user_id=user_id)
        return {
            "status": "ok",
            "generated_at": datetime.utcnow().isoformat(),
            "user_id": user_id,
            "report": report,
        }
    except TypeError:
        # Backwards compat: als functie nog geen user_id kent
        report = generate_daily_report_sections()
        return {
            "status": "ok",
            "generated_at": datetime.utcnow().isoformat(),
            "user_id": user_id,
            "report": report,
        }
    except Exception as e:
        logger.exception("[/report/daily/generate] Fout:")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/report/daily/export/pdf")
async def export_daily_pdf(
    date: str = Query(...),
    current_user: dict = Depends(get_current_user)
):
    user_id = current_user["id"]
    try:
        parsed_date = datetime.strptime(date, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(status_code=400, detail="Ongeldig datumformaat. Gebruik YYYY-MM-DD.")

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT *
                FROM daily_reports
                WHERE report_date = %s AND user_id = %s
                LIMIT 1;
                """,
                (parsed_date, user_id)
            )
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Geen dagelijks rapport gevonden")
            cols = [desc[0] for desc in cur.description]
            report = dict(zip(cols, row))

            return export_pdf("daily", report, date, user_id)
    finally:
        conn.close()


# ======================================================
# 📆 WEEKLY REPORTS
# ======================================================

@router.get("/report/weekly/latest")
async def get_weekly_latest(current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT *
                FROM weekly_reports
                WHERE user_id = %s
                ORDER BY report_date DESC
                LIMIT 1;
                """,
                (user_id,)
            )
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Geen weekrapport gevonden")
            cols = [desc[0] for desc in cur.description]
            return dict(zip(cols, row))
    finally:
        conn.close()


@router.get("/report/weekly/by-date")
async def get_weekly_by_date(
    date: str = Query(...),
    current_user: dict = Depends(get_current_user)
):
    user_id = current_user["id"]
    try:
        parsed_date = datetime.strptime(date, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(status_code=400, detail="Ongeldig datumformaat. Gebruik YYYY-MM-DD.")

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT *
                FROM weekly_reports
                WHERE report_date = %s AND user_id = %s
                LIMIT 1;
                """,
                (parsed_date, user_id)
            )
            row = cur.fetchone()
            if not row:
                raise HTTPException(
                    status_code=404,
                    detail=f"Geen weekrapport gevonden voor {parsed_date}"
                )
            cols = [desc[0] for desc in cur.description]
            return dict(zip(cols, row))
    finally:
        conn.close()


@router.get("/report/weekly/history")
async def get_weekly_report_history(
    current_user: dict = Depends(get_current_user)
):
    logger.info("🚀 Weekly report history endpoint aangeroepen")
    user_id = current_user["id"]
    conn = get_db_connection()

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT report_date
                FROM weekly_reports
                WHERE user_id = %s
                ORDER BY report_date DESC
                LIMIT 10;
                """,
                (user_id,)
            )
            rows = cur.fetchall()
            logger.info(f"🧪 Gevonden {len(rows)} weekly reports voor user {user_id}: {rows}")
            if not rows:
                raise HTTPException(status_code=404, detail="Geen weekly reports gevonden.")
            return [row[0].isoformat() for row in rows]
    finally:
        conn.close()


@router.post("/report/weekly/generate")
async def generate_weekly(current_user: dict = Depends(get_current_user)):
    """
    Start Celery-task voor weekrapport — per user.
    """
    user_id = current_user["id"]
    try:
        task = generate_weekly_report.delay(user_id=user_id)
        return {"message": "Weekrapport taak gestart", "task_id": task.id, "user_id": user_id}
    except TypeError:
        # Backwards compat: taak kent nog geen user_id
        task = generate_weekly_report.delay()
        return {
            "message": "Weekrapport taak gestart (zonder user_id in task)",
            "task_id": task.id,
            "user_id": user_id,
        }


@router.get("/report/weekly/export/pdf")
async def export_weekly_pdf(
    date: str = Query(...),
    current_user: dict = Depends(get_current_user)
):
    user_id = current_user["id"]
    try:
        parsed_date = datetime.strptime(date, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(status_code=400, detail="Ongeldig datumformaat. Gebruik YYYY-MM-DD.")

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT *
                FROM weekly_reports
                WHERE report_date = %s AND user_id = %s
                LIMIT 1;
                """,
                (parsed_date, user_id)
            )
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Geen weekrapport gevonden")
            cols = [desc[0] for desc in cur.description]
            report = dict(zip(cols, row))
            return export_pdf("weekly", report, date, user_id)
    finally:
        conn.close()


# ======================================================
# 📅 MONTHLY REPORTS
# ======================================================

@router.get("/report/monthly/latest")
async def get_monthly_latest(current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT *
                FROM monthly_reports
                WHERE user_id = %s
                ORDER BY report_date DESC
                LIMIT 1;
                """,
                (user_id,)
            )
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Geen maandrapport gevonden")
            cols = [desc[0] for desc in cur.description]
            return dict(zip(cols, row))
    finally:
        conn.close()


@router.get("/report/monthly/by-date")
async def get_monthly_by_date(
    date: str = Query(...),
    current_user: dict = Depends(get_current_user)
):
    user_id = current_user["id"]
    try:
        parsed_date = datetime.strptime(date, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(status_code=400, detail="Ongeldig datumformaat. Gebruik YYYY-MM-DD.")

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT *
                FROM monthly_reports
                WHERE report_date = %s AND user_id = %s
                LIMIT 1;
                """,
                (parsed_date, user_id)
            )
            row = cur.fetchone()
            if not row:
                raise HTTPException(
                    status_code=404,
                    detail=f"Geen maandrapport gevonden voor {parsed_date}"
                )
            cols = [desc[0] for desc in cur.description]
            return dict(zip(cols, row))
    finally:
        conn.close()


@router.get("/report/monthly/history")
async def get_monthly_report_history(
    current_user: dict = Depends(get_current_user)
):
    logger.info("🚀 Monthly report history endpoint aangeroepen")
    user_id = current_user["id"]
    conn = get_db_connection()

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT report_date
                FROM monthly_reports
                WHERE user_id = %s
                ORDER BY report_date DESC
                LIMIT 10;
                """,
                (user_id,)
            )
            rows = cur.fetchall()
            logger.info(f"🧪 Gevonden {len(rows)} monthly reports voor user {user_id}: {rows}")
            if not rows:
                raise HTTPException(status_code=404, detail="Geen monthly reports gevonden.")
            return [row[0].isoformat() for row in rows]
    finally:
        conn.close()


@router.post("/report/monthly/generate")
async def generate_monthly(current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]
    try:
        task = generate_monthly_report.delay(user_id=user_id)
        return {"message": "Maandrapport taak gestart", "task_id": task.id, "user_id": user_id}
    except TypeError:
        task = generate_monthly_report.delay()
        return {
            "message": "Maandrapport taak gestart (zonder user_id in task)",
            "task_id": task.id,
            "user_id": user_id,
        }


@router.get("/report/monthly/export/pdf")
async def export_monthly_pdf(
    date: str = Query(...),
    current_user: dict = Depends(get_current_user)
):
    user_id = current_user["id"]
    try:
        parsed_date = datetime.strptime(date, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(status_code=400, detail="Ongeldig datumformaat. Gebruik YYYY-MM-DD.")

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT *
                FROM monthly_reports
                WHERE report_date = %s AND user_id = %s
                LIMIT 1;
                """,
                (parsed_date, user_id)
            )
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Geen maandrapport gevonden")
            cols = [desc[0] for desc in cur.description]
            report = dict(zip(cols, row))
            return export_pdf("monthly", report, date, user_id)
    finally:
        conn.close()


# ======================================================
# 📊 QUARTERLY REPORTS
# ======================================================

@router.get("/report/quarterly/latest")
async def get_quarterly_latest(current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT *
                FROM quarterly_reports
                WHERE user_id = %s
                ORDER BY report_date DESC
                LIMIT 1;
                """,
                (user_id,)
            )
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Geen kwartaalrapport gevonden")
            cols = [desc[0] for desc in cur.description]
            return dict(zip(cols, row))
    finally:
        conn.close()


@router.get("/report/quarterly/by-date")
async def get_quarterly_by_date(
    date: str = Query(...),
    current_user: dict = Depends(get_current_user)
):
    user_id = current_user["id"]
    try:
        parsed_date = datetime.strptime(date, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(status_code=400, detail="Ongeldig datumformaat. Gebruik YYYY-MM-DD.")

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT *
                FROM quarterly_reports
                WHERE report_date = %s AND user_id = %s
                LIMIT 1;
                """,
                (parsed_date, user_id)
            )
            row = cur.fetchone()
            if not row:
                raise HTTPException(
                    status_code=404,
                    detail=f"Geen kwartaalrapport gevonden voor {parsed_date}"
                )
            cols = [desc[0] for desc in cur.description]
            return dict(zip(cols, row))
    finally:
        conn.close()


@router.get("/report/quarterly/history")
async def get_quarterly_report_history(
    current_user: dict = Depends(get_current_user)
):
    logger.info("🚀 Quarterly report history endpoint aangeroepen")
    user_id = current_user["id"]
    conn = get_db_connection()

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT report_date
                FROM quarterly_reports
                WHERE user_id = %s
                ORDER BY report_date DESC
                LIMIT 10;
                """,
                (user_id,)
            )
            rows = cur.fetchall()
            logger.info(f"🧪 Gevonden {len(rows)} quarterly reports voor user {user_id}: {rows}")
            if not rows:
                raise HTTPException(status_code=404, detail="Geen quarterly reports gevonden.")
            return [row[0].isoformat() for row in rows]
    finally:
        conn.close()


@router.post("/report/quarterly/generate")
async def generate_quarterly(current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]
    try:
        task = generate_quarterly_report.delay(user_id=user_id)
        return {"message": "Kwartaalrapport taak gestart", "task_id": task.id, "user_id": user_id}
    except TypeError:
        task = generate_quarterly_report.delay()
        return {
            "message": "Kwartaalrapport taak gestart (zonder user_id in task)",
            "task_id": task.id,
            "user_id": user_id,
        }


@router.get("/report/quarterly/export/pdf")
async def export_quarterly_pdf(
    date: str = Query(...),
    current_user: dict = Depends(get_current_user)
):
    user_id = current_user["id"]
    try:
        parsed_date = datetime.strptime(date, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(status_code=400, detail="Ongeldig datumformaat. Gebruik YYYY-MM-DD.")

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT *
                FROM quarterly_reports
                WHERE report_date = %s AND user_id = %s
                LIMIT 1;
                """,
                (parsed_date, user_id)
            )
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Geen kwartaalrapport gevonden")
            cols = [desc[0] for desc in cur.description]
            report = dict(zip(cols, row))
            return export_pdf("quarterly", report, date, user_id)
    finally:
        conn.close()


print("📦 report_api routes:")
for route in router.routes:
    print(f"{route.path} - {route.methods}")
