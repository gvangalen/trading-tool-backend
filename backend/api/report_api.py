print("🟢 report_api wordt geladen ✅")

import logging
from datetime import datetime
import os

from fastapi import APIRouter, HTTPException, Query, Depends
from fastapi.responses import StreamingResponse

import io
from backend.utils.pdf_playwright import render_report_pdf_via_playwright

from backend.utils.db import get_db_connection
from backend.ai_agents.report_ai_agent import generate_daily_report_sections
from backend.celery_task.daily_report_task import generate_daily_report
from backend.celery_task.weekly_report_task import generate_weekly_report
from backend.celery_task.monthly_report_task import generate_monthly_report
from backend.celery_task.quarterly_report_task import generate_quarterly_report
from backend.utils.auth_utils import get_current_user  # ✅ centrale user helper

router = APIRouter()
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


async def generate_pdf_response(
    *,
    table: str,
    report_type: str,
    date: str,
    user_id: int,
):
    conn = get_db_connection()

    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT 1
                FROM {table}
                WHERE report_date = %s
                AND user_id = %s
                LIMIT 1;
                """,
                (date, user_id),
            )

            if not cur.fetchone():
                raise HTTPException(status_code=404, detail="Report niet gevonden")

    finally:
        conn.close()

    pdf_bytes = await render_report_pdf_via_playwright(
        report_type=report_type,
        date_str=date,
        user_id=user_id,
    )

    filename = f"{report_type}_report_user_{user_id}_{date}.pdf"

    return StreamingResponse(
        io.BytesIO(pdf_bytes),
        media_type="application/pdf",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"'
        },
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
                (user_id,),
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
    current_user: dict = Depends(get_current_user),
):
    user_id = current_user["id"]

    try:
        parsed_date = datetime.strptime(date, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date")

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT *
                FROM daily_reports
                WHERE report_date = %s
                AND user_id = %s
                LIMIT 1;
                """,
                (parsed_date, user_id),
            )

            row = cur.fetchone()

            if not row:
                raise HTTPException(status_code=404, detail="Daily report niet gevonden")

            cols = [desc[0] for desc in cur.description]
            return dict(zip(cols, row))

    finally:
        conn.close()


@router.get("/report/daily/history")
async def get_daily_report_history(
    current_user: dict = Depends(get_current_user),
):
    """
    Geeft de laatste 10 daily-report datums terug voor de user.
    Bij geen data: gewoon [] (geen 404), zodat de frontend geen errors gooit.
    """
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
                (user_id,),
            )
            rows = cur.fetchall()
            logger.info(f"🧪 Gevonden {len(rows)} daily reports voor user {user_id}")
            return [r[0].isoformat() for r in rows]
    finally:
        conn.close()


@router.post("/report/daily/preview")
async def preview_daily_report(current_user: dict = Depends(get_current_user)):
    """
    Genereert een daily report *preview* via de AI-agent (zonder in DB op te slaan).
    Handig voor een snelle check in de UI.
    """
    user_id = current_user["id"]
    try:
        # Idealiter ondersteunt deze functie user_id
        try:
            report = generate_daily_report_sections(user_id=user_id)
        except TypeError:
            # Backwards compat als de functie nog geen user_id accepteert
            report = generate_daily_report_sections()

        return {
            "status": "ok",
            "generated_at": datetime.utcnow().isoformat(),
            "user_id": user_id,
            "report": report,
        }
    except Exception as e:
        logger.exception("[/report/daily/preview] Fout:")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/report/daily/generate")
async def generate_daily(current_user: dict = Depends(get_current_user)):
    """
    Start Celery-task voor het *echte* daily report voor deze user.
    Dit rapport wordt in de database opgeslagen.
    """
    user_id = current_user["id"]
    try:
        # Idealiter accepteert de Celery-task user_id
        try:
            task = generate_daily_report.delay(user_id=user_id)
        except TypeError:
            # Backwards compat zonder user_id in signature
            task = generate_daily_report.delay()

        return {
            "message": "Daily report taak gestart",
            "task_id": task.id,
            "user_id": user_id,
        }
    except Exception as e:
        logger.exception("[/report/daily/generate] Fout:")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/report/daily/export/pdf")
async def export_daily_pdf(
    date: str = Query(...),
    current_user: dict = Depends(get_current_user),
):
    user_id = current_user["id"]

    try:
        datetime.strptime(date, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date")

    return await generate_pdf_response(
        table="daily_reports",
        report_type="daily",
        date=date,
        user_id=user_id,
    )


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
                (user_id,),
            )
            row = cur.fetchone()

            if not row:
                return { "_status": "pending" }

            cols = [desc[0] for desc in cur.description]
            report = dict(zip(cols, row))

            return {
                "_status": "ready",
                **report,
            }

    finally:
        conn.close()

@router.get("/report/weekly/by-date")
async def get_weekly_by_date(
    date: str = Query(...),
    current_user: dict = Depends(get_current_user),
):
    user_id = current_user["id"]

    try:
        parsed_date = datetime.strptime(date, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date")

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT *
                FROM weekly_reports
                WHERE report_date = %s
                AND user_id = %s
                LIMIT 1;
                """,
                (parsed_date, user_id),
            )

            row = cur.fetchone()

            if not row:
                raise HTTPException(status_code=404, detail="Weekly report niet gevonden")

            cols = [desc[0] for desc in cur.description]
            return dict(zip(cols, row))

    finally:
        conn.close()


@router.get("/report/weekly/history")
async def get_weekly_report_history(
    current_user: dict = Depends(get_current_user),
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
                (user_id,),
            )
            rows = cur.fetchall()
            logger.info(f"🧪 Gevonden {len(rows)} weekly reports voor user {user_id}")
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
        try:
            task = generate_weekly_report.delay(user_id=user_id)
        except TypeError:
            task = generate_weekly_report.delay()

        return {
            "message": "Weekrapport taak gestart",
            "task_id": task.id,
            "user_id": user_id,
        }
    except Exception as e:
        logger.exception("[/report/weekly/generate] Fout:")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/report/weekly/export/pdf")
async def export_weekly_pdf(
    date: str = Query(...),
    current_user: dict = Depends(get_current_user),
):
    user_id = current_user["id"]

    try:
        datetime.strptime(date, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date")

    return await generate_pdf_response(
        table="weekly_reports",
        report_type="weekly",
        date=date,
        user_id=user_id,
    )


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
                (user_id,),
            )
            row = cur.fetchone()

            if not row:
                return { "_status": "pending" }

            cols = [desc[0] for desc in cur.description]
            report = dict(zip(cols, row))

            return {
                "_status": "ready",
                **report,
            }

    finally:
        conn.close()


@router.get("/report/monthly/by-date")
async def get_monthly_by_date(
    date: str = Query(...),
    current_user: dict = Depends(get_current_user),
):
    user_id = current_user["id"]

    try:
        parsed_date = datetime.strptime(date, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date")

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT *
                FROM monthly_reports
                WHERE report_date = %s
                AND user_id = %s
                LIMIT 1;
                """,
                (parsed_date, user_id),
            )

            row = cur.fetchone()

            if not row:
                raise HTTPException(status_code=404, detail="Monthly report niet gevonden")

            cols = [desc[0] for desc in cur.description]
            return dict(zip(cols, row))

    finally:
        conn.close()


@router.get("/report/monthly/history")
async def get_monthly_report_history(
    current_user: dict = Depends(get_current_user),
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
                (user_id,),
            )
            rows = cur.fetchall()
            logger.info(f"🧪 Gevonden {len(rows)} monthly reports voor user {user_id}")
            return [row[0].isoformat() for row in rows]
    finally:
        conn.close()


@router.post("/report/monthly/generate")
async def generate_monthly(current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]
    try:
        try:
            task = generate_monthly_report.delay(user_id=user_id)
        except TypeError:
            task = generate_monthly_report.delay()

        return {
            "message": "Maandrapport taak gestart",
            "task_id": task.id,
            "user_id": user_id,
        }
    except Exception as e:
        logger.exception("[/report/monthly/generate] Fout:")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/report/monthly/export/pdf")
async def export_monthly_pdf(
    date: str = Query(...),
    current_user: dict = Depends(get_current_user),
):
    user_id = current_user["id"]

    try:
        datetime.strptime(date, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date")

    return await generate_pdf_response(
        table="monthly_reports",
        report_type="monthly",
        date=date,
        user_id=user_id,
    )


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
                (user_id,),
            )
            row = cur.fetchone()

            if not row:
                return { "_status": "pending" }

            cols = [desc[0] for desc in cur.description]
            report = dict(zip(cols, row))

            return {
                "_status": "ready",
                **report,
            }

    finally:
        conn.close()

@router.get("/report/quarterly/by-date")
async def get_quarterly_by_date(
    date: str = Query(...),
    current_user: dict = Depends(get_current_user),
):
    user_id = current_user["id"]

    try:
        parsed_date = datetime.strptime(date, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date")

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT *
                FROM quarterly_reports
                WHERE report_date = %s
                AND user_id = %s
                LIMIT 1;
                """,
                (parsed_date, user_id),
            )

            row = cur.fetchone()

            if not row:
                raise HTTPException(status_code=404, detail="Quarterly report niet gevonden")

            cols = [desc[0] for desc in cur.description]
            return dict(zip(cols, row))

    finally:
        conn.close()

@router.get("/report/quarterly/history")
async def get_quarterly_report_history(
    current_user: dict = Depends(get_current_user),
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
                (user_id,),
            )
            rows = cur.fetchall()
            logger.info(f"🧪 Gevonden {len(rows)} quarterly reports voor user {user_id}")
            return [row[0].isoformat() for row in rows]
    finally:
        conn.close()


@router.post("/report/quarterly/generate")
async def generate_quarterly(current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]
    try:
        try:
            task = generate_quarterly_report.delay(user_id=user_id)
        except TypeError:
            task = generate_quarterly_report.delay()

        return {
            "message": "Kwartaalrapport taak gestart",
            "task_id": task.id,
            "user_id": user_id,
        }
    except Exception as e:
        logger.exception("[/report/quarterly/generate] Fout:")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/report/quarterly/export/pdf")
async def export_quarterly_pdf(
    date: str = Query(...),
    current_user: dict = Depends(get_current_user),
):
    user_id = current_user["id"]

    try:
        datetime.strptime(date, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date")

    return await generate_pdf_response(
        table="quarterly_reports",
        report_type="quarterly",
        date=date,
        user_id=user_id,
    )


print("📦 report_api routes:")
for route in router.routes:
    print(f"{route.path} - {route.methods}")
