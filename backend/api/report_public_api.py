from fastapi import APIRouter, HTTPException, Query
from datetime import datetime
import logging

from backend.utils.db import get_db_connection

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/public/report")
def get_public_report(token: str = Query(...)):
    """
    Public endpoint voor print & share.

    Wordt gebruikt door:
    - Playwright PDF rendering
    - Print view
    - Public sharing links
    """

    logger.info("🔓 Public report request | token=%s", token[:12])

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database unavailable")

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT report_json, valid_until, status
                FROM report_snapshots
                WHERE token = %s
                LIMIT 1;
                """,
                (token,),
            )

            row = cur.fetchone()

            if not row:
                logger.warning("❌ Snapshot not found")
                raise HTTPException(status_code=404, detail="Snapshot not found")

            report_json, valid_until, status = row

            # ⏰ Expired link
            if valid_until and valid_until < datetime.utcnow():
                logger.warning("⛔ Snapshot expired")
                raise HTTPException(status_code=410, detail="Link expired")

            # ⏳ Nog niet klaar
            if status != "ready":
                logger.warning("⏳ Snapshot not ready yet")
                raise HTTPException(status_code=425, detail="Report not ready")

            logger.info("✅ Public report delivered")

            return report_json

    except HTTPException:
        raise

    except Exception as e:
        logger.exception("❌ Public report crash")
        raise HTTPException(status_code=500, detail="Server error")

    finally:
        conn.close()
