from fastapi import APIRouter, HTTPException, Query
from backend.utils.db import get_db_connection

router = APIRouter()


@router.get("/public/report")
def get_public_report(token: str = Query(...)):

    conn = get_db_connection()

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
                raise HTTPException(status_code=404, detail="Snapshot not found")

            report_json, valid_until, status = row

            # 🔴 Expired link
            if valid_until and valid_until < datetime.utcnow():
                raise HTTPException(status_code=410, detail="Link expired")

            # optional — maar ik raad het aan
            if status != "ready":
                raise HTTPException(status_code=425, detail="PDF not ready")

            return report_json

    finally:
        conn.close()
