from fastapi import APIRouter, HTTPException, Query
from backend.utils.db import get_db_connection
from backend.utils.pdf_token import verify_pdf_token

router = APIRouter()


@router.get("/public/report")
def get_public_report(token: str = Query(...)):

    try:
        payload = verify_pdf_token(token)
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token")

    user_id = payload["uid"]
    report_type = payload["t"]
    date = payload["d"]

    table = f"{report_type}_reports"

    conn = get_db_connection()

    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT *
                FROM {table}
                WHERE report_date = %s
                AND user_id = %s
                LIMIT 1;
                """,
                (date, user_id),
            )

            row = cur.fetchone()

            if not row:
                raise HTTPException(status_code=404, detail="Report not found")

            cols = [d[0] for d in cur.description]

            return dict(zip(cols, row))

    finally:
        conn.close()
