import secrets
from datetime import datetime, timedelta
from backend.utils.db import get_db_connection


def create_report_snapshot(
    user_id: int,
    report_type: str,
    report_id: int,
    report_json: dict,
):
    conn = get_db_connection()

    token = secrets.token_urlsafe(32)

    valid_until = datetime.utcnow() + timedelta(days=7)

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO report_snapshots
            (user_id, report_type, report_id, token, report_json, valid_until, status)
            VALUES (%s,%s,%s,%s,%s,%s,'pending')
            RETURNING id, token
            """,
            (
                user_id,
                report_type,
                report_id,
                token,
                json.dumps(report_json),
                valid_until,
            ),
        )

        snapshot_id, token = cur.fetchone()

    conn.commit()
    conn.close()

    return snapshot_id, token
