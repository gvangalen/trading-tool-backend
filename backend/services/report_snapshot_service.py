import json
import secrets
import logging
from datetime import datetime, timedelta

from backend.utils.db import get_db_connection
from backend.celery_tasks.celery_task_generate_pdf import generate_report_pdf

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# =========================================================
# SNAPSHOT CREATOR
# =========================================================

def create_report_snapshot(
    user_id: int,
    report_type: str,
    report_id: int,
    report_json: dict,
):
    """
    Creates a snapshot AND triggers PDF generation.

    Guarantees:

    ✔ token security
    ✔ DB safety
    ✔ Celery trigger
    ✔ rollback on failure
    ✔ no silent crashes
    """

    conn = get_db_connection()
    if not conn:
        raise RuntimeError("DB unavailable")

    token = secrets.token_urlsafe(32)
    valid_until = datetime.utcnow() + timedelta(days=7)

    try:

        with conn.cursor() as cur:

            # -------------------------------------------------
            # Optional: prevent duplicate snapshots
            # (VERY recommended)
            # -------------------------------------------------

            cur.execute(
                """
                SELECT id
                FROM report_snapshots
                WHERE user_id=%s
                  AND report_type=%s
                  AND report_id=%s
                LIMIT 1
                """,
                (user_id, report_type, report_id),
            )

            existing = cur.fetchone()

            if existing:
                snapshot_id = existing[0]

                logger.info(
                    "Snapshot already exists → reusing id=%s",
                    snapshot_id,
                )

                return snapshot_id, None

            # -------------------------------------------------
            # Insert snapshot
            # -------------------------------------------------

            cur.execute(
                """
                INSERT INTO report_snapshots
                (
                    user_id,
                    report_type,
                    report_id,
                    token,
                    report_json,
                    valid_until,
                    status
                )
                VALUES (%s,%s,%s,%s,%s,%s,'pending')
                RETURNING id
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

            snapshot_id = cur.fetchone()[0]

        conn.commit()

        # -------------------------------------------------
        # Trigger PDF generation (async)
        # -------------------------------------------------

        generate_report_pdf.delay(snapshot_id)

        logger.info(
            "📄 Snapshot created → id=%s | PDF task queued",
            snapshot_id,
        )

        return snapshot_id, token

    except Exception:

        conn.rollback()
        logger.exception("❌ Failed to create report snapshot")
        raise

    finally:
        conn.close()
