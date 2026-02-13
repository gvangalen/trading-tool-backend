import os
import time
import logging
from pathlib import Path

from celery import shared_task
from playwright.sync_api import sync_playwright

from backend.utils.db import get_db_connection

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# =========================================================
# CONFIG
# =========================================================

FRONTEND_URL = os.getenv("FRONTEND_URL", "https://your-domain.com")

PDF_DIR = os.getenv(
    "PDF_OUTPUT_DIR",
    "/var/reports",   # <-- production path (bijv mounted volume / S3 sync)
)


# =========================================================
# Helpers
# =========================================================

def _ensure_pdf_dir():
    Path(PDF_DIR).mkdir(parents=True, exist_ok=True)


def _build_print_url(token: str) -> str:
    """
    MUST match your Next route:

    app/(print)/daily-report/page.tsx
    """

    return f"{FRONTEND_URL}/daily-report?token={token}"


def _update_snapshot_status(
    conn,
    snapshot_id: int,
    *,
    status: str,
    pdf_url: str | None = None,
    file_size: int | None = None,
    generation_ms: int | None = None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE report_snapshots
            SET
                status = %s,
                pdf_generated = %s,
                pdf_url = COALESCE(%s, pdf_url),
                file_size = COALESCE(%s, file_size),
                generation_ms = COALESCE(%s, generation_ms)
            WHERE id = %s
            """,
            (
                status,
                status == "ready",
                pdf_url,
                file_size,
                generation_ms,
                snapshot_id,
            ),
        )


# =========================================================
# 🎯 MAIN TASK
# =========================================================

@shared_task(
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_kwargs={"max_retries": 3},
)
def generate_report_pdf(self, snapshot_id: int):
    """
    Generates a PDF from the Playwright print route.

    Flow:

    DB -> token
       -> open Playwright
       -> wait for print marker
       -> save PDF
       -> update DB
    """

    conn = get_db_connection()
    if not conn:
        raise RuntimeError("DB unavailable")

    start_time = time.time()

    try:

        # =====================================================
        # Load snapshot
        # =====================================================

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT token
                FROM report_snapshots
                WHERE id = %s
                LIMIT 1
                """,
                (snapshot_id,),
            )
            row = cur.fetchone()

        if not row:
            raise RuntimeError(f"Snapshot {snapshot_id} not found")

        token = row[0]

        logger.info("📄 Generating PDF for snapshot %s", snapshot_id)

        _update_snapshot_status(conn, snapshot_id, status="generating")
        conn.commit()

        # =====================================================
        # Prepare path
        # =====================================================

        _ensure_pdf_dir()

        filename = f"report_{snapshot_id}.pdf"
        filepath = os.path.join(PDF_DIR, filename)

        url = _build_print_url(token)

        # =====================================================
        # PLAYWRIGHT
        # =====================================================

        with sync_playwright() as p:

            browser = p.chromium.launch(
                headless=True,
                args=["--no-sandbox"],
            )

            context = browser.new_context()

            page = context.new_page()

            logger.info("➡️ Opening print URL: %s", url)

            page.goto(
                url,
                wait_until="networkidle",
                timeout=60_000,
            )

            # 🔥 WAIT FOR PRINT READY MARKER
            page.wait_for_selector(
                "[data-print-ready='true']",
                timeout=60_000,
            )

            # give charts/fonts a breath
            page.wait_for_timeout(800)

            # =====================================================
            # EXPORT PDF
            # =====================================================

            page.pdf(
                path=filepath,
                format="A4",
                print_background=True,
                margin={
                    "top": "10mm",
                    "bottom": "10mm",
                    "left": "10mm",
                    "right": "10mm",
                },
            )

            browser.close()

        # =====================================================
        # Stats
        # =====================================================

        file_size = os.path.getsize(filepath)
        generation_ms = int((time.time() - start_time) * 1000)

        pdf_url = f"/reports/{filename}"  # adjust if using CDN/S3

        _update_snapshot_status(
            conn,
            snapshot_id,
            status="ready",
            pdf_url=pdf_url,
            file_size=file_size,
            generation_ms=generation_ms,
        )

        conn.commit()

        logger.info(
            "✅ PDF generated | snapshot=%s | size=%.2f KB | time=%sms",
            snapshot_id,
            file_size / 1024,
            generation_ms,
        )

        return {
            "snapshot_id": snapshot_id,
            "pdf_url": pdf_url,
        }

    except Exception as e:

        conn.rollback()

        logger.exception("❌ PDF generation failed")

        _update_snapshot_status(conn, snapshot_id, status="failed")
        conn.commit()

        raise self.retry(exc=e)

    finally:
        conn.close()
