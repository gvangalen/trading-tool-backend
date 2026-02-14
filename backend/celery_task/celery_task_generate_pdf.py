import os
import time
import logging
from pathlib import Path

from celery import shared_task
from playwright.sync_api import sync_playwright, TimeoutError
from backend.utils.db import get_db_connection

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# =========================================================
# CONFIG
# =========================================================

FRONTEND_URL = os.getenv("FRONTEND_URL")
PDF_DIR = os.getenv("PDF_OUTPUT_DIR", "/var/reports")

PRINT_TIMEOUT = 90_000  # ms

# =========================================================
# Helpers
# =========================================================

def _ensure_pdf_dir():
    Path(PDF_DIR).mkdir(parents=True, exist_ok=True)


def _build_print_url(token: str) -> str:
    return f"{FRONTEND_URL}/print/daily?token={token}"


def _update_snapshot_status(conn, snapshot_id, *, status,
                            pdf_url=None, file_size=None, generation_ms=None):
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
            (status, status == "ready", pdf_url, file_size, generation_ms, snapshot_id),
        )

# =========================================================
# MAIN TASK
# =========================================================

@shared_task(bind=True, autoretry_for=(Exception,), retry_backoff=10,
             retry_kwargs={"max_retries": 4}, acks_late=True)
def generate_report_pdf(self, snapshot_id: int):

    if not FRONTEND_URL:
        raise RuntimeError("FRONTEND_URL not configured")

    logger.info("🌐 FRONTEND_URL = %s", FRONTEND_URL)

    conn = get_db_connection()
    if not conn:
        raise RuntimeError("DB unavailable")

    start_time = time.time()

    try:
        # Load snapshot
        with conn.cursor() as cur:
            cur.execute(
                "SELECT token FROM report_snapshots WHERE id=%s LIMIT 1",
                (snapshot_id,),
            )
            row = cur.fetchone()

        if not row:
            raise RuntimeError(f"Snapshot {snapshot_id} not found")

        token = row[0]
        logger.info("📄 Generating PDF | snapshot=%s", snapshot_id)

        _update_snapshot_status(conn, snapshot_id, status="generating")
        conn.commit()

        _ensure_pdf_dir()
        filename = f"report_{snapshot_id}.pdf"
        filepath = os.path.join(PDF_DIR, filename)

        url = _build_print_url(token)
        logger.info("➡️ Opening print URL: %s", url)

        # =====================================================
        # PLAYWRIGHT
        # =====================================================

        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-gpu",
                ],
            )

            try:
                context = browser.new_context(
                    viewport={"width": 1280, "height": 900},
                    device_scale_factor=2,
                )

                page = context.new_page()

                page.goto(
                    url,
                    wait_until="domcontentloaded",
                    timeout=PRINT_TIMEOUT,
                )

                logger.info("⏳ Waiting for print-ready marker…")

                page.wait_for_selector(
                    "[data-print-ready='true']",
                    timeout=PRINT_TIMEOUT,
                )

                # stabilisatie
                page.wait_for_timeout(800)

                page.pdf(
                    path=filepath,
                    format="A4",
                    print_background=True,
                    prefer_css_page_size=True,
                )

            except TimeoutError:
                logger.error("❌ PRINT MARKER NOT FOUND")
                logger.error("Current URL: %s", page.url)
                raise

            finally:
                browser.close()

        # =====================================================
        # Stats
        # =====================================================

        file_size = os.path.getsize(filepath)
        generation_ms = int((time.time() - start_time) * 1000)
        pdf_url = f"/reports/{filename}"

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
            "✅ PDF generated | snapshot=%s | %.1fKB | %sms",
            snapshot_id,
            file_size / 1024,
            generation_ms,
        )

        return {"snapshot_id": snapshot_id, "pdf_url": pdf_url}

    except Exception as e:
        conn.rollback()
        logger.exception("❌ PDF generation failed")

        _update_snapshot_status(conn, snapshot_id, status="failed")
        conn.commit()

        raise self.retry(exc=e)

    finally:
        conn.close()
