import os
import time
import hmac
import json
import base64
import hashlib
import logging
from typing import Optional

from playwright.async_api import async_playwright

logger = logging.getLogger(__name__)

# URL van je frontend (Next.js)
FRONTEND_BASE_URL = os.getenv("FRONTEND_BASE_URL", "http://143.47.186.148:3000")

# Secret voor signed tokens
PDF_TOKEN_SECRET = os.getenv("PDF_TOKEN_SECRET", "CHANGE_ME_PLEASE")

# timeout/waits
DEFAULT_TIMEOUT_MS = int(os.getenv("PDF_RENDER_TIMEOUT_MS", "45000"))


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("utf-8").rstrip("=")


def _b64url_decode(s: str) -> bytes:
    pad = "=" * (-len(s) % 4)
    return base64.urlsafe_b64decode((s + pad).encode("utf-8"))


def make_print_token(user_id: int, report_type: str, date_str: str, ttl_seconds: int = 120) -> str:
    """
    Eenvoudige HMAC-signed token:
    payload = {"uid":..,"t":..,"d":..,"exp":..}
    token = base64(payload) + "." + base64(sig)
    """
    payload = {
        "uid": int(user_id),
        "t": str(report_type),
        "d": str(date_str),
        "exp": int(time.time()) + int(ttl_seconds),
    }
    payload_bytes = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    payload_b64 = _b64url(payload_bytes)

    sig = hmac.new(PDF_TOKEN_SECRET.encode("utf-8"), payload_b64.encode("utf-8"), hashlib.sha256).digest()
    sig_b64 = _b64url(sig)

    return f"{payload_b64}.{sig_b64}"


def verify_print_token(token: str) -> Optional[dict]:
    try:
        payload_b64, sig_b64 = token.split(".", 1)
        expected = hmac.new(PDF_TOKEN_SECRET.encode("utf-8"), payload_b64.encode("utf-8"), hashlib.sha256).digest()
        if not hmac.compare_digest(_b64url(expected), sig_b64):
            return None

        payload = json.loads(_b64url_decode(payload_b64).decode("utf-8"))
        if int(payload.get("exp", 0)) < int(time.time()):
            return None
        return payload
    except Exception:
        return None


async def render_report_pdf_via_playwright(report_type: str, date_str: str, user_id: int) -> bytes:
    """
    Render de Next.js print pagina en maak PDF bytes.
    """
    token = make_print_token(user_id=user_id, report_type=report_type, date_str=date_str)
    url = f"{FRONTEND_BASE_URL}/print/{report_type}?date={date_str}&token={token}"

    logger.info("🧾 Playwright PDF render: %s", url)

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ],
        )

        context = await browser.new_context(
            viewport={"width": 1440, "height": 900},
            device_scale_factor=1,
        )

        page = await context.new_page()

        # ga naar pagina
        await page.goto(url, wait_until="networkidle", timeout=DEFAULT_TIMEOUT_MS)

        # wacht expliciet op "print-ready" marker
        await page.wait_for_selector('[data-print-ready="true"]', timeout=DEFAULT_TIMEOUT_MS)

        # PDF
        pdf_bytes = await page.pdf(
            format="A4",
            print_background=True,  # belangrijk: cards/kleuren
            margin={"top": "14mm", "right": "14mm", "bottom": "14mm", "left": "14mm"},
            prefer_css_page_size=True,
        )

        await context.close()
        await browser.close()

        logger.info("✅ Playwright PDF bytes klaar (%d bytes)", len(pdf_bytes))
        return pdf_bytes
