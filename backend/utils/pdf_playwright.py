import os
import time
import hmac
import json
import base64
import hashlib
import logging
from typing import Optional

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout

logger = logging.getLogger(__name__)

# =========================================================
# CONFIG
# =========================================================

FRONTEND_BASE_URL = os.getenv(
    "FRONTEND_BASE_URL",
    "http://143.47.186.148:3000"
)

PDF_TOKEN_SECRET = os.getenv(
    "PDF_TOKEN_SECRET",
    "CHANGE_ME_PLEASE"
)

DEFAULT_TIMEOUT_MS = int(
    os.getenv("PDF_RENDER_TIMEOUT_MS", "60000")  # iets ruimer
)

# =========================================================
# BASE64 HELPERS
# =========================================================

def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("utf-8").rstrip("=")


def _b64url_decode(s: str) -> bytes:
    pad = "=" * (-len(s) % 4)
    return base64.urlsafe_b64decode((s + pad).encode("utf-8"))

# =========================================================
# TOKEN
# =========================================================

def make_print_token(
    user_id: int,
    report_type: str,
    date_str: str,
    ttl_seconds: int = 180,
) -> str:
    """
    Signed token zonder JWT overhead.
    Sneller + minder deps.
    """

    payload = {
        "uid": int(user_id),
        "t": str(report_type),
        "d": str(date_str),
        "exp": int(time.time()) + ttl_seconds,
    }

    payload_bytes = json.dumps(
        payload,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")

    payload_b64 = _b64url(payload_bytes)

    sig = hmac.new(
        PDF_TOKEN_SECRET.encode("utf-8"),
        payload_b64.encode("utf-8"),
        hashlib.sha256,
    ).digest()

    sig_b64 = _b64url(sig)

    return f"{payload_b64}.{sig_b64}"


def verify_print_token(token: str) -> Optional[dict]:
    """
    Gebruik deze in je PUBLIC endpoint.
    """

    try:
        payload_b64, sig_b64 = token.split(".", 1)

        expected = hmac.new(
            PDF_TOKEN_SECRET.encode("utf-8"),
            payload_b64.encode("utf-8"),
            hashlib.sha256,
        ).digest()

        if not hmac.compare_digest(_b64url(expected), sig_b64):
            return None

        payload = json.loads(
            _b64url_decode(payload_b64).decode("utf-8")
        )

        if int(payload.get("exp", 0)) < int(time.time()):
            return None

        return payload

    except Exception:
        return None


# =========================================================
# 🔥 MAIN RENDERER (PRO)
# =========================================================

async def render_report_pdf_via_playwright(
    report_type: str,
    date_str: str,
    user_id: int,
) -> bytes:

    token = make_print_token(
        user_id=user_id,
        report_type=report_type,
        date_str=date_str,
    )

    # 🔥 LET OP — GEEN date param meer nodig
    url = f"{FRONTEND_BASE_URL}/print/{report_type}?token={token}"

    logger.info("🧾 Playwright PDF render: %s", url)

    try:

        async with async_playwright() as p:

            browser = await p.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-gpu",
                ],
            )

            context = await browser.new_context(
                viewport={"width": 1280, "height": 900},
                device_scale_factor=2,  # 🔥 retina -> super sharp charts
            )

            page = await context.new_page()

            # sneller dan networkidle vaak
            await page.goto(
                url,
                wait_until="domcontentloaded",
                timeout=DEFAULT_TIMEOUT_MS,
            )

            # 🔥 wacht op print-ready marker
            await page.wait_for_selector(
                '[data-print-ready="true"]',
                timeout=DEFAULT_TIMEOUT_MS,
            )

            # kleine stabilisatie
            await page.wait_for_timeout(400)

            pdf_bytes = await page.pdf(
                format="A4",
                print_background=True,
                prefer_css_page_size=True,
                margin={
                    "top": "12mm",
                    "right": "12mm",
                    "bottom": "12mm",
                    "left": "12mm",
                },
            )

            await context.close()
            await browser.close()

            logger.info(
                "✅ Playwright PDF klaar (%d bytes)",
                len(pdf_bytes),
            )

            return pdf_bytes

    except PlaywrightTimeout:

        logger.exception("❌ PDF render timeout")
        raise Exception("PDF render timeout — check print-ready marker")

    except Exception as e:

        logger.exception("❌ Playwright crash")
        raise Exception(f"PDF render error: {str(e)}")
