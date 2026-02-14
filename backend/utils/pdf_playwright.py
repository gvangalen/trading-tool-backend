async def render_report_pdf_via_playwright(token: str) -> bytes:
    """
    Render report PDF via frontend print route.
    Token komt uit report_snapshots tabel.
    """

    url = f"{FRONTEND_BASE_URL}/print/daily?token={token}"

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
                device_scale_factor=2,
            )

            page = await context.new_page()

            await page.goto(
                url,
                wait_until="domcontentloaded",
                timeout=DEFAULT_TIMEOUT_MS,
            )

            # wacht tot print klaar
            await page.wait_for_selector(
                '[data-print-ready="true"]',
                timeout=DEFAULT_TIMEOUT_MS,
            )

            await page.wait_for_timeout(500)

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

            await browser.close()

            logger.info("✅ PDF render OK (%d bytes)", len(pdf_bytes))
            return pdf_bytes

    except PlaywrightTimeout:
        logger.exception("❌ PDF render timeout")
        raise Exception("PDF render timeout — print marker ontbreekt")

    except Exception as e:
        logger.exception("❌ Playwright crash")
        raise Exception(f"PDF render error: {str(e)}")
