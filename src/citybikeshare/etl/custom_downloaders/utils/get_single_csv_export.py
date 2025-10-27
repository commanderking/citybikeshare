from playwright.sync_api import sync_playwright


def run_get_exports(playwright, url, file_path):
    browser = playwright.chromium.launch(headless=True)
    context = browser.new_context(accept_downloads=True)
    page = context.new_page()

    page.goto(url)
    page.click("text=Export")

    print(f"navigated to {url}")

    with page.expect_download(timeout=120000) as download_info:
        # Click the "Download" button
        page.get_by_test_id("export-download-button").click()
    download = download_info.value
    download.save_as(file_path)
    print(f"Downloaded {download.suggested_filename}")


def get_exports(url, file_path):
    """Applies to Austin and Chattanooga so far"""
    with sync_playwright() as playwright:
        run_get_exports(playwright, url, file_path)
