from playwright.sync_api import sync_playwright
from citybikeshare.context import PipelineContext
from citybikeshare.etl.custom_downloaders.utils.browser import launch_chromium


def run_get_exports(playwright, url, context: PipelineContext):
    download_path = context.download_directory

    target_file_name = "austin_all_trips.csv"
    file_path = download_path / target_file_name
    browser = launch_chromium(playwright)
    browser_context = browser.new_context(accept_downloads=True)
    page = browser_context.new_page()

    page.goto(url)
    page.click("text=Export")

    print(f"navigated to {url}")

    with page.expect_download(timeout=120000) as download_info:
        # Click the "Download" button
        page.get_by_test_id("export-download-button").click()
    download = download_info.value
    download.save_as(file_path)
    print(f"Downloaded {download.suggested_filename}")


def get_exports(url, context: PipelineContext):
    """Applies to Austin and Chattanooga so far"""
    with sync_playwright() as playwright:
        run_get_exports(playwright, url, context)
