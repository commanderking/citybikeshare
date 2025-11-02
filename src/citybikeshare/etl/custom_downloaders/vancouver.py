from playwright.sync_api import sync_playwright
from src.citybikeshare.etl.custom_downloaders.utils.download_helpers import (
    download_if_new_data,
)
from src.citybikeshare.context import PipelineContext


def run_get_exports(playwright, url, context: PipelineContext):
    download_path = context.download_directory
    browser = playwright.chromium.launch(headless=True)
    context = browser.new_context(accept_downloads=True)
    page = context.new_page()

    page.goto(url)

    links = page.query_selector_all("a")
    page.locator("#axeptio_btn_dismiss").click()

    for link in links:
        href = link.get_attribute("href")
        if href and "drive.google.com" in href:
            # Create a new page manually
            new_page = context.new_page()
            new_page.goto(href)
            new_page.wait_for_load_state()

            with new_page.expect_download() as download_info:
                # Perform the action that initiates download
                new_page.get_by_label("Download", exact=True).click()
            download_if_new_data(
                download_info,
                download_path,
            )
    browser.close()


def download(config, context: PipelineContext):
    url = config.get("source_url")
    with sync_playwright() as playwright:
        run_get_exports(playwright, url, context)
