import os
import shutil
import polars as pl
from playwright.sync_api import sync_playwright
import scripts.utils as utils

ZIP_PATH = utils.get_zip_directory("montreal")
OPEN_DATA_URL = "https://bixi.com/en/open-data/"
MONTREAL_CSV_PATHS = utils.get_raw_files_directory("montreal")
date_formats = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"]


def run_get_exports(playwright, url, csv_path):
    browser = playwright.chromium.launch(headless=True)
    context = browser.new_context(accept_downloads=True)
    page = context.new_page()

    page.goto(url)

    links = page.query_selector_all("a")

    page.locator("#onetrust-accept-btn-handler").click()

    for link in links:
        href = link.get_attribute("href")
        if href and ".zip" in href:
            print(f"Clicking on link with href: {href}")
            link.click()
            with page.expect_download(timeout=120000) as download_info:
                link.click()
            download = download_info.value
            download.save_as(os.path.join(csv_path, download.suggested_filename))

    browser.close()


def get_exports(url, zip_path):
    with sync_playwright() as playwright:
        # Clear out folder - Montreal changes filename month to month as more data is added
        shutil.rmtree(zip_path, ignore_errors=True)
        os.makedirs(zip_path, exist_ok=True)
        run_get_exports(playwright, url, zip_path)


if __name__ == "__main__":
    get_exports(OPEN_DATA_URL, ZIP_PATH)
