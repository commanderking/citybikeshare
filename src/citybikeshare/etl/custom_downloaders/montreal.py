import os
import shutil
from playwright.sync_api import sync_playwright
import scripts.utils as utils

ZIP_PATH = utils.get_zip_directory("montreal")
OPEN_DATA_URL = "https://bixi.com/en/open-data/"


def run_get_exports(playwright, url, csv_path):
    browser = playwright.chromium.launch(headless=True)
    context = browser.new_context(accept_downloads=True)
    page = context.new_page()
    page.goto(url)
    page.locator("#onetrust-accept-btn-handler").click()
    links = page.query_selector_all("a")
    for link in links:
        href = link.get_attribute("href")
        if href and ".zip" in href:
            print(f"Attempting Download of {href}")
            with page.expect_download(timeout=120000) as download_info:
                link.click()
            download = download_info.value
            download.save_as(os.path.join(csv_path, download.suggested_filename))
            print(f"✅ Downloaded {os.path.basename(download.suggested_filename)}")
    browser.close()


def download(config):
    """Standard entrypoint for ETL to call."""
    zip_path = utils.get_zip_directory(config["name"])
    url = config["source_url"]
    with sync_playwright() as playwright:
        shutil.rmtree(zip_path, ignore_errors=True)
        os.makedirs(zip_path, exist_ok=True)
        run_get_exports(playwright, url, zip_path)
