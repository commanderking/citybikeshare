import os
from playwright.sync_api import sync_playwright
import scripts.utils as utils


def run_get_exports(playwright, url, zip_path, csv_path):
    browser = playwright.chromium.launch(headless=True)
    context = browser.new_context(accept_downloads=True)
    page = context.new_page()

    page.goto(url)
    page.get_by_text("Confirm").click()
    csv_buttons = page.locator('role=button[name="CSV"]')

    for button in csv_buttons.all():
        desired_filename = (
            button.get_attribute("content").split(".no/")[1].replace("/", "-")
        )
        target_folder = zip_path if desired_filename.endswith(".zip") else csv_path
        with page.expect_download(timeout=120000) as download_info:
            button.click()
            download = download_info.value
            print(f"Downloading {desired_filename}")
            download.save_as(os.path.join(target_folder, desired_filename))

    browser.close()


def get_exports(url, city):
    ZIP_PATH = utils.get_zip_directory(city)
    CSV_PATH = utils.get_raw_files_directory(city)

    with sync_playwright() as playwright:
        run_get_exports(playwright, url, ZIP_PATH, CSV_PATH)
