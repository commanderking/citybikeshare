import os
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import scripts.utils as utils
import scripts.utils_playwright as utils_playwright


def click_buttons_to_download(page, buttons, zip_path, csv_path):
    for button in buttons.all():
        # links are formatted as: - to get unique names and not 06.csv for ever year, get text after .no/ domain
        ## New - https://data.urbansharing.com/oslobysykkel.no/trips/v1/2019/06.csv
        ## Legacy -  https://data-legacy.urbansharing.com/oslobysykkel.no/2018/06.csv.zip
        desired_filename = (
            button.get_attribute("content").split(".no/")[1].replace("/", "-")
        )
        target_folder = zip_path if desired_filename.endswith(".zip") else csv_path
        try:
            with page.expect_download(timeout=5000) as download_info:
                button.click()
                utils_playwright.download_if_new_data(
                    download_info, target_folder, desired_filename=desired_filename
                )
        # For Trondheim, certain months do not have trip data. Rather than not show these files, Trondheim has a link, but the link leads to nothing!
        except PlaywrightTimeoutError:
            print("No valid download found")


def run_get_exports(playwright, url, zip_path, csv_path):
    browser = playwright.chromium.launch(headless=True)
    context = browser.new_context(accept_downloads=True)
    page = context.new_page()

    page.goto(url)
    page.get_by_text("Confirm").click()
    csv_buttons = page.locator('role=button[name="CSV"]')

    click_buttons_to_download(page, csv_buttons, zip_path, csv_path)
    browser.close()


def get_exports(url, city):
    ZIP_PATH = utils.get_zip_directory(city)
    CSV_PATH = utils.get_raw_files_directory(city)

    with sync_playwright() as playwright:
        run_get_exports(playwright, url, ZIP_PATH, CSV_PATH)


date_formats = ["%Y-%m-%d %H:%M:%S%.f%:z", "%Y-%m-%d %H:%M:%S%:z"]
date_columns = ["start_time", "end_time"]
final_columns = ["start_station_name", "end_station_name", *date_columns]

norway_renamed_columns = {
    "started_at": "start_time",
    "ended_at": "end_time",
    "start_station_name": "start_station_name",
    "end_station_name": "end_station_name",
}
