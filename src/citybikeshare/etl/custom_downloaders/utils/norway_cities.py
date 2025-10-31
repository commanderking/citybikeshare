from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from src.citybikeshare.etl.custom_downloaders.utils.download_helpers import (
    download_if_new_data,
)
from src.citybikeshare.utils.paths import (
    get_zip_directory,
)


def click_buttons_to_download(page, buttons, zip_path):
    for button in buttons.all():
        # links are formatted as: - to get unique names and not 06.csv for ever year, get text after .no/ domain
        ## New - https://data.urbansharing.com/oslobysykkel.no/trips/v1/2019/06.csv
        ## Legacy -  https://data-legacy.urbansharing.com/oslobysykkel.no/2018/06.csv.zip
        desired_filename = (
            button.get_attribute("content").split(".no/")[1].replace("/", "-")
        )
        try:
            with page.expect_download(timeout=5000) as download_info:
                button.click()
                download_if_new_data(
                    download_info, zip_path, desired_filename=desired_filename
                )
        # For Trondheim, certain months do not have trip data. Rather than not show these files, Trondheim has a link, but the link leads to nothing!
        except PlaywrightTimeoutError:
            print("No valid download found")


def run_get_exports(playwright, url, zip_path):
    browser = playwright.chromium.launch(headless=True)
    context = browser.new_context(accept_downloads=True)
    page = context.new_page()

    page.goto(url)
    page.get_by_text("Confirm").click()
    csv_buttons = page.locator('role=button[name="CSV"]')

    click_buttons_to_download(page, csv_buttons, zip_path)
    browser.close()


def download_files(url, city):
    zip_path = get_zip_directory(city)

    with sync_playwright() as playwright:
        run_get_exports(playwright, url, zip_path)
