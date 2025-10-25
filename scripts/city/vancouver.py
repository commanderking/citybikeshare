from playwright.sync_api import sync_playwright
import scripts.utils as utils
import scripts.utils_playwright as utils_playwright

OPEN_DATA_URL = "https://www.mobibikes.ca/en/system-data"
CSV_PATH = utils.get_raw_files_directory("vancouver")
date_formats = ["%Y-%m-%d %H:%M", "%m/%d/%Y %H:%M"]


def run_get_exports(playwright, url, csv_path):
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
            utils_playwright.download_if_new_data(
                download_info,
                csv_path,
            )
    browser.close()


def get_exports(url, csv_path):
    with sync_playwright() as playwright:
        run_get_exports(playwright, url, csv_path)


if __name__ == "__main__":
    get_exports(OPEN_DATA_URL, CSV_PATH)
