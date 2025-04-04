import os
import requests
import scripts.utils as utils
from playwright.sync_api import sync_playwright


def run(playwright, url, city):
    STATIONS_CSV_PATH = utils.get_raw_files_directory(city)
    DOWNLOAD_PATH = utils.get_zip_directory(city)
    # Create a downloaded zip directory if it doesn't exist
    os.makedirs(DOWNLOAD_PATH, exist_ok=True)

    browser = playwright.chromium.launch(headless=True)
    context = browser.new_context(accept_downloads=True)
    page = context.new_page()
    page.goto(url)

    # Find all .zip file links and download them
    zip_links = page.query_selector_all('a[href$=".zip"]')
    for link in zip_links:
        with page.expect_download() as download_info:
            link.click()
        download = download_info.value
        download.save_as(os.path.join(DOWNLOAD_PATH, download.suggested_filename))
        print(f"Downloaded { download.suggested_filename }")

    # Download stations csv directly into csv folder
    stations_csv_link = page.get_by_role("link", name="Station Table")
    with page.expect_download() as stations_download_info:
        stations_csv_link.click()
    stations_download = stations_download_info.value
    stations_download.save_as(os.path.join(STATIONS_CSV_PATH, "stations.csv"))
    print(f"Downloaded { stations_download.suggested_filename } as stations.csv")

    browser.close()


def get_file_size_from_url(url):
    response = requests.head(url, allow_redirects=True)
    if response.status_code == 200 and "Content-Length" in response.headers:
        return int(response.headers["Content-Length"])
    return None


def download_if_new_data(download_info, target_folder, **kwargs):
    download = download_info.value
    desired_filename = kwargs.get("desired_filename", download.suggested_filename)

    file_size = get_file_size_from_url(download.url)
    file_exists = utils.does_file_exist(desired_filename, file_size, target_folder)
    if not file_exists:
        print(f"Downloading {desired_filename}")
        download.save_as(os.path.join(target_folder, desired_filename))
    else:
        print(f"{desired_filename} already downloaded")


def get_bicycle_transit_systems_zips(url, city):
    with sync_playwright() as playwright:
        run(playwright, url, city)


def run_get_exports(playwright, url, file_path):
    browser = playwright.chromium.launch(headless=True)
    context = browser.new_context(accept_downloads=True)
    page = context.new_page()

    page.goto(url)
    page.click("text=Export")

    print(f"navigated to {url}")

    with page.expect_download(timeout=120000) as download_info:
        # Click the "Download" button
        page.click("button:has-text('Download')")
    download = download_info.value
    download.save_as(file_path)
    print(f"Downloaded {download.suggested_filename}")


def get_exports(url, file_path):
    """Applies to Austin and Chattanooga so far"""
    with sync_playwright() as playwright:
        run_get_exports(playwright, url, file_path)
