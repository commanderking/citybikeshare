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
        # Skip download if already in folder
        url = link.get_attribute("href")
        filename = os.path.basename(url)
        target_file_path = os.path.join(DOWNLOAD_PATH, filename)
        if os.path.exists(target_file_path):
            print(f"🟡 Skipping Download - ${filename} file already exists")

        else:
            with page.expect_download() as download_info:
                link.click()
            download = download_info.value
            download.save_as(os.path.join(DOWNLOAD_PATH, filename))
            print(f"Downloaded {filename}")

    # Download stations csv directly into csv folder
    stations_csv_link = page.get_by_role("link", name="Station Table")
    with page.expect_download() as stations_download_info:
        stations_csv_link.click()
    stations_download = stations_download_info.value
    stations_download.save_as(os.path.join(STATIONS_CSV_PATH, "stations.csv"))
    print(f"Downloaded {stations_download.suggested_filename} as stations.csv")

    browser.close()


def get_file_size_from_url(url):
    response = requests.head(url, allow_redirects=True)
    if response.status_code == 200 and "Content-Length" in response.headers:
        return int(response.headers["Content-Length"])
    return None


def download_files(config):
    city = config.get("name")
    url = config.get("source_url")
    with sync_playwright() as playwright:
        run(playwright, url, city)
