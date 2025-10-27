import os
import json
from playwright.sync_api import sync_playwright
import requests
from src.citybikeshare.etl.custom_downloaders.utils.norway_cities import (
    click_buttons_to_download,
)

from src.citybikeshare.utils.paths import (
    get_zip_directory,
    get_raw_files_directory,
    get_metadata_directory,
)


CITY = "oslo"

ZIP_PATH = get_zip_directory(CITY)
OPEN_DATA_URL = "https://oslobysykkel.no/en/open-data/historical"
CSV_PATH = get_raw_files_directory(CITY)
METADATA_PATH = get_metadata_directory(CITY)
CURRENT_STATIONS_URL = (
    "https://gbfs.urbansharing.com/oslobysykkel.no/station_information.json"
)

version_one_columns = {
    "started_at": "start_time",
    "ended_at": "end_time",
    "start_station_name": "start_station_name",
    "end_station_name": "end_station_name",
}

legacy_columns = {
    "Start station": "start_station_id",
    "End station": "end_station_id",
    "Start time": "start_time",
    "End time": "end_time",
}


def get_stations_information():
    response = requests.get(CURRENT_STATIONS_URL)

    # Check if the request was successful
    if response.status_code == 200:
        json_data = response.json()
        with open(METADATA_PATH / "station_information.json", "w") as json_file:
            json.dump(json_data, json_file, indent=4)
        print(f"Downloaded JSON from url: {CURRENT_STATIONS_URL}")
    else:
        print(f"Failed to retrieve JSON. Status code: {response.status_code}")


def get_file_size_from_url(url):
    response = requests.head(url, allow_redirects=True)
    if response.status_code == 200 and "Content-Length" in response.headers:
        return int(response.headers["Content-Length"])
    return None


def run_get_exports(playwright, url):
    browser = playwright.chromium.launch(headless=True)
    context = browser.new_context(accept_downloads=True)
    page = context.new_page()

    page.goto(url)
    csv_buttons = page.locator('role=button[name="CSV"]')

    # Download old to new station id mapping
    old_new_stations_button = page.get_by_text("Legacy/New station ID mapping")
    with page.expect_download(timeout=120000) as download_info:
        old_new_stations_button.click()
        download = download_info.value
        print(f"Downloading {download.suggested_filename}")
        download.save_as(os.path.join(METADATA_PATH, download.suggested_filename))

    click_buttons_to_download(page, csv_buttons, ZIP_PATH, CSV_PATH)
    browser.close()


def download(config):
    url = config.get("source_url")
    get_stations_information()
    with sync_playwright() as playwright:
        run_get_exports(playwright, url)
