import os
from playwright.sync_api import sync_playwright
from citybikeshare.context import PipelineContext
from citybikeshare.etl.custom_downloaders.utils.download_helpers import should_download


def run(playwright, url, context: PipelineContext):
    download_path = context.download_directory
    # Create a downloaded zip directory if it doesn't exist
    os.makedirs(download_path, exist_ok=True)

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
        target_file_path = os.path.join(download_path, filename)
        if should_download(target_file_path):
            with page.expect_download() as download_info:
                link.click()
            download = download_info.value
            download.save_as(os.path.join(download_path, filename))
            print(f"Downloaded {filename}")

    # Download stations csv directly into csv folder
    stations_csv_link = page.get_by_role("link", name="Station Table")
    with page.expect_download() as stations_download_info:
        stations_csv_link.click()
    stations_download = stations_download_info.value
    stations_download.save_as(os.path.join(download_path, "stations.csv"))
    print(f"Downloaded {stations_download.suggested_filename} as stations.csv")

    browser.close()


def download_files(config, context):
    url = config.get("source_url")
    with sync_playwright() as playwright:
        run(playwright, url, context)
