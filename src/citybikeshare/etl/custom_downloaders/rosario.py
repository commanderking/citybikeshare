import os
from playwright.sync_api import sync_playwright
from src.citybikeshare.utils.paths import get_zip_directory, get_raw_files_directory


def run(playwright, config):
    browser = playwright.chromium.launch(headless=True)  # Set to True for silent mode
    context = browser.new_context(accept_downloads=True)
    page = context.new_page()

    url = "https://datosabiertos.rosario.gob.ar/dataset/0e487f13-7725-4bbf-afea-52e429fa92e5"
    page.goto(url)

    city = config.get("name")
    downloadPath = get_zip_directory(city)
    target_file_path = os.path.join(downloadPath)

    # Wait for the button to appear and click "Descargar todos"
    with page.expect_download(timeout=120000) as download_info:
        page.get_by_text("Descargar todos", exact=True).click()

    # Save the downloaded file
    download = download_info.value
    path = f"{target_file_path}/{download.suggested_filename}"

    download.save_as(path)

    print(f"Downloaded: {path}")

    context.close()
    browser.close()


def download(config):
    with sync_playwright() as playwright:
        run(playwright, config)
