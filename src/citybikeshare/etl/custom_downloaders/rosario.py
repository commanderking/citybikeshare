import os
from playwright.sync_api import sync_playwright
from src.citybikeshare.context import PipelineContext


def run(playwright, config, context: PipelineContext):
    browser = playwright.chromium.launch(headless=True)  # Set to True for silent mode
    browser_context = browser.new_context(accept_downloads=True)
    page = browser_context.new_page()

    url = "https://datosabiertos.rosario.gob.ar/dataset/0e487f13-7725-4bbf-afea-52e429fa92e5"
    page.goto(url)

    download_directory = context.download_directory
    target_file_path = os.path.join(download_directory)

    # Wait for the button to appear and click "Descargar todos"
    with page.expect_download(timeout=120000) as download_info:
        page.get_by_text("Descargar todos", exact=True).click()

    # Save the downloaded file
    download = download_info.value
    path = f"{target_file_path}/{download.suggested_filename}"

    download.save_as(path)

    print(f"Downloaded: {path}")

    browser_context.close()
    browser.close()


def download(config, context: PipelineContext):
    with sync_playwright() as playwright:
        run(playwright, config, context)
