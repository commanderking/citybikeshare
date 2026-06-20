import os
from playwright.sync_api import sync_playwright
from citybikeshare.context import PipelineContext
from citybikeshare.etl.custom_downloaders.utils.download_helpers import should_download


def _accept_dialog(dialog):
    # After clicking download, data.go.kr pops a native alert
    # ("…변경된 데이터입니다") that must be accepted (OK) before the file is served.
    print(f"ℹ️  Accepting dialog: {dialog.message}")
    dialog.accept()


def run(playwright, url, context: PipelineContext):
    download_path = context.download_directory
    browser = playwright.chromium.launch(headless=True)
    browser_context = browser.new_context(accept_downloads=True)
    page = browser_context.new_page()
    page.on("dialog", _accept_dialog)
    page.goto(url, wait_until="domcontentloaded")

    # Click the "다운로드" (Download) link. The page renders in Korean headless, so the
    # link text is "다운로드"; get_by_role matches it cleanly (the onclick handler is
    # fileDetailObj.fn_fileDataDown(...)). If data.go.kr ever serves English, this name
    # would be "Download".
    link = page.get_by_role("link", name="다운로드").first
    with page.expect_download(timeout=300000) as download_info:
        print(
            "Clicking link - because this is a large zip file, downloading will take a while."
        )
        link.click()
    download = download_info.value

    target = os.path.join(download_path, download.suggested_filename)
    if should_download(target):
        download.save_as(target)
        print(f"✅ Downloaded {download.suggested_filename}")
    browser.close()


def download(config, context: PipelineContext):
    url = config.get("source_url")
    with sync_playwright() as playwright:
        run(playwright, url, context)
