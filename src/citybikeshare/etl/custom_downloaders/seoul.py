import os
from playwright.sync_api import sync_playwright
from citybikeshare.context import PipelineContext
from citybikeshare.etl.custom_downloaders.utils.download_helpers import (
    prune_stale_downloads,
    should_download,
)
from citybikeshare.etl.custom_downloaders.utils.browser import launch_chromium


def main(p, config, context: PipelineContext):
    download_path = context.download_directory

    browser = launch_chromium(p)
    browser_context = browser.new_context(accept_downloads=True)
    page = browser_context.new_page()
    page.goto("https://data.seoul.go.kr/dataList/OA-15182/F/1/datasetView.do#")

    # Wait for the page content and list of downloads to appear
    page.wait_for_selector("text=파일내려받기")

    # Click “전체 파일보기” to expand the list
    page.click("text=전체 파일보기")

    # Each "다운로드" link is a javascript:downloadFile(id) call with no real URL, but its
    # title carries the exact filename the browser saves it as — so the skip check runs
    # before clicking instead of after pulling a multi-GB file.
    links = page.query_selector_all('a:has-text("다운로드")')
    remote_filenames = {link.get_attribute("title") for link in links}

    print(f"Found {len(links)} download links")

    for link in links:
        filename = link.get_attribute("title")
        if not should_download(os.path.join(download_path, filename)):
            continue

        with page.expect_download() as download_info:
            link.click()
            print(f"Downloading {filename}")
        download = download_info.value

        download.save_as(os.path.join(download_path, download.suggested_filename))
        print(f"✅ Downloaded {download.suggested_filename}")

    # The current year ships as monthly CSVs (…_2601.csv) and past years as one zip
    # (…_2025.zip), so a finished year's monthlies vanish when it's re-bundled.
    prune_stale_downloads(download_path, remote_filenames)
    browser.close()


def download(config, context):
    with sync_playwright() as playwright:
        main(playwright, config, context)
