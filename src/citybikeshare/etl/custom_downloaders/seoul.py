import os
from playwright.sync_api import sync_playwright
from src.citybikeshare.utils.paths import get_download_directory


def main(p, config):
    config.get("name")
    city = config.get("name")
    raw_file_path = get_download_directory(city)

    browser = p.chromium.launch(headless=True)
    context = browser.new_context(accept_downloads=True)
    page = context.new_page()
    page.goto("https://data.seoul.go.kr/dataList/OA-15182/F/1/datasetView.do#")

    # Wait for the page content and list of downloads to appear
    page.wait_for_selector("text=파일내려받기")

    # Click “전체 파일보기” to expand the list
    page.click("text=전체 파일보기")

    # Find all the “다운로드” links in the list
    links = page.query_selector_all('a:has-text("다운로드")')

    print(f"Found {len(links)} download links")

    for link in links:
        with page.expect_download() as download_info:
            link.click()
            print(f"Downloading file #{link}")
            download = download_info.value

            download.save_as(os.path.join(raw_file_path, download.suggested_filename))
            print(f"✅ Downloaded {os.path.basename(download.suggested_filename)}")

    browser.close()


def download(config):
    with sync_playwright() as playwright:
        main(playwright, config)
