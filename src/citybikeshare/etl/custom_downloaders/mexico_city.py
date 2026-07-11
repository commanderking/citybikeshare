import os
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urljoin, urlparse

import requests
from playwright.sync_api import sync_playwright

from citybikeshare.etl.custom_downloaders.utils.download_helpers import should_download


# ecobici throttles each connection to ~0.5 MB/s, so wall-clock time is bound by
# concurrency, not our link. Their server also caps keep-alive at max=50, so keep
# the pool modest to stay well under that and be a polite client.
_MAX_CONCURRENT_DOWNLOADS = 4


def _fetch_csv_hrefs(playwright, url):
    browser = playwright.chromium.launch(headless=True)
    context = browser.new_context()
    page = context.new_page()

    # ecobici's page keeps a request open so neither "load" nor
    # "domcontentloaded" ever fires; commit as soon as the navigation is
    # accepted, then wait explicitly for the content we need below.
    page.goto(url, wait_until="commit")

    # The CSV anchors are rendered client-side and sit in a collapsed/hidden
    # accordion, so wait for them to be attached to the DOM (not visible) — the
    # UI no longer lets us click them, but the href is a direct link to the file.
    page.wait_for_selector("a[href*='.csv']", state="attached", timeout=60000)

    hrefs = page.eval_on_selector_all(
        "a[href*='.csv']", "els => els.map(el => el.getAttribute('href'))"
    )
    browser.close()
    return hrefs


def _download_csv(file_url, csv_path):
    filename = os.path.basename(urlparse(file_url).path)
    target_path = os.path.join(csv_path, filename)
    # Monthly files are immutable once published — don't re-fetch
    if not should_download(target_path):
        return

    print(f"Downloading {file_url}")
    # (connect, read) timeouts: the read timeout is the gap allowed *between*
    # chunks, so a slow-but-steady transfer isn't capped while a stalled
    # connection still fails loudly instead of hanging forever.
    with requests.get(file_url, stream=True, timeout=(30, 120)) as response:
        response.raise_for_status()
        # Stream to a .part file and rename only on success — should_download
        # trusts filename presence, so a truncated file at target_path would be
        # silently treated as complete on the next run.
        part_path = target_path + ".part"
        with open(part_path, "wb") as file:
            for chunk in response.iter_content(chunk_size=1 << 20):
                file.write(chunk)
    os.replace(part_path, target_path)


def run_get_exports(playwright, url, csv_path):
    hrefs = _fetch_csv_hrefs(playwright, url)
    file_urls = [urljoin(url, href) for href in hrefs]

    with ThreadPoolExecutor(max_workers=_MAX_CONCURRENT_DOWNLOADS) as executor:
        futures = [
            executor.submit(_download_csv, file_url, csv_path) for file_url in file_urls
        ]
        # .result() re-raises any worker exception, so a failed download aborts
        # the run rather than being swallowed by the executor.
        for future in futures:
            future.result()


def download(config, context):
    url = config.get("source_url")
    download_path = context.download_directory
    with sync_playwright() as playwright:
        run_get_exports(playwright, url, download_path)
