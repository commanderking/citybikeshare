import os
from pathlib import Path
from urllib.parse import urljoin, urlparse
from playwright.sync_api import sync_playwright
from citybikeshare.context import PipelineContext
from citybikeshare.etl.custom_downloaders.utils.download_helpers import (
    does_file_exist,
    get_file_size_from_url,
)


def _zip_filename(href: str) -> str:
    """Local filename a linked archive will be stored under (its URL basename)."""
    return os.path.basename(urlparse(href).path)


def _prune_stale_zips(download_path: Path, remote_filenames: set[str]) -> None:
    """Remove local .zip archives the page no longer offers.

    Montreal renames the current-year archive as months are appended
    (DonneesOuvertes2026_010203.zip → ...01020304.zip). Pruning whatever is no
    longer linked drops the stale archive without re-downloading the immutable
    completed-year ones — the reason the old code wiped download/ wholesale.
    """
    for existing in download_path.glob("*.zip"):
        if existing.name not in remote_filenames:
            print(f"🗑️  Removing stale archive no longer offered: {existing.name}")
            existing.unlink()


def run_get_exports(playwright, url, download_path: Path):
    browser = playwright.chromium.launch(headless=True)
    context = browser.new_context(accept_downloads=True)
    page = context.new_page()
    page.goto(url)
    page.locator("#onetrust-accept-btn-handler").click()

    # Collect every .zip link as (filename, absolute_url, element).
    remote = []
    for link in page.query_selector_all("a"):
        href = link.get_attribute("href")
        if href and ".zip" in href:
            absolute = urljoin(url, href)
            remote.append((_zip_filename(absolute), absolute, link))

    if not remote:
        # Don't prune on an empty scrape — a transient page failure shouldn't
        # wipe good archives.
        print("⚠️  No .zip links found on Montreal page; leaving download/ untouched")
        browser.close()
        return

    for filename, href, link in remote:
        remote_size = get_file_size_from_url(href)
        if does_file_exist(filename, remote_size, download_path):
            print(f"🟡 Skipping download - {filename} unchanged ({remote_size} bytes)")
            continue

        print(f"Attempting Download of {href}")
        with page.expect_download(timeout=120000) as download_info:
            link.click()
        download = download_info.value
        download.save_as(os.path.join(download_path, filename))
        print(f"✅ Downloaded {filename}")

    _prune_stale_zips(download_path, {filename for filename, _, _ in remote})
    browser.close()


def download(config, context: PipelineContext):
    """Standard entrypoint for ETL to call."""
    download_path = context.download_directory
    download_path.mkdir(parents=True, exist_ok=True)
    url = config["source_url"]
    with sync_playwright() as playwright:
        ## Montreal renames the same year's archive as it appends months
        ## (2026_01 → 2026_01020304). Rather than wipe and refetch everything,
        ## we skip archives already on disk (by name + Content-Length) and prune
        ## any the page no longer offers, so only new/changed archives download.
        run_get_exports(playwright, url, download_path)
