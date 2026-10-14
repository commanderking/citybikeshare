import os
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urljoin, urlparse

import requests
from playwright.sync_api import sync_playwright

from citybikeshare.etl.custom_downloaders.utils.download_helpers import should_download
from citybikeshare.etl.custom_downloaders.utils.browser import launch_chromium


# ecobici throttles each connection to ~0.5 MB/s, so wall-clock time is bound by
# concurrency, not our link. Their server also caps keep-alive at max=50, so keep
# the pool modest to stay well under that and be a polite client.
_MAX_CONCURRENT_DOWNLOADS = 4


def _fetch_csv_links(playwright, url):
    """Return the page's CSV anchors as (label, href) pairs; the label is the month
    ("2025-02") the page claims the link is for."""
    browser = launch_chromium(playwright)
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

    links = page.eval_on_selector_all(
        "a[href*='.csv']",
        "els => els.map(el => [el.textContent.trim(), el.getAttribute('href')])",
    )
    browser.close()
    return [(label, href) for label, href in links]


def _apply_url_overrides(links, overrides):
    """Point each overridden month label at its configured file, adding labels the
    page doesn't list at all. ecobici's page is an unreliable index: some labels link
    the wrong file, and some months exist on the server with no link (see
    `url_overrides` in the city config)."""
    resolved = [(label, overrides.get(label, href)) for label, href in links]
    listed = {label for label, _ in links}
    resolved += [(label, href) for label, href in overrides.items() if label not in listed]
    return resolved


def _assert_unique_hrefs(links):
    """Two month labels sharing one file means a month is silently missing — how
    Feb 2025 was lost (its label pointed at 2025-01.csv)."""
    labels_by_href = {}
    for label, href in links:
        labels_by_href.setdefault(href, []).append(label)
    shared = {href: labels for href, labels in labels_by_href.items() if len(labels) > 1}
    if shared:
        details = "; ".join(f"{href} <- {labels}" for href, labels in shared.items())
        raise ValueError(
            f"ecobici page links one file under several month labels: {details}. "
            "Find the real file for the mislinked month (the WordPress media API, "
            "/wp-json/wp/v2/media, lists every upload) and add it to url_overrides."
        )


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


def run_get_exports(playwright, url, csv_path, url_overrides):
    links = _apply_url_overrides(_fetch_csv_links(playwright, url), url_overrides)
    _assert_unique_hrefs(links)
    file_urls = [urljoin(url, href) for _, href in links]

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
        run_get_exports(
            playwright, url, download_path, config.get("url_overrides", {})
        )
