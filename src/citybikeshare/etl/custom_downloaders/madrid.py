import os
import re
from pathlib import Path
from urllib.parse import urljoin

import requests
from playwright.sync_api import sync_playwright

from citybikeshare.context import PipelineContext
from citybikeshare.etl.custom_downloaders.utils.download_helpers import (
    does_file_exist,
    get_file_size_from_url,
)

# datos.madrid.es serves each year's trips as an opaquely-named CKAN resource archive
# (900034-<n>-bicimad-viajes-estaciones.zip). The <n> is a resource number, NOT the year,
# and every year's archive shares the same basename — so the URL alone can't tell them
# apart. The year lives only in each card's "Datos Bicimad YYYY" label, which is why we
# scrape it off the DOM and store each archive under a year-keyed name below.
_YEAR_RE = re.compile(r"(20\d{2})")

# The card's year label sits in a <p> like "Datos Bicimad 2023"; from the Descarga link,
# walk up to the nearest ancestor that contains that <p> and read it.
_YEAR_LABEL_XPATH = (
    "xpath=ancestor::*[.//p[contains(.,'Datos Bicimad')]][1]"
    "//p[contains(.,'Datos Bicimad')]"
)


def _scrape_descarga_links(playwright, url: str) -> list[tuple[str, str]]:
    """Return [(year, absolute_zip_url)] for every "Descarga" button on the page.

    Matches the link text exactly so the "Descargas" nav item and the "Descargado"
    counters (both superstrings of "Descarga") don't get picked up.
    """
    browser = playwright.chromium.launch(headless=True)
    page = browser.new_context().new_page()
    try:
        page.goto(url, wait_until="domcontentloaded")
        links = page.get_by_text("Descarga", exact=True)

        results: list[tuple[str, str]] = []
        for i in range(links.count()):
            element = links.nth(i)
            href = element.get_attribute("href")
            label = element.locator(_YEAR_LABEL_XPATH).first.inner_text()
            match = _YEAR_RE.search(label or "")
            # Fail loud rather than silently skip: a link we can't tie to a year means the
            # page layout changed, and quietly dropping a year would look like success.
            if not href or not match:
                raise ValueError(
                    f"Could not resolve year/href for a 'Descarga' link "
                    f"(label={label!r}, href={href!r}) — Madrid page layout may have changed"
                )
            results.append((match.group(1), urljoin(url, href)))
    finally:
        browser.close()

    if not results:
        raise ValueError(
            "No 'Descarga' links found on the Madrid downloads page — "
            "page structure may have changed"
        )
    return results


def _download_zip(year: str, zip_url: str, download_path: Path) -> None:
    """Fetch one year's archive, storing it under a year-keyed name.

    All years share the same source basename, so we name by year to keep them distinct.
    A HEAD size check skips the (large) download when the on-disk copy already matches.
    """
    filename = f"bicimad_trips_{year}.zip"
    remote_size = get_file_size_from_url(zip_url)
    if does_file_exist(filename, remote_size, download_path):
        print(f"🟡 Skipping download - {filename} unchanged ({remote_size} bytes)")
        return

    target_path = download_path / filename
    # Stream to a .part file and rename only on success, so an interrupted transfer
    # never leaves a truncated archive that the size check would trust next run.
    part_path = Path(str(target_path) + ".part")
    print(f"Downloading {zip_url} -> {filename}")
    with requests.get(zip_url, stream=True, timeout=(30, 300)) as response:
        response.raise_for_status()
        with open(part_path, "wb") as file:
            for chunk in response.iter_content(chunk_size=1 << 20):
                file.write(chunk)
    os.replace(part_path, target_path)
    print(f"✅ Downloaded {filename}")


def download(config, context: PipelineContext) -> None:
    """Standard entrypoint for the ETL download stage."""
    download_path = context.download_directory
    download_path.mkdir(parents=True, exist_ok=True)
    url = config["source_url"]

    # Playwright only discovers the (year, url) pairs; the archives are direct links, so we
    # close the browser and stream them with requests (as mexico_city does).
    with sync_playwright() as playwright:
        year_links = _scrape_descarga_links(playwright, url)

    for year, zip_url in sorted(year_links):
        _download_zip(year, zip_url, download_path)
