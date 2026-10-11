import os
from urllib.parse import urlparse

import requests

from citybikeshare.context import PipelineContext
from citybikeshare.etl.custom_downloaders.utils.download_helpers import (
    does_file_exist,
    get_file_size_from_url,
    prune_stale_downloads,
)

# Toronto publishes on CKAN: one "package" whose "resources" are a yearly zip of monthly CSVs
# (2017 on) or an xlsx workbook (2014–2016). The zips can nest another zip (2022-11 ships as
# `Bike share ridership 2022-11.zip` inside the 2022 archive), so unzipping here would have to
# reimplement what the shared extract stage already does — this downloader only fetches bytes.
_CKAN_BASE = "https://ckan0.cf.opendata.inter.prod-toronto.ca"
_PACKAGE_ID = "bike-share-toronto-ridership-data"


def _list_resources():
    """(local filename, download url) for every downloadable resource in the package."""
    package = requests.get(
        f"{_CKAN_BASE}/api/3/action/package_show", params={"id": _PACKAGE_ID}, timeout=60
    ).json()
    remote = []
    for resource in package["result"]["resources"]:
        if resource["datastore_active"]:
            continue
        url = resource["url"]
        remote.append((os.path.basename(urlparse(url).path), url))
    return remote


def download(config, context: PipelineContext):
    download_path = context.download_directory
    download_path.mkdir(parents=True, exist_ok=True)

    remote = _list_resources()
    for filename, url in remote:
        remote_size = get_file_size_from_url(url)
        if remote_size is not None and does_file_exist(filename, remote_size, download_path):
            print(f"🟡 Skipping download - {filename} unchanged ({remote_size} bytes)")
            continue

        print(f"⬇️  Downloading {filename}")
        with requests.get(url, stream=True, timeout=600) as response:
            response.raise_for_status()
            with open(download_path / filename, "wb") as target:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    target.write(chunk)
        print(f"✅ Downloaded {filename}")

    # The current year's zip keeps its name as months are appended (re-fetched by size above);
    # pruning catches anything that drops out of the listing.
    prune_stale_downloads(download_path, {filename for filename, _ in remote})
