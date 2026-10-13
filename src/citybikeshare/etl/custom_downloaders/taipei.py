import os
import re
import shutil
from zipfile import ZipFile, ZipInfo
from io import BytesIO
import requests
import polars as pl
from citybikeshare.etl.custom_downloaders.utils.download_helpers import should_download

HISTORY_INDEX = (
    "https://tcgbusfs.blob.core.windows.net/dotapp/"
    "youbike_second_ticket_opendata/YouBikeHis.csv"
)

_URL_MONTH_RE = re.compile(r"^(\d{6})_")


def _extract_month_from_url(file_url: str) -> str:
    """YYYYMM from a monthly zip's basename (e.g. 202602_YouBike2.0….zip).

    The URL is the only reliable place to read the month: member names inside the zip
    have changed layout over time (see _select_trip_csv_member).
    """
    match = _URL_MONTH_RE.match(os.path.basename(file_url))
    if not match:
        raise ValueError(
            f"Taipei zip URL has no YYYYMM_ prefix: {file_url}. "
            "Check for a source naming change."
        )
    return match.group(1)


def _expected_output(file_url: str, download_path) -> str:
    """Target CSV path for a monthly zip, derived from the URL alone, so a month we
    already have is skipped without fetching the (large) archive."""
    return os.path.join(download_path, f"{_extract_month_from_url(file_url)}.csv")


def _select_trip_csv_member(zip_file: ZipFile, month: str) -> ZipInfo:
    """The one trip CSV inside a monthly zip.

    Member layouts seen so far:
      - ``202602_YouBike2.0….csv``
      - ``202112_…/`` directory entry + ``202112_…/202112_….csv`` (2021-12 – 2022-09)
      - ``YouBike2.0…_202603.csv`` — month moved to a suffix (2026-03 onward)
    """
    csv_members = [
        info
        for info in zip_file.infolist()
        if not info.is_dir() and info.filename.lower().endswith(".csv")
    ]
    if len(csv_members) != 1:
        raise ValueError(
            f"Taipei {month} zip should hold exactly one CSV, found "
            f"{[info.filename for info in zip_file.infolist()]}"
        )
    return csv_members[0]


def _write_month_csv(zip_bytes: bytes, month: str, target_path: str) -> None:
    with ZipFile(BytesIO(zip_bytes)) as zip_file:
        member = _select_trip_csv_member(zip_file, month)
        # Write to .part and rename only on success: should_download trusts filename
        # presence, so a truncated file at target_path would never be re-fetched.
        part_path = target_path + ".part"
        with zip_file.open(member) as source, open(part_path, "wb") as target_file:
            shutil.copyfileobj(source, target_file, length=1 << 20)
    os.replace(part_path, target_path)
    print(f"Downloaded file to {target_path}")


##
# Get main csv, which lists monthly csv data in zip form
# Write each month's trip CSV to download/<YYYYMM>.csv
def download(config, context):
    download_path = context.download_directory
    df = pl.read_csv(HISTORY_INDEX)

    for file_url in df["fileURL"].to_list():
        # Monthly archives are immutable once published, and the month is encoded in
        # the URL, so a month we already have is skipped without any network fetch.
        target_path = _expected_output(file_url, download_path)
        if not should_download(target_path):
            continue

        response = requests.get(file_url, timeout=10000)
        if response.status_code != 200:
            print(f"Failed to download file from {file_url}")
            continue

        _write_month_csv(
            response.content, _extract_month_from_url(file_url), target_path
        )
