import os
from zipfile import ZipFile
from io import BytesIO
import requests
import polars as pl
from citybikeshare.etl.custom_downloaders.utils.download_helpers import should_download

HISTORY_INDEX = (
    "https://tcgbusfs.blob.core.windows.net/dotapp/"
    "youbike_second_ticket_opendata/YouBikeHis.csv"
)


### CSV files often have non-ASCII characters (i.e. 202403_YouBike2.0≤º√“®Í•d∏ÍÆ∆.csv)
def clean_filename(filename):
    return f"{filename[:6]}.csv"


def _expected_output(file_url: str, download_path) -> str:
    """Target CSV path for a monthly zip, derived from the URL alone.

    Each zip's basename starts with YYYYMM (e.g. 202602_YouBike2.0….zip) — the
    same prefix clean_filename pulls from the member name. So we can tell whether
    a month is already present without fetching and unzipping the (large) archive.
    """
    return os.path.join(download_path, clean_filename(os.path.basename(file_url)))


##
# Get main csv, which lists monthly csv data in zip form
# For all monthly zips, unzip all csv files to folder
# Read all csvs and bundle into one large parquet file
def download(config, context):
    download_path = context.download_directory
    df = pl.read_csv(HISTORY_INDEX)

    file_urls = df["fileURL"].to_list()

    for file_url in file_urls:
        # Monthly archives are immutable once published. Skip the network
        # download entirely when the month's CSV is already present — the month
        # is encoded in the URL, so no fetch/unzip is needed to decide.
        if not should_download(_expected_output(file_url, download_path)):
            continue

        response = requests.get(file_url, timeout=10000)
        if response.status_code != 200:
            print(f"Failed to download file from {file_url}")
            continue

        with ZipFile(BytesIO(response.content)) as zip_file:
            for file in zip_file.namelist():
                target_path = os.path.join(download_path, clean_filename(file))

                # Guards multi-member archives; the per-URL check above already
                # skipped months we have.
                if not should_download(target_path):
                    continue

                with zip_file.open(file) as source, open(
                    target_path, "wb"
                ) as target_file:
                    target_file.write(source.read())
                    print(f"Downloaded file to {target_path}")
