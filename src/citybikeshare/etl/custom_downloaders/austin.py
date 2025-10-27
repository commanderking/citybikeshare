import os
from src.citybikeshare.utils.paths import get_raw_files_directory
from src.citybikeshare.etl.custom_downloaders.utils.get_single_csv_export import (
    get_exports,
)

FILE_NAME = "austin_all_trips.csv"
CSV_PATH = get_raw_files_directory("austin")
FILE_PATH = os.path.join(CSV_PATH, FILE_NAME)


def download(config):
    source_url = config.get("source_url", "")
    get_exports(source_url, FILE_PATH)
