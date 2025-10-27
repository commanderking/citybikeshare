import os
import scripts.utils as utils
import scripts.utils_playwright as utils_playwright

FILE_NAME = "austin_all_trips.csv"
CSV_PATH = utils.get_raw_files_directory("austin")
FILE_PATH = os.path.join(CSV_PATH, FILE_NAME)


def download(config):
    source_url = config.get("source_url", "")
    utils_playwright.get_exports(source_url, FILE_PATH)
