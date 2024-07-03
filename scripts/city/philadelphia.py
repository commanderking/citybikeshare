import os
import sys
import polars as pl

project_root = os.getenv('PROJECT_ROOT')
sys.path.insert(0, project_root)
import scripts.utils as utils
import scripts.utils_playwright as utils_playwright

PHILADELPHIA_BIKESHARE_URL = 'https://www.rideindego.com/about/data/'
STATIONS_CSV_PATH = utils.get_raw_files_directory("philadelphia")
DOWNLOAD_PATH = utils.get_zip_directory("philadelphia")

def get_zip_files():
    utils_playwright.get_bicycle_transit_systems_zips(PHILADELPHIA_BIKESHARE_URL, DOWNLOAD_PATH, STATIONS_CSV_PATH)

if __name__ == "__main__":
    get_zip_files()
