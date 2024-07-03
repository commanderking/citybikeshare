import os
import sys
from playwright.sync_api import sync_playwright
import polars as pl

project_root = os.getenv('PROJECT_ROOT')
sys.path.insert(0, project_root)
import definitions
import scripts.utils as utils
import scripts.utils_playwright as utils_playwright


DOWNLOAD_PATH = utils.get_zip_directory("los_angeles")
STATIONS_CSV_PATH = utils.get_raw_files_directory("los_angeles")

def get_zip_files():
    utils_playwright.get_bicycle_transit_systems_zips('https://bikeshare.metro.net/about/data/', DOWNLOAD_PATH, STATIONS_CSV_PATH )
    
    
if __name__ == "__main__":
    get_zip_files()
