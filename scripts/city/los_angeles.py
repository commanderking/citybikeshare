import os
import sys
from playwright.sync_api import sync_playwright
import polars as pl

project_root = os.getenv("PROJECT_ROOT")
sys.path.insert(0, project_root)
import scripts.utils_playwright as utils_playwright

LA_BIKESHARE_URL = "https://bikeshare.metro.net/about/data/"


def get_zip_files():
    utils_playwright.get_bicycle_transit_systems_zips(LA_BIKESHARE_URL, "los_angeles")


if __name__ == "__main__":
    get_zip_files()
