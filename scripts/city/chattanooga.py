import os
import scripts.utils as utils
import scripts.utils_playwright as utils_playwright

URL = "https://internal.chattadata.org/Recreation/Bike-Chattanooga-Trip-Data/tdrg-39c4/about_data"

FILE_NAME = "chattanooga_all_trips.csv"
CSV_PATH = utils.get_raw_files_directory("chattanooga")
FILE_PATH = os.path.join(CSV_PATH, FILE_NAME)


def get_trips_csv():
    utils_playwright.get_exports(URL, FILE_PATH)


if __name__ == "__main__":
    get_trips_csv()
