import os
import scripts.utils as utils
import scripts.utils_playwright as utils_playwright

URL = "https://data.austintexas.gov/Transportation-and-Mobility/Austin-MetroBike-Trips/tyfh-5r8s/about_data"

FILE_NAME = "austin_all_trips.csv"
CSV_PATH = utils.get_raw_files_directory("austin")
FILE_PATH = os.path.join(CSV_PATH, FILE_NAME)


def get_trips_csv():
    utils_playwright.get_exports(URL, FILE_PATH)


if __name__ == "__main__":
    get_trips_csv()
