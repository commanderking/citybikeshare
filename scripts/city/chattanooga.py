import os
import scripts.utils as utils
import scripts.utils_playwright as utils_playwright

URL = "https://www.chattadata.org/dataset/Historical-Bike-Chattanooga-Trip-Data/wq49-8xgg/about_data"

FILE_NAME = "chattanooga_all_trips.csv"
CSV_PATH = utils.get_raw_files_directory("chattanooga")
FILE_PATH = os.path.join(CSV_PATH, FILE_NAME)


def get_trips_csv():
    utils_playwright.get_exports(URL, FILE_PATH)


if __name__ == "__main__":
    get_trips_csv()
