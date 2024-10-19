import os
import sys
import json
import polars as pl
from playwright.sync_api import sync_playwright

project_root = os.getenv("PROJECT_ROOT")
sys.path.insert(0, project_root)
import scripts.utils as utils

BERGEN = "bergen"

ZIP_PATH = utils.get_zip_directory(BERGEN)
OPEN_DATA_URL = "https://bergenbysykkel.no/en/open-data/historical"
CSV_PATH = utils.get_raw_files_directory(BERGEN)
METADATA_PATH = utils.get_metadata_directory(BERGEN)
date_formats = ["%Y-%m-%d %H:%M:%S.%f%:z", "%Y-%m-%d %H:%M:%S%:z"]

renamed_columns = {
    "started_at": "start_time",
    "ended_at": "end_time",
    "start_station_name": "start_station_name",
    "end_station_name": "end_station_name",
}
final_columns = ["start_station_name", "end_station_name", "start_time", "end_time"]


def run_get_exports(playwright, url):
    browser = playwright.chromium.launch(headless=True)
    context = browser.new_context(accept_downloads=True)
    page = context.new_page()

    page.goto(url)
    page.get_by_text("Confirm").click()
    csv_buttons = page.locator('role=button[name="CSV"]')

    for button in csv_buttons.all():
        desired_filename = (
            button.get_attribute("content").split(".no/")[1].replace("/", "-")
        )
        target_folder = ZIP_PATH if desired_filename.endswith(".zip") else CSV_PATH
        with page.expect_download(timeout=120000) as download_info:
            button.click()
            download = download_info.value
            print(f"Downloading {desired_filename}")
            download.save_as(os.path.join(target_folder, desired_filename))

    browser.close()


def create_all_trips_df(args):
    files = utils.get_csv_files(CSV_PATH)
    all_dfs = []
    for file in files:
        print(f"reading {file}")
        df = (
            pl.read_csv(file)
            .rename(renamed_columns)
            .select(final_columns)
            .with_columns(
                [
                    pl.coalesce(
                        [
                            pl.col("start_time").str.strptime(
                                pl.Datetime, format, strict=False
                            )
                            for format in date_formats
                        ]
                    ),
                    pl.coalesce(
                        [
                            pl.col("end_time").str.strptime(
                                pl.Datetime, format, strict=False
                            )
                            for format in date_formats
                        ]
                    ),
                ]
            )
        )
        all_dfs.append(df)

    all_trips = pl.concat(all_dfs)
    return all_trips


def get_exports(url):
    with sync_playwright() as playwright:
        run_get_exports(playwright, url)


def build_trips(args):
    df = create_all_trips_df(args)
    utils.log_final_results(df, args)
    utils.create_all_trips_file(df, args)
    utils.create_recent_year_file(df, args)
    return df


if __name__ == "__main__":
    get_exports(OPEN_DATA_URL)
