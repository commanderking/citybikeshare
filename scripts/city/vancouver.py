import os
import polars as pl
from playwright.sync_api import sync_playwright
import scripts.utils as utils

renamed_columns_2024 = {
    "Departure": "start_time",
    "Return": "end_time",
    "Bike": "bike_id",
    "Electric bike": "is_electric_bike",
    "Departure station": "start_station_name",
    "Return station": "end_station_name",
    "Membership type": "membership_type",
    "Covered distance (m)": "covered_distance_meters",
    "Duration (sec.)": "duration_seconds",
    "Stopover duration (sec.)": "stopover_duration",
    "Number of stopovers": "stopover_count",
}
date_columns = ["start_time", "end_time"]
final_column_headers = [
    "start_time",
    "end_time",
    "duration_seconds",
    "start_station_name",
    "end_station_name",
]
config = {
    "name": "vancouver",
    "file_matcher": ["Mobi_System_Data"],
    "renamed_columns": {**renamed_columns_2024},
}

# ZIP_DIRECTORY = utils.get_zip_directory("vancouver")
OPEN_DATA_URL = "https://www.mobibikes.ca/en/system-data"
CSV_PATH = utils.get_raw_files_directory("vancouver")
date_formats = ["%Y-%m-%d %H:%M", "%m/%d/%Y %H:%M"]


def run_get_exports(playwright, url, csv_path):
    browser = playwright.chromium.launch(headless=True)
    context = browser.new_context(accept_downloads=True)
    page = context.new_page()

    page.goto(url)

    links = page.query_selector_all("a")
    page.locator("#axeptio_btn_dismiss").click()

    for link in links:
        href = link.get_attribute("href")
        if href and "drive.google.com" in href:
            print(f"Clicking on link with href: {href}")

            # Create a new page manually
            new_page = context.new_page()
            new_page.goto(href)
            new_page.wait_for_load_state()  # Wait for the new page to load

            with new_page.expect_download() as download_info:
                # Perform the action that initiates download
                new_page.get_by_label("Download", exact=True).click()
            download = download_info.value
            download.save_as(os.path.join(csv_path, download.suggested_filename))
    browser.close()

    ### TODO: Convert 2017.xls file to csv


def format_files(files, args):
    renamed_columns = config["renamed_columns"]

    dfs = []
    for file in files:
        print(file)
        df = (
            # There is a ascii encoding for: 0099 ax��YnYq Xwtl'e7�n5 Square - Vancouver Art Gallery, which requires encoding="utf8-lossy"
            pl.read_csv(
                file,
                infer_schema_length=0,
                dtypes={"Covered distance (m)": pl.Float64},
                encoding="utf8-lossy",
            )
            ### In 2023, many files end in tens of thosuands of rows that have no data for any column, likely due to itts storage in Google Drive
            .filter(~pl.all_horizontal(pl.all().is_null()))
            .pipe(
                utils.rename_columns_for_keys(
                    renamed_columns,
                )
            )
            ## Getting some null values because some dates are not zero-padded "2020-04-01 0:00"
            .pipe(utils.convert_columns_to_datetime(date_columns, date_formats))
            .with_columns([pl.col("duration_seconds").cast(pl.Int64)])
            .pipe(utils.offset_two_digit_years)
            .select(final_column_headers)
            .pipe(utils.assess_null_data)
        )

        dfs.append(df)

    return pl.concat(dfs)


def build_trips(args):
    print("building trips")
    # No unzipping needed - files already downloaded as csv
    files = utils.get_csv_files(CSV_PATH)
    df = format_files(files, args)
    utils.create_final_files_and_logs(df, args)


def get_exports(url, csv_path):
    with sync_playwright() as playwright:
        run_get_exports(playwright, url, csv_path)


if __name__ == "__main__":
    get_exports(OPEN_DATA_URL, CSV_PATH)
