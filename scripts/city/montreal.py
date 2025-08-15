import os
import polars as pl
from playwright.sync_api import sync_playwright
import scripts.utils as utils

CSV_PATH = utils.get_zip_directory("montreal")
OPEN_DATA_URL = "https://bixi.com/en/open-data/"
MONTREAL_CSV_PATHS = utils.get_raw_files_directory("montreal")
date_formats = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"]


def run_get_exports(playwright, url, csv_path):
    browser = playwright.chromium.launch(headless=True)
    context = browser.new_context(accept_downloads=True)
    page = context.new_page()

    page.goto(url)

    links = page.query_selector_all("a")

    page.locator("#onetrust-accept-btn-handler").click()

    for link in links:
        href = link.get_attribute("href")
        if href and ".zip" in href:
            print(f"Clicking on link with href: {href}")
            link.click()
            with page.expect_download(timeout=120000) as download_info:
                link.click()
            download = download_info.value
            download.save_as(os.path.join(csv_path, download.suggested_filename))

    browser.close()


earliest_days_columns = {
    "start_date": "start_time",
    "end_date": "end_time",
    "start_station_code": "start_station_name",
    "end_station_code": "end_station_name",
}

emplacement_days_columns = {
    "start_date": "start_time",
    "end_date": "end_time",
    "emplacement_pk_start": "start_station_name",
    "emplacement_pk_end": "end_station_name",
}

recent_days_columns = {
    "STARTSTATIONNAME": "start_station_name",
    "ENDSTATIONNAME": "end_station_name",
    "STARTTIMEMS": "start_ms",
    "ENDTIMEMS": "end_ms",
}

final_columns = ["start_station_name", "end_station_name", "start_time", "end_time"]


def get_renamed_columns(df):
    headers = df.columns
    renamed_columns = {}
    ### Bixi has had three separate column names in its history
    if "start_station_code" in headers:
        renamed_columns = earliest_days_columns
    elif "emplacement_pk_start" in headers:
        renamed_columns = emplacement_days_columns
    elif "STARTSTATIONNAME" in headers:
        renamed_columns = recent_days_columns
    else:
        raise Exception("Did not find matching set of headers to rename columns")

    return df.rename(renamed_columns)


def convert_milliseconds_to_datetime(df):
    headers = df.columns
    ### most recent Montreal data notes start time and end time in ms whereas previous versions used a date.
    if "start_ms" in headers:
        df = df.with_columns(
            [
                pl.from_epoch("start_ms", time_unit="ms").alias("start_time"),
                pl.from_epoch("end_ms", time_unit="ms").alias("end_time"),
            ]
        )
    return df


def stringify_start_end_station_names(df):
    return df.with_columns(
        [
            pl.col("start_station_name").cast(pl.String),
            pl.col("end_station_name").cast(pl.String),
        ]
    )


def create_all_trips_df():
    files = utils.get_csv_files(MONTREAL_CSV_PATHS)
    all_dfs = []
    for file in files:
        print(f"reading {file}")
        if "station" not in file.lower():
            df = (
                pl.read_csv(file, null_values="MTL-ECO5.1-01")
                .pipe(get_renamed_columns)
                .pipe(convert_milliseconds_to_datetime)
                .select(final_columns)
                .with_columns(
                    [
                        pl.col("start_station_name").cast(pl.String),
                        pl.col("end_station_name").cast(pl.String),
                    ]
                )
                .pipe(
                    utils.convert_columns_to_datetime(
                        ["start_time", "end_time"], date_formats
                    )
                )
            )
            all_dfs.append(df)

    all_trips = pl.concat(all_dfs, how="diagonal")
    return all_trips


def build_trips(args):
    if not args.skip_unzip:
        utils.unzip_city_zips("montreal")
    df = create_all_trips_df()

    utils.create_all_trips_file(df, args)
    utils.create_recent_year_file(df, args)
    utils.log_final_results(df, args)


def get_exports(url, csv_path):
    with sync_playwright() as playwright:
        run_get_exports(playwright, url, csv_path)


if __name__ == "__main__":
    get_exports(OPEN_DATA_URL, CSV_PATH)
