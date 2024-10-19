import os
import json
import polars as pl
from playwright.sync_api import sync_playwright
import requests
import scripts.utils as utils

OSLO = "oslo"

ZIP_PATH = utils.get_zip_directory(OSLO)
OPEN_DATA_URL = "https://oslobysykkel.no/en/open-data/historical"
CSV_PATH = utils.get_raw_files_directory(OSLO)
METADATA_PATH = utils.get_metadata_directory(OSLO)
date_formats = ["%Y-%m-%d %H:%M:%S %z", "%Y-%m-%d %H:%M:%S%.f%:z"]
CURRENT_STATIONS_URL = (
    "https://gbfs.urbansharing.com/oslobysykkel.no/station_information.json"
)

version_one_columns = {
    "started_at": "start_time",
    "ended_at": "end_time",
    "start_station_name": "start_station_name",
    "end_station_name": "end_station_name",
}

legacy_columns = {
    "Start station": "start_station_id",
    "End station": "end_station_id",
    "Start time": "start_time",
    "End time": "end_time",
}


def get_stations_information():
    response = requests.get(CURRENT_STATIONS_URL)

    # Check if the request was successful
    if response.status_code == 200:
        json_data = response.json()
        with open(METADATA_PATH / "station_information.json", "w") as json_file:
            json.dump(json_data, json_file, indent=4)
        print(f"Downloaded JSON from url: {CURRENT_STATIONS_URL}")
    else:
        print(f"Failed to retrieve JSON. Status code: {response.status_code}")


def run_get_exports(playwright, url):
    browser = playwright.chromium.launch(headless=True)
    context = browser.new_context(accept_downloads=True)
    page = context.new_page()

    page.goto(url)
    csv_buttons = page.locator('role=button[name="CSV"]')

    # Download old to new station id mapping
    old_new_stations_button = page.get_by_text("Legacy/New station ID mapping")
    with page.expect_download(timeout=120000) as download_info:
        old_new_stations_button.click()
        download = download_info.value
        print(f"Downloading {download.suggested_filename}")
        download.save_as(os.path.join(METADATA_PATH, download.suggested_filename))

    for button in csv_buttons.all():
        # links are formatted as: - to get unique names and not 06.csv for ever year, get text after .no/ domain
        ## New - https://data.urbansharing.com/oslobysykkel.no/trips/v1/2019/06.csv
        ## Legacy -  https://data-legacy.urbansharing.com/oslobysykkel.no/2018/06.csv.zip
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


final_columns = ["start_station_name", "end_station_name", "start_time", "end_time"]


def get_renamed_columns(headers):
    ### Bixi has had three separate column names in its history
    if "started_at" in headers:
        return version_one_columns

    if "Start station" in headers:
        return legacy_columns

    raise Exception("Did not find matching set of headers to rename columns")


def get_stations_df():
    station_info_json = METADATA_PATH / "station_information.json"
    stations = []
    with open(station_info_json, "r") as file:
        data = json.load(file)
        stations = data["data"]["stations"]
    return pl.DataFrame(stations).select(["station_id", "name"])


def map_legacy_station_id_to_name(stations_df):
    stations_df = stations_df.select(["station_id", "name"]).with_columns(
        [pl.col("station_id").cast(pl.Int64)]
    )

    def inner(df):
        headers = df.columns

        ### Older data does not contain duration column
        if "duration" not in headers:
            station_mapping_df = pl.read_csv(
                METADATA_PATH / "legacy_new_station_id_mapping.csv"
            )
            df = (
                df.rename(
                    {
                        "start_station_id": "start_station_legacy_id",
                        "end_station_id": "end_station_legacy_id",
                    }
                )
                .join(
                    station_mapping_df,
                    left_on="start_station_legacy_id",
                    right_on="legacy_id",
                )
                .rename({"new_id": "start_station_id"})
                .join(
                    station_mapping_df,
                    left_on="end_station_legacy_id",
                    right_on="legacy_id",
                )
                .rename({"new_id": "end_station_id"})
                .join(stations_df, left_on="start_station_id", right_on="station_id")
                .rename({"name": "start_station_name"})
                .join(stations_df, left_on="end_station_id", right_on="station_id")
                .rename({"name": "end_station_name"})
            )
        return df

    return inner


def create_all_trips_df(args):
    files = utils.get_csv_files(CSV_PATH)
    stations_df = get_stations_df()
    all_dfs = []
    for file in files:
        print(f"reading {file}")

        df = pl.read_csv(file)
        empty = df.is_empty()
        if not empty:
            renamed_columns = get_renamed_columns(df.columns)
            df = (
                df.rename(renamed_columns)
                .pipe(map_legacy_station_id_to_name(stations_df))
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

    all_trips = pl.concat(all_dfs, how="diagonal")
    return all_trips


def get_exports(url):
    get_stations_information()
    with sync_playwright() as playwright:
        run_get_exports(playwright, url)


def build_trips(args):
    if not args.skip_unzip:
        utils.unzip_city_zips(args.city)

    df = create_all_trips_df(args)
    utils.log_final_results(df, args)
    utils.create_all_trips_file(df, args)
    utils.create_recent_year_file(df, args)
    return df


if __name__ == "__main__":
    get_exports(OPEN_DATA_URL)
