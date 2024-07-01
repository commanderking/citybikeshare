import os
import sys
from playwright.sync_api import sync_playwright
import polars as pl

project_root = os.getenv('PROJECT_ROOT')
sys.path.insert(0, project_root)
import definitions
import scripts.utils as utils

STATIONS_CSV_PATH = utils.get_raw_files_directory("philadelphia")

RENAMED_STATION_COLUMNS = {
    "Station_ID": "station_id",
    "Station_Name": "station_name",
    "Day of Go_live_date": "go_live_date",
    "Status": "status"
}


def run(playwright):
    # Create a downloaded zip directory if it doesn't exist
    download_path = utils.get_zip_directory("philadelphia")
    os.makedirs(download_path, exist_ok=True)

    browser = playwright.chromium.launch(headless=True)
    context = browser.new_context(accept_downloads=True)
    page = context.new_page()
    page.goto('https://www.rideindego.com/about/data/')

    # Find all .zip file links and download them
    zip_links = page.query_selector_all('a[href$=".zip"]')
    for link in zip_links:
        with page.expect_download() as download_info:
            link.click()
        download = download_info.value
        download.save_as(os.path.join(download_path, download.suggested_filename))
        print(f'Downloaded { download.suggested_filename }')

    # Download stations csv directly into csv folder
    stations_csv_link = page.get_by_role("link", name="Station Table")    
    with page.expect_download() as stations_download_info:
        stations_csv_link.click()
    stations_download = stations_download_info.value
    stations_download.save_as(os.path.join(STATIONS_CSV_PATH, "stations.csv"))
    print(f'Downloaded { stations_download.suggested_filename } as stations.csv')

    browser.close()

def get_zip_files():
    with sync_playwright() as playwright:
        run(playwright)

def get_stations_df():
    df = pl.read_csv(os.path.join(STATIONS_CSV_PATH, "stations.csv"))
    return df.rename(RENAMED_STATION_COLUMNS).with_columns([
        pl.col("station_id").cast(pl.String),
    ])
def append_station_names(trips_df, stations_df):
    joined_df = trips_df.join(
        stations_df.select(['station_id', 'station_name']),
        left_on='start_station_id',
        right_on='station_id',
        how='left'
    ).with_columns(
        pl.col('station_name').alias('start_station_name')
    ).join(
        stations_df.select(['station_id', 'station_name']),
        left_on='end_station_id',
        right_on='station_id',
        how='left'   
    ).with_columns(
        pl.col('station_name').alias('end_station_name')
    ).drop(["station_name", "station_name_right"])
    
    return joined_df

if __name__ == "__main__":
    get_zip_files()
