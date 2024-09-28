import os
import sys
import zipfile
import datetime
import polars as pl
from playwright.sync_api import sync_playwright

project_root = os.getenv('PROJECT_ROOT')
sys.path.insert(0, project_root)
import scripts.utils as utils

### 2022 - Montreal moved to new system

# FILE_NAME = "mexico_city_all_trips.csv"
CSV_PATH = utils.get_zip_directory("montreal")
# FILE_PATH = os.path.join(CSV_PATH, FILE_NAME)

OPEN_DATA_URL = "https://bixi.com/en/open-data/"
MONTREAL_CSV_PATHS = utils.get_raw_files_directory("montreal")


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

def extract_csvs(city):
    city_zip_directory = utils.get_zip_directory(city)
    print(city_zip_directory)
    for file in os.listdir(city_zip_directory):
        file_path = os.path.join(city_zip_directory, file)
        if file_path.endswith(".zip"):
            with zipfile.ZipFile(file_path, mode="r") as archive:
                archive.extractall(utils.get_raw_files_directory(city))

["start_time", "end_time", "start_station_name", "end_station_name"]

earliest_days_columns = {
    "start_date": "start_time",
    "end_date": "end_time",
    "start_station_code": "start_station_name",
    "end_station_code": "end_station_name"
}

emplacement_days_columns = {
    "start_date": "start_time",
    "end_date": "end_time",
    "emplacement_pk_start": "start_station_name",
    "emplacement_pk_end": "end_station_name",
}

# STARTSTATIONNAME	STARTSTATIONARRONDISSEMENT	STARTSTATIONLATITUDE	STARTSTATIONLONGITUDE	ENDSTATIONNAME	ENDSTATIONARRONDISSEMENT	ENDSTATIONLATITUDE	ENDSTATIONLONGITUDE	STARTTIMEMS	ENDTIMEMS

recent_days_columns = {
    "STARTSTATIONNAME": "start_station_name",
    "ENDSTATIONNAME": "end_station_name",
    "STARTTIMEMS": "start_ms",
    "ENDTIMEMS": "end_ms",
}

final_columns = ["start_station_name", "end_station_name", "start_time", "end_time"]



# column_mappings = [
#     {
#         "header_matcher": "ride_id",
#         "mapping": commonized_system_data_columns
#     },
#     {
#         "header_matcher": "from_station_name",
#         "mapping": chicago_renamed_columns_pre_march_2023
#     },
#     {
#         "header_matcher": "01 - Rental Details Local Start Time",
#         "mapping": chicago_renamed_columns_oddball
#     }
# ]



def create_all_trips_df():
    files = utils.get_csv_files(MONTREAL_CSV_PATHS)
    all_dfs = []
    for file in files:
        print(file)
        if "station" not in file.lower():
            df = pl.read_csv(file, null_values="MTL-ECO5.1-01")
            
            headers = df.columns
            renamed_columns = {}
            if "start_station_code" in headers:
                renamed_columns = earliest_days_columns
                
            if "emplacement_pk_start" in headers:
                renamed_columns = emplacement_days_columns
                
            if "STARTSTATIONNAME" in headers:
                renamed_columns = recent_days_columns
            
            df = df.rename(renamed_columns)
            
            post_rename_headers = df.columns

            if "start_ms" in post_rename_headers:
                df = df.with_columns([pl.from_epoch("start_ms", time_unit="ms").alias("start_time"), pl.from_epoch("end_ms", time_unit="ms").alias("end_time")])
            
            df = df.with_columns([pl.col("start_station_name").cast(pl.String), pl.col("end_station_name").cast(pl.String)])
            
            date_formats = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"]
            df = df.with_columns([
                # Replace . and everything that follows with empty string. Some Boston dates have milliseconds
                pl.coalesce([pl.col("start_time").str.replace(r"\.\d+", "").str.strptime(pl.Datetime, format, strict=False) for format in date_formats]),
                pl.coalesce([pl.col("end_time").str.replace(r"\.\d+", "").str.strptime(pl.Datetime, format, strict=False) for format in date_formats]),
            ])
                
            all_dfs.append(df.select(final_columns))
            print(df.select(final_columns))
            
    all_trips = pl.concat(all_dfs, how="diagonal")
    print(all_trips)
    
    return all_trips


def build_trips(args):
    if not args.skip_unzip:
        extract_csvs("montreal")
    all_trips_df = create_all_trips_df()
    
    utils.log_final_results(all_trips_df, args)
    
    utils.create_all_trips_file(all_trips_df, args)
    utils.create_recent_year_file(all_trips_df, args)

### Latest column names


### 05_2014
# start_date	start_station_code	end_date	end_station_code	duration_sec	is_member

### 2021
# start_date	emplacement_pk_start	end_date	emplacement_pk_end	duration_sec	is_member

def get_exports(url, csv_path):
    with sync_playwright() as playwright:
        run_get_exports(playwright, url, csv_path)



if __name__ == "__main__":
    # get_exports(OPEN_DATA_URL, CSV_PATH)
    # build_trips("hello")
          create_all_trips_df()
