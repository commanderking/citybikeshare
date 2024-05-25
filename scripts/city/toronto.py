from zipfile import ZipFile
from io import BytesIO
import os
import sys
import requests
import polars as pl

import utils

project_root = os.getenv('PROJECT_ROOT')
sys.path.insert(0, project_root)

import definitions

PARQUET_OUTPUT_PATH = definitions.DATA_DIR / "toronto_all_trips.parquet" 

TORONTO_CSV_PATH = utils.get_raw_files_directory("toronto")

initial_renamed_columns = {
    "trip_id": "trip_id",
    "trip_start_time": "start_time",
    "trip_stop_time": "end_time",
    "trip_duration_seconds": "duration",
    "from_station_name": "start_station_name",
    "to_station_name":"end_station_name",
    "user_type": "user_type"
}

current_renamed_columns = {
    "Trip Id": "trip_id",
    "Trip  Duration": "duration",
    "Start Station Id": "start_station_id",
    "Start Time": "start_time",
    "Start Station Name": "start_station_name",
    "End Station Id": "end_station_id",
    "End Time": "end_time",
    "End Station Name": "end_station_name",
    "Bike Id": "bike_id",
    "User Type": "user_type"
}

toronto_final_columns = ["start_time", "end_time", "duration", "start_station_name", "end_station_name", "user_type"]

def map_columns(df):
    headers = df.columns
    
    if ("trip_id" in headers):
        df = df.rename(initial_renamed_columns)
    else:
        df = df.rename(current_renamed_columns)
    
    return df.select(toronto_final_columns)

def extract_csvs(): 
    # To hit our API, you'll be making requests to:
    base_url = "https://ckan0.cf.opendata.inter.prod-toronto.ca"
    
    # Datasets are called "packages". Each package can contain many "resources"
    # To retrieve the metadata for this package and its resources, use the package name in this page's URL:
    url = base_url + "/api/3/action/package_show"
    params = { "id": "bike-share-toronto-ridership-data"}
    package = requests.get(url, params = params).json()
    
    # To get resource data:
    for idx, resource in enumerate(package["result"]["resources"]):
    
        # To get metadata for non datastore_active resources:
        if not resource["datastore_active"]:
            url = base_url + "/api/3/action/resource_show?id=" + resource["id"]
            resource_metadata = requests.get(url).json()
            # From here, you can use the "url" attribute to download this file
            response = requests.get(resource_metadata['result']['url'], timeout=10000)
            if response.status_code == 200:
                with ZipFile(BytesIO(response.content)) as zip_file:
                    zip_contents = zip_file.namelist()                    
                    for file in zip_contents:
                        if file.endswith(".csv"):
                            source = zip_file.open(file)
                            print(file)
                            file_name = os.path.basename(file)
                            target_path = os.path.join(TORONTO_CSV_PATH, file_name).lower().replace(" ", "_")

                            with open(target_path, 'wb') as target_file:
                                target_file.write(source.read())
                                print(f"Extracted and cleaned file to {target_path}")
            else:
                print(f"Failed to download file from {resource_metadata['result']['url']}")

def create_all_trips_df():
    csv_files = [os.path.join(TORONTO_CSV_PATH, f) for f in os.listdir(TORONTO_CSV_PATH) if f.endswith('.csv')]
    
    dfs = []
    for file in csv_files:
        print(file)
        # TODO: utf8-lossy needed because there are some special characters in csv
        # Example: Gailbraith Rd / KingG��s College Cr. (U of T)
        df = pl.read_csv(file, infer_schema_length=0, encoding="utf8-lossy")

        df = map_columns(df)

        df = df.with_columns([
            # Toronto data has three different possible date formats =( - Look at 2017_q1 for examples
            pl.when(pl.col("start_time").str.strptime(pl.Datetime, "%m/%d/%Y %H:%M", strict=False).is_not_null())
            .then(pl.col("start_time").str.strptime(pl.Datetime, "%m/%d/%Y %H:%M", strict=False))
                .when(pl.col("start_time").str.strptime(pl.Datetime, "%m/%d/%Y %H:%M:%S", strict=False).is_not_null())
                    .then(pl.col("start_time").str.strptime(pl.Datetime, "%m/%d/%Y %H:%M:%S", strict=False))
                    .otherwise(pl.col("start_time").str.strptime(pl.Datetime, "%d/%m/%Y %H:%M", strict=False))
            .alias("start_time"),

            pl.when(pl.col("end_time").str.strptime(pl.Datetime, "%m/%d/%Y %H:%M", strict=False).is_not_null())
            .then(pl.col("end_time").str.strptime(pl.Datetime, "%m/%d/%Y %H:%M", strict=False))
                .when(pl.col("end_time").str.strptime(pl.Datetime, "%m/%d/%Y %H:%M:%S", strict=False).is_not_null())
                    .then(pl.col("end_time").str.strptime(pl.Datetime, "%m/%d/%Y %H:%M:%S", strict=False))
                    .otherwise(pl.col("end_time").str.strptime(pl.Datetime, "%d/%m/%Y %H:%M", strict=False))
            .alias("end_time"),

            pl.col("duration").cast(pl.Int32)
        ])

        # If the start time and end time are both null, assume it's an invalid entry
        df = df.filter(pl.col("start_time").is_not_null() & pl.col("end_time").is_not_null())
        dfs.append(df)
    return pl.concat(dfs)



def build_trips(args):
    if not args.skip_unzip:
        extract_csvs()
    all_trips_df = create_all_trips_df()
    
    # Print all rows that have NULL in at least one column
    # df_missing = (
    #     all_trips_df
    #     .filter(
    #         pl.any_horizontal(pl.all().is_null())
    #     )
    # )    
    # print(df_missing)
    
    utils.create_all_trips_file(all_trips_df, args)
    utils.create_recent_year_file(all_trips_df, args)