import argparse
import os
import sqlite3
import zipfile
import pandas as pd

SQLITE_DB = "./data/bluebikes.db"
ALL_TRIPS ="./data/all_trips.csv"
RAW_BLUEBIKE_ZIP_DIRECTORY = "./data/bluebikeData"
CSV_DIRECTORY = "./data/monthlyTripCsvs"

renamed_columns = {
    "tripduration" : "trip_duration",
    "starttime": "start_time",
    "stoptime": "stop_time",
    "start station id": "start_station_id",
    "start station name": "start_station_name",
    "start station latitude": "start_station_latitude",
    "start station longitude": "start_station_longitude",
    "end station id": "end_station_id",
    "end station name": "end_station_name",
    "end station latitude": "end_station_latitude",
    "end station longitude": "end_station_longitude",
    "bikeid": "bike_id",
    "usertype": "usertype",
    "birth year": "birth_year",
    "gender": "gender",
    "postal code": "postal_code"
}

def setup_argparse():
    parser = argparse.ArgumentParser(description='Merging all Bike Trip Data into One File')
    parser.add_argument(
        '--csv',
        help='Output merged bike trip data into csv file only',
        action='store_true'
    )
    parser.add_argument(
        '--sqlite',
        help='Output merged bike trip data into sqlite file only',
        action='store_true'
    )
    parser.add_argument(
        '--skip_unzip',
        help='Skips unzipping of files if files have already been unzipped',
        action='store_true'
    )

    args = parser.parse_args()
    return args

def extract_zip_files():
    print('unzipping bluebike trip files')
    for file in os.listdir(RAW_BLUEBIKE_ZIP_DIRECTORY):
        file_path = os.path.join(RAW_BLUEBIKE_ZIP_DIRECTORY, file)
        if (zipfile.is_zipfile(file_path) and "bluebikes-tripdata" in file):
            with zipfile.ZipFile(file_path, mode="r") as archive:
                archive.extractall(CSV_DIRECTORY)

def export_data(args):
    output_csv = args.csv
    output_sqlite = args.sqlite

    if output_csv is False and output_sqlite is False:
        output_csv = True
        output_sqlite = True

    trip_files = []
    for file in os.listdir(CSV_DIRECTORY):
        if (file.endswith(".csv")):
            csv_path = os.path.join(CSV_DIRECTORY, file)
            trip_files.append(csv_path)

    print("reading all csv files...")
    df = pd.concat(map(pd.read_csv, trip_files), ignore_index=True)
    df.rename(columns=renamed_columns, inplace=True)

    # Beacuse of NaN in data, birth_year and gender are floats. Converting to Int64 allows for <NA> type in integer column
    df[["birth_year", "gender"]] = df[["birth_year", "gender"]].astype("Int64")

    if output_sqlite:
        print("generating sqlite db... this will take a bit...")
        connection = sqlite3.connect(SQLITE_DB)
        with connection:
            df.to_sql(name="bike_trip", con=connection, if_exists="replace")

    if output_csv:
        print ("generating csv...this will take a bit...")
        df.to_csv(ALL_TRIPS, index=True, header=True)

def merge_data():
    args = setup_argparse()
    if args.skip_unzip is False:
        extract_zip_files()
    export_data(args)

merge_data()
