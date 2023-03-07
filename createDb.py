import pandas as pd
import sqlite3, zipfile, os
import argparse

sqlite_db = "./data/bluebikes.db"
all_trips ="./data/all_trips.csv"
raw_bluebike_zip_directory = "./data/bluebikeData"
csv_directory = "./data/monthlyTripCsvs"

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
    for file in os.listdir(raw_bluebike_zip_directory):
        file_path = os.path.join(raw_bluebike_zip_directory, file)
        if (zipfile.is_zipfile(file_path) and "bluebikes-tripdata" in file):
            with zipfile.ZipFile(file_path, mode="r") as archive:
                archive.extractall(csv_directory)

def export_data(args):
    output_csv = args.csv
    output_sqlite = args.sqlite

    if output_csv == False and output_sqlite == False:
        output_csv = True
        output_sqlite = True

    trip_files = []
    for file in os.listdir(csv_directory):
        if (file.endswith(".csv")):
            csv_path = os.path.join(csv_directory, file)
            trip_files.append(csv_path)

    print("reading all csv files...")
    df = pd.concat(map(pd.read_csv, trip_files), ignore_index=True)
    df.rename(columns=renamed_columns, inplace=True)

    # Beacuse of NaN in data, birth_year and gender are floats. Converting to Int64 allows for <NA> type in integer column
    df[["birth_year", "gender"]] = df[["birth_year", "gender"]].astype("Int64")

    if output_sqlite:
        print("generating sqlite db... this will take a bit...")
        connection = sqlite3.connect(sqlite_db)
        with connection:
            df.to_sql(name="bike_trip", con=connection, if_exists="replace")

    if output_csv:
        print ("generating csv...this will take a bit...")
        df.to_csv(all_trips, index=True, header=True)

def merge_data():
    args = setup_argparse()
    if args.skip_unzip == False:
        extract_zip_files()
    export_data(args)

__name__
merge_data()
