import pandas as pd
import sqlite3, zipfile, os

sqlite_db = "./data/blue_bikes.db"
all_trips ="./data/all_trips.csv"
zip_directory = "./data/bluebikeData"
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

def format_data():
    for file in os.listdir(zip_directory):
        file_path = os.path.join(zip_directory, file)
        if (zipfile.is_zipfile(file_path) and "bluebikes-tripdata" in file):
            with zipfile.ZipFile(file_path, mode="r") as archive:
                archive.printdir()
                archive.extractall(csv_directory)

def insert_to_pandas():
    trip_files = []
    for file in os.listdir(csv_directory):
        if (file.endswith(".csv")):
            csv_path = os.path.join(csv_directory, file)
            trip_files.append(csv_path)

    # Still two issues
    # 1) birth_year and gender are floats instead of int. This is due to NaN present
    # 2) indexes reset with each frame even when using ignore_index option
    df = pd.concat(map(pd.read_csv, trip_files))
    df.rename(columns=renamed_columns, inplace=True)

    connection = sqlite3.connect(sqlite_db)

    with connection:
        df.to_sql(name="bike_trip", con=connection)

    df.to_csv(all_trips, index=True, header=True)



__name__
# format_data()
insert_to_pandas()