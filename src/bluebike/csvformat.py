import os
import pandas as pd

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

def get_csv_files(directory):
    ### takes directory where to find csv files and returns as a list
    trip_files = []
    for file in os.listdir(directory):
        if (file.endswith(".csv")):
            csv_path = os.path.join(directory, file)
            trip_files.append(csv_path)

    return trip_files


def create_formatted_df(trip_files):
    print("reading all csv files...")
    df = pd.concat(map(pd.read_csv, trip_files), ignore_index=True)
    df.rename(columns=renamed_columns, inplace=True)

    # Beacuse of NaN in data, birth_year and gender are floats. Converting to Int64 allows for <NA> type in integer column
    df[["birth_year", "gender"]] = df[["birth_year", "gender"]].astype("Int64")

    return df