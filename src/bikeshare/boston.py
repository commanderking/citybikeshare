import os
import pandas as pd
import utils
renamed_columns_pre_march_2023 = {
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

renamed_columns_march_2023_and_beyond = {
    "ride_id": "ride_id",
    "rideable_type": "rideable_type",	
    "started_at": "start_time",	
    "ended_at": "stop_time",	
    "start_station_name": "start_station_name",
    "start_station_id": "start_station_id",	
    "end_station_name": "end_station_name",	
    "end_station_id": "end_station_id", 
    "start_lat": "start_station_latitude",	
    "start_lng": "start_station_longitude",
    "end_lat": "end_station_latitude",	
    "end_lng": "end_station_longitude",	
    "member_casual": "member_casual"
}

def get_df_with_correct_columns(trip_file):
    df = pd.read_csv(trip_file)
    headers = list(df)
    ### ride_id is column only available starting march 2023 - denotes new headers are used
    if ("ride_id" in headers):
        df.rename(columns=renamed_columns_march_2023_and_beyond, inplace=True)
        return df
    else:
        ### trip_duration no longer provided in post march 2023 ones - removing to avoid confusion with new columns not having this
        df.drop(["tripduration"], axis=1, inplace=True)
        df.rename(columns=renamed_columns_pre_march_2023, inplace=True)
        return df
    

def create_formatted_df(trip_files, output_path):
    file_dataframes = []
    for file in trip_files:
        df = get_df_with_correct_columns(file)
        file_dataframes.append(df)

    print("concatenating all csv files...")

    all_trips_df = pd.concat(file_dataframes, join='outer', ignore_index=True)

    # Beacuse of NaN in data, birth_year and gender are floats. Converting to Int64 allows for <NA> type in integer column
    all_trips_df[["birth_year", "gender"]] = all_trips_df[["birth_year", "gender"]].astype("Int64")
    all_trips_df[["start_time", "stop_time"]] = all_trips_df[["start_time", "stop_time"]].astype("datetime64[ns]")
    all_trips_df[["start_station_id", "end_station_id"]] = all_trips_df[["start_station_id", "end_station_id"]].astype("str")

    utils.create_file(all_trips_df, output_path)

def build_all_trips(csv_source_directory, output_path):
    trip_files = utils.get_csv_files(csv_source_directory)
    create_formatted_df(trip_files, output_path)
