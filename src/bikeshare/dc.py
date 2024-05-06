import os
import pandas as pd
import utils
renamed_columns_pre_may_2020 = {
    "Duration": "duration",
    "Start date": "start_time",
    "End date": "end_time",
    "Start station number": "start_station_id",
    "Start station": "start_station_name",
    "End station number": "end_station_id",
    "End station": "end_station_name",
    "Bike number": "bike_number",
    "Member type": "member_type",
}

renamed_columns_may_2020_and_beyond = {
    "ride_id": "ride_id",
    "rideable_type": "rideable_type",	
    "started_at": "start_time",	
    "ended_at": "end_time",	
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
    ### ride_id column is only available starting may 2020
    if ("ride_id" in headers):
        df.rename(columns=renamed_columns_may_2020_and_beyond, inplace=True)
        return df
    else:
        ### trip_duration no longer provided in after may 2020 - removing to avoid confusion with new columns not having this
        df.drop(["Duration"], axis=1, inplace=True)
        df.rename(columns=renamed_columns_pre_may_2020, inplace=True)
        return df
    

def create_formatted_df(trip_files, output_path):

    file_dataframes = []
    for file in trip_files:
        df = get_df_with_correct_columns(file)
        file_dataframes.append(df)


    print("processing all csv files...")

    all_trips_df = pd.concat(file_dataframes, join='outer', ignore_index=True)

    all_trips_df[["start_time", "end_time"]] = all_trips_df[["start_time", "end_time"]].astype("datetime64[ns]")
    all_trips_df[["start_station_id", "end_station_id"]] = all_trips_df[["start_station_id", "end_station_id"]].astype("str")

    utils.create_file(all_trips_df, output_path)    

def build_all_trips(csv_source_directory, output_path):
    trip_files = utils.get_csv_files(csv_source_directory)
    create_formatted_df(trip_files, output_path)
