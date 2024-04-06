import os
import pandas as pd

### This applies to bluebike rides up to and including March, 2023
### After this date, the bluebike shape changed :(
renamed_columns_pre_march_2023 = {
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



def get_csv_files(directory):
    ### takes directory where to find csv files and returns as a list
    trip_files = []
    for file in os.listdir(directory):
        if (file.endswith(".csv")):
            csv_path = os.path.join(directory, file)
            trip_files.append(csv_path)

    return trip_files

def get_df_with_correct_columns(trip_file):
    df = pd.read_csv(trip_file)
    headers = list(df)
    ### ride_id is column in the post march 2023 data
    if ("ride_id" in headers):
        print("new file")
        df.rename(columns=renamed_columns_march_2023_and_beyond, inplace=True)
        return df
    else:
        print ("old")

        df.rename(columns=renamed_columns_pre_march_2023, inplace=True)

        return df
    

def create_formatted_df(trip_files):

    file_dataframes = []
    for file in trip_files:
        df = get_df_with_correct_columns(file)
        file_dataframes.append(df)


    print("reading all csv files...")
    # df = pd.concat(map(pd.read_csv, trip_files), ignore_index=True)
    # print(list(df))
    # df.rename(columns=renamed_columns, inplace=True)
    # print(list(df))

    all_trips_df = pd.concat(file_dataframes, join='outer', ignore_index=True)
            ### trip_duration no longer provided in post march 2023 ones - removing to avoid confusion with new columns not having this
    all_trips_df.drop(["trip_duration"], axis=1, inplace=True)

    print(list(all_trips_df))

    # Beacuse of NaN in data, birth_year and gender are floats. Converting to Int64 allows for <NA> type in integer column
    all_trips_df[["birth_year", "gender"]] = all_trips_df[["birth_year", "gender"]].astype("Int64")
    all_trips_df[["start_time", "stop_time"]] = all_trips_df[["start_time", "stop_time"]].astype("datetime64[ns]")
    all_trips_df[["start_station_id", "end_station_id"]] = all_trips_df[["start_station_id", "end_station_id"]].astype("str")

    return all_trips_df
    