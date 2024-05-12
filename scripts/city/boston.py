import pandas as pd
import utils
import polars as pl

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

final_columns = ["start_time", "stop_time", "start_station_name", "end_station_name", "start_station_id", "end_station_id"]

def get_applicable_columns_mapping(df, rename_dict):
    # Filter the rename dictionary to include only columns that exist in the DataFrame
    existing_columns = df.columns
    filtered_rename_dict = {old: new for old, new in rename_dict.items() if old in existing_columns}

    # Apply the renaming
    return filtered_rename_dict

def get_df_with_renamed_columns(trip_file):
    """Map columns of different csvs to consistent column names and return a DataFrame"""
    
    # Some columns like birth year have value \\N for some reason.
    # Polars will not read the csvs if it detects these values in what it deems an int column
    df = pl.read_csv(trip_file, null_values='\\N')
    headers = df.columns
    
    # Applicable columns needed because not all csvs (not even those pre/post 3/2023 can have slightly different columns)
    # Polars throws error if it finds a column it can't rename
    if "ride_id" in headers:
        applicable_renamed_columns = get_applicable_columns_mapping(df, renamed_columns_march_2023_and_beyond)
        df = df.rename(applicable_renamed_columns)
    else:
        applicable_renamed_columns = get_applicable_columns_mapping(df, renamed_columns_pre_march_2023)
        df = df.rename(applicable_renamed_columns)
    
    return df.select(final_columns)

def format_df_generate_parquet(trip_files, output_path):
    """Get correct column data structures"""
    file_dataframes = []
    for file in trip_files:
        df = get_df_with_renamed_columns(file)
        df = df.with_columns([
            # Need to remove fractional seconds for certain csv files
            pl.col("start_time").str.replace(r"\.\d+", "").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S", strict=False),
            pl.col("stop_time").str.replace(r"\.\d+", "").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S", strict=False),
            pl.col(["start_station_id", "end_station_id"]).cast(pl.Utf8)
        ])
        
        file_dataframes.append(df)

    print("concatenating all csv files...")
    
    all_trips_df = pl.concat(file_dataframes)
    all_trips_df.write_parquet(output_path)
    return all_trips_df

def build_all_trips(csv_source_directory, output_path):
    # Polars takes boaut 11 seconds for Boston. Was 60-70 seconds using pandas
    trip_files = utils.get_csv_files(csv_source_directory)
    format_df_generate_parquet(trip_files, output_path)


