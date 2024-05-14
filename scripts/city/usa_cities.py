import utils
import polars as pl
import zipfile
import os

boston_renamed_columns_pre_march_2023 = {
    "starttime": "start_time",
    "stoptime": "end_time",
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

boston_renamed_columns_march_2023_and_beyond = {
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

dc_renamed_columns_pre_may_2020 = {
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

dc_renamed_columns_may_2020_and_beyond = {
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


final_columns = ["start_time", "end_time", "start_station_name", "end_station_name", "start_station_id", "end_station_id"]

def get_applicable_columns_mapping(df, rename_dict):
    # Filter the rename dictionary to include only columns that exist in the DataFrame
    existing_columns = df.columns
    filtered_rename_dict = {old: new for old, new in rename_dict.items() if old in existing_columns}

    return filtered_rename_dict

def rename_boston_columns(df):
    """Map columns of different csvs to consistent column names and return a DataFrame"""
    headers = df.columns
    
    # Applicable columns needed because not all csvs (not even those pre/post 3/2023 can have slightly different columns)
    # Polars throws error if it finds a column it can't rename
    if "ride_id" in headers:
        applicable_renamed_columns = get_applicable_columns_mapping(df, boston_renamed_columns_march_2023_and_beyond)
        df = df.rename(applicable_renamed_columns)
    else:
        applicable_renamed_columns = get_applicable_columns_mapping(df, boston_renamed_columns_pre_march_2023)
        df = df.rename(applicable_renamed_columns)
    
    return df.select(final_columns)

def rename_dc_columns(df):
    headers = df.columns
    
    # Applicable columns needed because not all csvs (not even those pre/post 3/2023 can have slightly different columns)
    # Polars throws error if it finds a column it can't rename
    if "ride_id" in headers:
        applicable_renamed_columns = get_applicable_columns_mapping(df, dc_renamed_columns_may_2020_and_beyond)
        df = df.rename(applicable_renamed_columns)
    else:
        applicable_renamed_columns = get_applicable_columns_mapping(df, dc_renamed_columns_pre_may_2020)
        df = df.rename(applicable_renamed_columns)
    
    return df.select(final_columns)

def format_and_concat_files(trip_files, rename_df_columns):
    """Get correct column data structures"""
    
    print("adding files to polars df")
    file_dataframes = []
    for file in trip_files:
        print(file)
        # Some columns like birth year have value \\N for some reason.
        # Polars will not read the csvs if it detects these values in what it deems an int column
        # DC data has MTL-ECO5-03 in 202102 :(
        df = pl.read_csv(file, null_values=['\\N', 'MTL-ECO5-03'])
        df = rename_df_columns(df)
        df = df.with_columns([
            # Need to remove fractional seconds for certain csv files
            pl.col("start_time").str.replace(r"\.\d+", "").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S", strict=False),
            pl.col("end_time").str.replace(r"\.\d+", "").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S", strict=False),
            pl.col(["start_station_id", "end_station_id"]).cast(pl.Utf8)
        ])
        
        file_dataframes.append(df)

    print("concatenating all csv files...")
    
    all_trips_df = pl.concat(file_dataframes)
    return all_trips_df

def extract_zip_files(city):
    print(f'unzipping {city} trip files')
    city_file_matcher = {
        "boston": "-tripdata.zip",
        "NYC": "citibike-tripdata",
        "dc": "capitalbikeshare-tripdata.zip",
        "Chicago": "divvy-tripdata"
    }

    city_zip_directory = utils.get_zip_directory(city)

    for file in os.listdir(city_zip_directory):
        file_path = os.path.join(city_zip_directory, file)
        if (zipfile.is_zipfile(file_path) and city_file_matcher[city] in file):
            with zipfile.ZipFile(file_path, mode="r") as archive:
                archive.extractall(utils.get_raw_files_directory(city))


def build_all_trips(args, csv_source_directory, output_path, rename_columns):
    if args.skip_unzip is False:
        extract_zip_files(args.city)
    else:
        print("skipping unzipping files")
    trip_files = utils.get_csv_files(csv_source_directory)
    all_trips_df = format_and_concat_files(trip_files, rename_columns)
    
    utils.create_file(all_trips_df, output_path)
    