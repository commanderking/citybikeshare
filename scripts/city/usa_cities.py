import zipfile
import os
import polars as pl
import utils


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

chicago_renamed_columns_pre_march_2023 = {
    "starttime": "start_time",
    "stoptime": "end_time",
    "from_station_id": "start_station_id",
    "from_station_name": "start_station_name",
    "to_station_id": "end_station_id",
    "to_station_name": "end_station_name",
    "usertype": "usertype",
    "birth year": "birth_year",
    "gender": "gender",
}

# 2018_Q1 2019_Q2 - maybe others
chicago_renamed_columns_oddball = {
    "01 - Rental Details Local Start Time": "start_time",
    "01 - Rental Details Local End Time": "end_time",
    "03 - Rental Start Station ID": "start_station_id",
    "03 - Rental Start Station Name": "start_station_name",
    "02 - Rental End Station ID": "end_station_id",
    "02 - Rental End Station Name": "end_station_name",
    "User Type": "usertype",
    "Member Gender": "gender",
    "05 - Member Details Member Birthday Year": "birth_year"
}


chicago_renamed_columns_2021_Q1_and_beyond = {
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

nyc_renamed_columns_initial = {
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
    "Gender": "gender",
    "postal code": "postal_code"
}

nyc_renamed_columns_2017_03_to_2020_01 = {
    "Trip Duration" : "duration",
    "Start Time": "start_time",
    "Stop Time": "end_time",
    "Start Station ID": "start_station_id",
    "Start Station Name" :"start_station_name",
    "Start Station Latitude" : "start_station_latitude",
    "Start Station Longitude": "start_station_longitude",
    "End Station ID": "end_station_id",
    "End Station Name": "end_station_name",
    "End Station Latitude": "end_station_latitude",
    "End Station Longitude": "end_station_longitude",
    "Bike ID": "bike_id",
    "User Type": "usertype",
    "Birth Year": "birth_year",
    "gender": "gender"
    
}

nyc_renamed_columns_2021_01_and_beyond = {
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

city_file_matcher = {
    "boston": ["-tripdata"],
    "nyc": ["citibike-tripdata"],
    "dc": ["capitalbikeshare-tripdata"],
    "chicago": ["trip", "Trips"]
}

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

def rename_chicago_columns(df):
    headers = df.columns
    
    # Applicable columns needed because not all csvs (not even those pre/post 3/2023 can have slightly different columns)
    # Polars throws error if it finds a column it can't rename
    if "ride_id" in headers:
        applicable_renamed_columns = get_applicable_columns_mapping(df, chicago_renamed_columns_2021_Q1_and_beyond)
        df = df.rename(applicable_renamed_columns)
    elif "01 - Rental Details Local Start Time" in headers:
        applicable_renamed_columns = get_applicable_columns_mapping(df, chicago_renamed_columns_oddball)
        df = df.rename(applicable_renamed_columns)
    else:
        applicable_renamed_columns = get_applicable_columns_mapping(df, chicago_renamed_columns_pre_march_2023)
        df = df.rename(applicable_renamed_columns)
    
    return df.select(final_columns)

def rename_nyc_columns(df):
    headers = df.columns
        
    if "ride_id" in headers:
        applicable_renamed_columns = get_applicable_columns_mapping(df, nyc_renamed_columns_2021_01_and_beyond)
        df = df.rename(applicable_renamed_columns)
    elif "Trip Duration" in headers:
        print("in trip duration")
        applicable_renamed_columns = get_applicable_columns_mapping(df, nyc_renamed_columns_2017_03_to_2020_01)
        df = df.rename(applicable_renamed_columns)
    else:
        applicable_renamed_columns = get_applicable_columns_mapping(df, nyc_renamed_columns_initial)
        df = df.rename(applicable_renamed_columns)
    
    return df.select(final_columns)
    
    

def format_and_concat_files(trip_files, rename_df_columns):
    """Get correct column data structures"""
    
    print("adding files to polars df")
    file_dataframes = []
    for file in trip_files:
        print(file)
        # Some columns like birth year have value \\N.
        # TODO: Map \\N to correct values
        df = pl.read_csv(file, infer_schema_length=0)
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
    city_zip_directory = utils.get_zip_directory(city)
    
    def city_match(file_path, city):
        if city == "nyc":
            # JC files are duplicates of other files, but contain a more limited set of columns
            return "JC" not in file_path
        else: 
            return any(word in file_path for word in city_file_matcher[city])        

    for file in os.listdir(city_zip_directory):
        file_path = os.path.join(city_zip_directory, file)
        if (zipfile.is_zipfile(file_path) and city_match(file_path, city)):
            with zipfile.ZipFile(file_path, mode="r") as archive:
                archive.extractall(utils.get_raw_files_directory(city))

def filter_filenames(filenames, matching_words):
    return [filename for filename in filenames if any(word in filename for word in matching_words)]

def build_all_trips(args, rename_columns):
    source_directory = utils.get_raw_files_directory(args.city)

    if args.skip_unzip is False:
        extract_zip_files(args.city)
    else:
        print("skipping unzipping files")
    trip_files = utils.get_csv_files(source_directory)
    filtered_files = filter_filenames(trip_files, city_file_matcher[args.city])
    all_trips_df = format_and_concat_files(filtered_files, rename_columns)
    
    utils.create_all_trips_file(all_trips_df, args)
    utils.create_recent_year_file(all_trips_df, args)
