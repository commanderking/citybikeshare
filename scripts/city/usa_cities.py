import zipfile
import os
import polars as pl
import utils
import constants

final_columns = constants.final_columns

city_file_matcher = {
    "boston": ["-tripdata"],
    "nyc": ["citibike-tripdata"],
    "dc": ["capitalbikeshare-tripdata"],
    "chicago": ["trip", "Trips"],
    "sf": ["tripdata"]
}

def get_applicable_columns_mapping(df, rename_dict):
    # Filter the rename dictionary to include only columns that exist in the DataFrame
    existing_columns = df.columns
    filtered_rename_dict = {old: new for old, new in rename_dict.items() if old in existing_columns}

    return filtered_rename_dict

def rename_columns(df, args):
    city = args.city
    mappings = constants.column_mapping[city]
    headers = df.columns
    applicable_renamed_columns = []
    
    # TODO: This should be more robust - theoretically, multiple column mappings could match and the 
    # last match would be the mapping used
    for mapping in mappings:
        if (mapping["header_matcher"] in headers):
            applicable_renamed_columns = get_applicable_columns_mapping(df, mapping["column_mapping"])
    return df.rename(applicable_renamed_columns).select(final_columns)


def format_and_concat_files(trip_files, args):
    """Get correct column data structures"""
    
    print("adding files to polars df")
    file_dataframes = []
    for file in trip_files:
        print(file)
        # Some columns like birth year have value \\N.
        # TODO: Map \\N to correct values
        df = pl.read_csv(file, infer_schema_length=0)
        df = rename_columns(df ,args)
        df = df.with_columns([
            # Need to remove fractional seconds for certain csv files
            pl.col("start_time").str.replace(r"\.\d+", "").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S", strict=False),
            pl.col("end_time").str.replace(r"\.\d+", "").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S", strict=False),
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
    # os.path.basename - Chicago files have a stations_and_trips folder, which creates a csv for stations. I don't want to include this stations csv in our checks, so filtering on just the filename not folder
    return [filename for filename in filenames if any(word in os.path.basename(filename) for word in matching_words)]

def build_all_trips(args):
    source_directory = utils.get_raw_files_directory(args.city)

    if args.skip_unzip is False:
        extract_zip_files(args.city)
    else:
        print("skipping unzipping files")
    trip_files = utils.get_csv_files(source_directory)
    filtered_files = filter_filenames(trip_files, city_file_matcher[args.city])
    all_trips_df = format_and_concat_files(filtered_files, args)
    
    utils.create_all_trips_file(all_trips_df, args)
    utils.create_recent_year_file(all_trips_df, args)
