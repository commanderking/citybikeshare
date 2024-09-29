import zipfile
import os
import polars as pl
import utils
import constants
import utils_bicycle_transit_systems

default_final_columns = constants.final_columns

def get_applicable_columns_mapping(df, rename_dict):
    # Filter the rename dictionary to include only columns that exist in the DataFrame
    existing_columns = df.columns
    filtered_rename_dict = {old: new for old, new in rename_dict.items() if old in existing_columns}

    return filtered_rename_dict

def rename_columns(df, args):
    city = args.city
    mappings = constants.config[city]['column_mappings']
    headers = df.columns
    applicable_renamed_columns = []
    
    # TODO: This should be more robust - theoretically, multiple column mappings could match and the 
    # last match would be the mapping used
    for mapping in mappings:
        if (mapping["header_matcher"] in headers):
            applicable_renamed_columns = get_applicable_columns_mapping(df, mapping["mapping"])
            final_columns = mapping.get("final_columns", default_final_columns)
            renamed_df = df.rename(applicable_renamed_columns).select(final_columns)
            return renamed_df
    raise ValueError(f'We could not rename the columns because no valid column mappings for {city} match the data! The headers we found are: {df.columns}')

def format_and_concat_files(trip_files, args):
    """Get correct column data structures"""
    
    print("adding files to polars df")
    file_dataframes = []
    
    for file in trip_files:
        print(file)

        date_formats = [
            "%Y-%m-%d %H:%M:%S",
            "%m/%d/%Y %H:%M:%S",
            "%m/%d/%Y %H:%M",
            "%Y-%m-%d %H:%M", # Chicago - Divvy_Trips_2013
            '%Y-%m-%dT%H:%M:%S', # Pittsburgh
            '%a, %b %d, %Y, %I:%M %p', #Pittsburgh one file - 8e8a5cd9-943e-4d21-a7ed-05f865dd0038 (data-id), April 2023,
            "%m/%d/%Y %I:%M:%S %p" # Austin, TX BCycle
        ]
        # TODO: Some columns like birth year have value \\N. Map \\N to correct values
        df = pl.read_csv(file, infer_schema_length=0)
        # For debugging columns that have missing data
        utils.assess_null_data(df)
        df = rename_columns(df ,args)

        ### TODO - move this to configuration for preprocessing. Austin doesn't have end_time so we need to calculate before casting times
        if (args.city) == "austin":
            df = df.with_columns([
                pl.coalesce(
                    [pl.col("start_time").str.replace(r"\.\d+", "").str.strptime(pl.Datetime, "%m/%d/%Y %I:%M:%S %p", strict=True)]
                ),
                pl.col("duration_minutes").cast(pl.Int32)
            ])
            df = df.with_columns(
                (pl.col("start_time") + pl.duration(minutes=pl.col("duration_minutes"))).alias("end_time"))

        df = df.with_columns([
            # Replace . and everything that follows with empty string. Some Boston dates have milliseconds
            pl.coalesce([pl.col("start_time").str.replace(r"\.\d+", "").str.strptime(pl.Datetime, format, strict=False) for format in date_formats]),
            pl.coalesce([pl.col("end_time").str.replace(r"\.\d+", "").str.strptime(pl.Datetime, format, strict=False) for format in date_formats]),
        ])
        
        df = df.with_columns([
            pl.when(pl.col('start_time').dt.year() < 100)
                .then(pl.col('start_time').dt.offset_by('2000y'))
                .otherwise(pl.col('start_time'))
                .alias('start_time'),
            pl.when(pl.col('end_time').dt.year() < 100)
                .then(pl.col('end_time').dt.offset_by('2000y'))
                .otherwise(pl.col('end_time'))
                .alias('end_time'),
        ])

        # For debugging and printing tables with null data for a particular column after formatting
        # df_start_time = df.filter(pl.col("start_time").is_null())
        # print(df_start_time)

        # TODO: This station name mapping should apply to all stations
        # May want to make this configuration based rather than explicit city checks here
        if (args.city == "philadelphia" or args.city == "los_angeles"):
            stations_df = utils_bicycle_transit_systems.stations_csv_to_df(args)
            df = utils_bicycle_transit_systems.append_station_names(df, stations_df).drop("start_station_id", "end_station_id")      

        file_dataframes.append(df)

    print("concatenating all csv files...")
    
    all_trips_df = pl.concat(file_dataframes)
    return all_trips_df

def extract_zip_files(city):
    print(f'unzipping {city} trip files')    
    def city_match(file_path, city):
        if city == "nyc":
            # JC files are duplicates of other files, but contain a more limited set of columns
            return "JC" not in file_path
        else:
            return any(word in file_path for word in constants.config[city]['file_matcher']) 
        
    utils.unzip_city_zips(city, city_match)       

def filter_filenames(filenames, args):
    matching_words = constants.config[args.city]['file_matcher']
    excluded_filenames = constants.config.get(args.city, {}).get('excluded_filenames', [])
    # os.path.basename - Chicago files have a stations_and_trips folder, which creates a csv for stations. I don't want to include this stations csv in our checks, so filtering on just the filename not folder
    files = [filename for filename in filenames 
        if any(word in os.path.basename(filename) for word in matching_words)
        ### NYC use case where csv files in 2018 can duplicated. We need to explicitly ignore the duplicates
        ### By filtering out their files
        and not any(partial_filename in filename for partial_filename in excluded_filenames)
    ]    
    return files


def build_all_trips(args):
    source_directory = utils.get_raw_files_directory(args.city)

    if args.skip_unzip is False:
        extract_zip_files(args.city)
    else:
        print("skipping unzipping files")
    trip_files = utils.get_csv_files(source_directory)
    filtered_files = filter_filenames(trip_files, args)
    all_trips_df = format_and_concat_files(filtered_files, args)
    
    utils.create_all_trips_file(all_trips_df, args)
    utils.create_recent_year_file(all_trips_df, args)
    
    utils.log_final_results(all_trips_df, args)
