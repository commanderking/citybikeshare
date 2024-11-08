import os
import polars as pl
import utils
import constants
import utils_bicycle_transit_systems
import logger


def filter_filenames(filenames, args):
    matching_words = constants.config[args.city]["file_matcher"]
    excluded_filenames = constants.config.get(args.city, {}).get(
        "excluded_filenames", []
    )
    # os.path.basename - Chicago files have a stations_and_trips folder, which creates a csv for stations. I don't want to include this stations csv in our checks, so filtering on just the filename not folder
    files = [
        filename
        for filename in filenames
        if any(word in os.path.basename(filename) for word in matching_words)
        ### NYC use case where csv files in 2018 can duplicated. We need to explicitly ignore the duplicates
        ### By filtering out their files
        and not any(
            partial_filename in filename for partial_filename in excluded_filenames
        )
    ]
    return files


def austin_check(args):
    def inner(df):
        if (args.city) == "austin":
            df = df.with_columns(
                [
                    pl.coalesce(
                        [
                            pl.col("start_time")
                            .str.replace(r"\.\d+", "")
                            .str.strptime(
                                pl.Datetime, "%m/%d/%Y %I:%M:%S %p", strict=True
                            )
                        ]
                    ),
                    pl.col("duration_minutes").cast(pl.Int32),
                ]
            )
            df = df.with_columns(
                (
                    pl.col("start_time")
                    + pl.duration(minutes=pl.col("duration_minutes"))
                ).alias("end_time")
            )
            return df
        return df

    return inner


def process_bicycle_transit_system(args):
    def inner(df):
        if args.city == "philadelphia" or args.city == "los_angeles":
            stations_df = utils_bicycle_transit_systems.stations_csv_to_df(args)
            df = utils_bicycle_transit_systems.append_station_names(
                df, stations_df
            ).drop("start_station_id", "end_station_id")
        return df

    return inner


def format_and_concat_files(trip_files, args):
    """Get correct column data structures"""
    mappings = constants.config[args.city]["column_mappings"]
    date_formats = constants.config[args.city]["date_formats"]

    print("adding files to polars df")
    file_dataframes = []

    for file in trip_files:
        print(file)
        # DEBUGGING TIPS
        # For debugging and printing tables with null data for a particular column after formatting
        # df_start_time = df.filter(pl.col("start_time").is_null())
        # print(df_start_time)
        df = (
            pl.read_csv(file, infer_schema_length=0)
            .pipe(utils.rename_columns(args, mappings))
            .pipe(logger.print_null_data_df)
            .pipe(utils.assess_null_data)
            ### TODO - move this to configuration for preprocessing. Austin doesn't have end_time so we need to calculate before casting times
            .pipe(austin_check(args))
            .pipe(
                utils.convert_columns_to_datetime(
                    ["start_time", "end_time"], date_formats
                )
            )
            .pipe(utils.offset_two_digit_years)
            # TODO: This station name mapping should apply to all stations
            # May want to make this configuration based rather than explicit city checks here
            .pipe(process_bicycle_transit_system(args))
        )
        file_dataframes.append(df)

    print("concatenating all csv files...")
    return pl.concat(file_dataframes)


def extract_zip_files(city):
    print(f"unzipping {city} trip files")

    def city_match(file_path, city):
        if city == "nyc":
            # JC files are duplicates of other files, but contain a more limited set of columns
            return "JC" not in file_path
        else:
            return any(
                word in file_path for word in constants.config[city]["file_matcher"]
            )

    utils.unzip_city_zips(city, city_match)


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
