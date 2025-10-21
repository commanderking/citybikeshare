import os
import hashlib
import polars as pl
import utils
import utils_dolt
import utils_bicycle_transit_systems
import scripts.constants as constants


def compute_file_hash(filepath, chunk_size=8192):
    """Compute SHA256 hash for file integrity checking."""
    hasher = hashlib.sha256()
    with open(filepath, "rb") as f:
        while chunk := f.read(chunk_size):
            hasher.update(chunk)
    return hasher.hexdigest()


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


def austin_check(df, args):
    if (args.city) == "austin":
        df = df.with_columns(
            [
                pl.coalesce(
                    [
                        pl.col("start_time")
                        .str.replace(r"\.\d+", "")
                        .str.strptime(pl.Datetime, "%m/%d/%Y %I:%M:%S %p", strict=True)
                    ]
                ),
                pl.col("duration_minutes").cast(pl.Int32),
            ]
        )
        df = df.with_columns(
            (
                pl.col("start_time") + pl.duration(minutes=pl.col("duration_minutes"))
            ).alias("end_time")
        )
        return df
    return df


def process_bicycle_transit_system(df, args):
    stations_df = utils_bicycle_transit_systems.stations_csv_to_df(args)
    df = utils_bicycle_transit_systems.append_station_names(df, stations_df).drop(
        "start_station_id", "end_station_id"
    )
    print(df)
    return df


def get_file_metadata(filepath, city_config):
    """Get file size and last modified timestamp."""
    stat = os.stat(filepath)
    size = stat.st_size
    modified_at = stat.st_mtime
    name = os.path.basename(filepath)
    file_hash = compute_file_hash(filepath)

    return {
        "size": size,
        "modified_at": modified_at,
        "name": name,
        "file_hash": file_hash,
        "system_name": city_config["system_name"],
    }


def convert_milliseconds_to_datetime(df):
    headers = df.columns
    print(headers)
    ### most recent Montreal data notes start time and end time in ms whereas previous versions used a date.
    if "start_ms" in headers:
        df = df.with_columns(
            # start_ms auto converts to string instead of integer - cast before converting to datetime
            [pl.col("start_ms").cast(pl.Int64), pl.col("end_ms").cast(pl.Int64)]
        ).with_columns(
            [
                pl.from_epoch("start_ms", time_unit="ms").alias("start_time"),
                pl.from_epoch("end_ms", time_unit="ms").alias("end_time"),
            ]
        )
    return df


def filter_null_rows(df):
    return df.filter(~pl.all_horizontal(pl.all().is_null()))


PROCESSING_FUNCTIONS = {
    "rename_columns": lambda df, ctx: df.pipe(
        utils.rename_columns_for_keys(ctx["renamed_columns"])
    ),
    "convert_to_datetime": lambda df, ctx: df.pipe(
        utils.convert_columns_to_datetime(
            ["start_time", "end_time"], ctx["date_formats"]
        )
    ),
    "select_final_columns": lambda df, ctx: df.select(constants.final_columns),
    "offset_two_digit_years": lambda df, ctx: utils.offset_two_digit_years(df),
    "austin_calculate_end_time": lambda df, ctx: austin_check(df, ctx["args"]),
    "convert_milliseconds_to_datetime": lambda df,
    ctx: convert_milliseconds_to_datetime(df),
    # Philadelphia and Los Angeles
    "process_bicycle_transit_stations": lambda df, ctx: process_bicycle_transit_system(
        df, ctx["args"]
    ),
    "filter_null_rows": lambda df, ctx: filter_null_rows(df),
}


# def create_trip_df(file, args):
#     city_config = constants.config[args.city]
#     date_formats = city_config["date_formats"]
#     renamed_columns = city_config["renamed_columns"]
#     final_columns = city_config.get("final_columns", constants.final_columns)

#     df_lazy = (
#         pl.scan_csv(file, infer_schema_length=0)
#         .pipe(utils.rename_columns_for_keys(renamed_columns))
#         # TODO: This station name mapping should apply to all stations
#         # May want to make this configuration based rather than explicit city checks here
#         .pipe(process_bicycle_transit_system(args))
#         # For debugging
#         # .pipe(utils.print_null_data)
#         # .pipe(utils.assess_null_data)
#         ### TODO - move this to configuration for preprocessing. Austin doesn't have end_time so we need to calculate before casting times
#         .pipe(austin_check(args))
#         .pipe(
#             utils.convert_columns_to_datetime(["start_time", "end_time"], date_formats)
#         )
#         .select(final_columns)
#         .pipe(utils.offset_two_digit_years)
#     )

#     return df_lazy


def create_trip_df(file, args):
    config = constants.config[args.city]
    read_csv_options = config.get("read_csv_options", {})
    df = pl.scan_csv(file, infer_schema_length=0, **read_csv_options)
    context = {**config, "args": args}

    for step in config.get(
        "processing_pipeline", constants.DEFAULT_PROCESSING_PIPELINE
    ):
        fn = PROCESSING_FUNCTIONS[step]
        df = fn(df, context)

    return df


def add_trips_to_db(files, args):
    engine = utils_dolt.establish_engine()
    for file in files:
        city_config = constants.config[args.city]
        file_metadata = get_file_metadata(file, city_config)
        file_processed = utils_dolt.is_file_processed(engine, file_metadata)
        file_name = file_metadata["name"]
        if file_processed:
            print(f"🟡 Skipping {file_name} - already processed")
        else:
            print(f"🐢 processing {file_name}")

            # DEBUGGING TIPS
            # For debugging and printing tables with null data for a particular column after formatting
            # df_start_time = df.filter(pl.col("start_time").is_null())
            # print(df_start_time)
            df_lazy = create_trip_df(file, args)

            utils_dolt.insert_trip_data(engine, df_lazy, file_metadata)


def get_dfs_for_parquet(files, args):
    file_dataframes = []
    for file in files:
        df_lazy = create_trip_df(file, args)
        file_dataframes.append(df_lazy)

    return pl.concat(file_dataframes)


def extract_zip_files(city):
    print(f"unzipping {city} trip files")

    def city_match(file_path, city):
        if city == "new_york_city":
            # JC are for Jersey City, we don't wait to consume those for new york city
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

    ## Adding to parquet path
    if args.parquet:
        all_trips_df_lazy = get_dfs_for_parquet(filtered_files, args)
        utils.create_final_files_and_logs(all_trips_df_lazy, args)

    ## Adding to doltdb path
    else:
        add_trips_to_db(filtered_files, args)
