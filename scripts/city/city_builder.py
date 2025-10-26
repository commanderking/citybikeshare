import os
import hashlib
import polars as pl
import utils
import shutil
import utils_dolt
import utils_bicycle_transit_systems
import scripts.constants as constants
from src.citybikeshare.config.loader import load_city_config
from pathlib import Path


def compute_file_hash(filepath, chunk_size=8192):
    """Compute SHA256 hash for file integrity checking."""
    hasher = hashlib.sha256()
    with open(filepath, "rb") as f:
        while chunk := f.read(chunk_size):
            hasher.update(chunk)
    return hasher.hexdigest()


def filter_filenames(filenames, config):
    matching_words = config.get("file_matcher")
    excluded_filenames = config.get("excluded_filenames", [])

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


def austin_check(df, context):
    """
    Parse Austin start_time using all available date_formats,
    ensure duration_minutes is numeric, and compute end_time.
    """
    date_formats = context.get("date_formats", ["%m/%d/%Y %I:%M:%S %p"])

    # Have consistent start time
    df = df.with_columns(
        [
            pl.coalesce(
                [
                    pl.col("start_time")
                    .str.replace(r"\.\d+", "")  # strip decimals
                    .str.strptime(pl.Datetime, fmt, strict=False)
                    for fmt in date_formats
                ]
            ).alias("start_time"),
            ## Austin has some numbers that have commas (1,027)
            pl.col("duration_minutes").str.replace_all(",", "").cast(pl.Int32),
        ]
    )

    # Compute end_time
    df = df.with_columns(
        (pl.col("start_time") + pl.duration(minutes=pl.col("duration_minutes"))).alias(
            "end_time"
        )
    )

    return df


def process_bicycle_transit_system(df, args):
    stations_df = utils_bicycle_transit_systems.stations_csv_to_df(args)
    df = utils_bicycle_transit_systems.append_station_names(df, stations_df).drop(
        "start_station_id", "end_station_id"
    )
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
    headers = df.collect_schema().names()
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


def select_final_columns(df, final_columns):
    return df.select(final_columns)


def handle_odd_hour_duration(df):
    ### HH:MM:SS - but hours can go over 24 for Taipei
    parts = pl.col("duration").str.split_exact(":", 3)
    return df.with_columns(
        (
            # hour to seconds
            parts.struct.field("field_0").cast(pl.Int64) * 3600
            # minutes to seconds
            + parts.struct.field("field_1").cast(pl.Int64) * 60
            + parts.struct.field("field_2").cast(pl.Int64)
        ).alias("duration")
    )


PROCESSING_FUNCTIONS = {
    "rename_columns": lambda df, ctx: df.pipe(
        utils.rename_columns_for_keys(ctx["renamed_columns"])
    ),
    "convert_to_datetime": lambda df, ctx: df.pipe(
        utils.convert_columns_to_datetime(
            ["start_time", "end_time"], ctx["date_formats"]
        )
    ),
    "select_final_columns": lambda df, ctx: select_final_columns(
        df, ctx.get("final_columns", constants.DEFAULT_FINAL_COLUMNS)
    ),
    "offset_two_digit_years": lambda df, ctx: utils.offset_two_digit_years(df),
    "austin_calculate_end_time": lambda df, ctx: austin_check(df, ctx),
    "convert_milliseconds_to_datetime": lambda df,
    ctx: convert_milliseconds_to_datetime(df),
    "filter_null_rows": lambda df, ctx: filter_null_rows(df),
    # City-centric functions
    ### Oslo
    "handle_oslo_legacy_stations": lambda df, ctx: utils.handle_oslo_legacy_stations(
        df, ctx["args"]
    ),
    ### Philadelphia and Los Angeles
    "process_bicycle_transit_stations": lambda df, ctx: process_bicycle_transit_system(
        df, ctx["args"]
    ),
    ### Guadalajara
    "handle_guadalajara_stations": lambda df, ctx: utils.handle_guadalajara_stations(
        df
    ),
    ### Taipei
    "handle_odd_hour_duration": lambda df, ctx: handle_odd_hour_duration(df),
    ### Mexico City
    "join_mexico_city_station_names": lambda df,
    ctx: utils.join_mexico_city_station_names(df),
    "clean_datetimes": lambda df, ctx: utils.clean_datetimes(df),
    "combine_datetimes": lambda df, ctx: utils.combine_datetimes(df),
}


def determine_has_header(file_path, expected_columns):
    # Not all files have headers
    with open(file_path, "r", encoding="utf-8", errors="replace") as file:
        first_line = file.readline().strip().split(",")
        return all(item in expected_columns for item in first_line)


def get_csv_scan_params(file_path, opts):
    has_header = opts.get("has_header", True)
    new_columns = opts.get("new_columns")

    base = {**opts, "encoding": "utf8-lossy", "infer_schema_length": 0}
    base = opts | {"encoding": "utf8-lossy", "infer_schema_length": 0}
    if has_header == "auto":
        if not new_columns:
            raise ValueError("has_header: auto requires new_columns.")
        file_has_header = determine_has_header(file_path, new_columns)

        return base | (
            {"has_header": True}
            if file_has_header
            else {
                "has_header": False,
                "new_columns": new_columns,
                "infer_schema_length": 10000,
            }
        )

    return base | (
        {"has_header": True}
        if has_header
        else {
            "has_header": False,
            "new_columns": new_columns,
            "infer_schema_length": 10000,
        }
    )


def create_parquet(file, args):
    config = load_city_config(args.city)
    csv_options = config.get("read_csv_options", {})
    params = get_csv_scan_params(file, csv_options)

    df = pl.scan_csv(file, **params)
    context = {**config, "args": args}
    for step in config.get(
        "processing_pipeline", constants.DEFAULT_PROCESSING_PIPELINE
    ):
        execute_step = PROCESSING_FUNCTIONS[step]
        df = execute_step(df, context)

    parquet_directory = utils.get_parquet_directory(args.city)
    file_name = os.path.basename(file).replace(".csv", ".parquet")
    parquet_path = parquet_directory / file_name

    df.sink_parquet(parquet_path)
    print(f"✅ Created {os.path.basename(parquet_path)}")


def delete_folder(folder_path):
    """
    Delete a folder and all its contents (files + subdirectories).
    """
    path = Path(folder_path)
    if not path.exists():
        print(f"⚠️ Folder not found: {path}")
        return

    shutil.rmtree(path)
    print("🗑️  Clearing folder to write completely new parquets")


def partition_parquet(args):
    parquet_directory = utils.get_parquet_directory(args.city)
    output_path = utils.get_city_output_directory(args.city)

    print(f"Scanning all files in {parquet_directory}")
    lf = pl.scan_parquet(parquet_directory / "*.parquet").with_columns(
        [
            pl.col("start_time").dt.year().alias("year"),
            pl.col("start_time").dt.month().alias("month"),
        ]
    )

    df = lf.collect(engine="streaming")

    ### Clear out old parquets each time so we don't keep adding to the same folder on each run
    delete_folder(output_path)

    df.write_parquet(
        output_path,
        partition_by=["year", "month"],
    )

    print("All files created and partitioned!")


def create_trip_df(file, args):
    config = load_city_config(args.city)
    read_csv_options = config.get("read_csv_options", {})
    df = pl.scan_csv(file, infer_schema_length=0, **read_csv_options)
    context = {**config, "args": args}

    for step in config.get(
        "processing_pipeline", constants.DEFAULT_PROCESSING_PIPELINE
    ):
        fn = PROCESSING_FUNCTIONS[step]
        df = fn(df, context)

    return df


# Old way of importing into doltdb
# def add_trips_to_db(files, args):
#     engine = utils_dolt.establish_engine()
#     for file in files:
#         city_config = constants.config[args.city]
#         file_metadata = get_file_metadata(file, city_config)
#         file_processed = utils_dolt.is_file_processed(engine, file_metadata)
#         file_name = file_metadata["name"]
#         if file_processed:
#             print(f"🟡 Skipping {file_name} - already processed")
#         else:
#             print(f"🐢 processing {file_name}")

#             # DEBUGGING TIPS
#             # For debugging and printing tables with null data for a particular column after formatting
#             # df_start_time = df.filter(pl.col("start_time").is_null())
#             # print(df_start_time)
#             df_lazy = create_trip_df(file, args)

#             utils_dolt.insert_trip_data(engine, df_lazy, file_metadata)


### Vancouver data currently has hidden \r in files (probably from Google Doc or Windows save)
def normalize_newlines(csv_path: str) -> None:
    """
    Normalize line endings in a CSV file:
    - Converts Windows (\r\n) and stray carriage returns (\r) to Unix (\n)

    Parameters
    ----------
    csv_path : str
        Path to the CSV file to clean.
    backup : bool, default False
        Whether to create a backup file (e.g., file.csv.bak) before overwriting.
    """
    path = Path(csv_path)

    text = path.read_text(encoding="utf-8", errors="ignore")
    text_clean = text.replace("\r\n", "\n").replace("\r", "\n")
    path.write_text(text_clean, encoding="utf-8")


CSV_TO_PARQUET_FUNCTIONS = {"normalize_newlines": normalize_newlines}


def convert_csvs_to_parquet(files, args):
    config = load_city_config(args.city)
    for file in files:
        csv_to_parquet_pipeline = config.get("csv_to_parquet_pipeline", [])
        for step in csv_to_parquet_pipeline:
            CSV_TO_PARQUET_FUNCTIONS[step](file)

        create_parquet(file, args)


def get_dfs_for_parquet(files, args):
    file_dataframes = []
    for file in files:
        df_lazy = create_trip_df(file, args)
        file_dataframes.append(df_lazy)

    return pl.concat(file_dataframes)


def build_all_trips(args):
    source_directory = utils.get_raw_files_directory(args.city)

    if args.skip_unzip is False:
        utils.unzip_city_zips(args.city)
    else:
        print("skipping unzipping files")

    trip_files = utils.get_csv_files(source_directory)
    config = load_city_config(args.city)
    filtered_files = filter_filenames(trip_files, config)

    convert_csvs_to_parquet(filtered_files, args)
    partition_parquet(args)
    # all_trips_df_lazy = get_dfs_for_parquet(filtered_files, args)
    # utils.create_final_files_and_logs(all_trips_df_lazy, args)
