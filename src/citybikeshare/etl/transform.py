import os
import polars as pl
from src.citybikeshare.config.loader import load_city_config
from src.citybikeshare.utils.io_transform import (
    delete_folder,
)

from src.citybikeshare.etl.pipelines.common import PROCESSING_FUNCTIONS
from src.citybikeshare.etl.constants import DEFAULT_PROCESSING_PIPELINE
from src.citybikeshare.utils.paths import (
    get_parquet_directory,
    get_city_output_directory,
    get_raw_files_directory,
    get_csv_files,
)

from src.citybikeshare.utils.io_clean import CLEAN_FUNCTIONS


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


def determine_has_header(file_path, expected_columns):
    # Not all files have headers
    with open(file_path, "r", encoding="utf-8", errors="replace") as file:
        first_line = file.readline().strip().split(",")
        return all(item in expected_columns for item in first_line)


def get_csv_scan_params(file_path, opts):
    has_header = opts.get("has_header", True)
    new_columns = opts.get("new_columns")

    base = {"encoding": "utf8-lossy", "infer_schema_length": 0} | opts
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
    for step in config.get("processing_pipeline", DEFAULT_PROCESSING_PIPELINE):
        execute_step = PROCESSING_FUNCTIONS[step]
        df = execute_step(df, context)

    parquet_directory = get_parquet_directory(args.city)
    file_name = os.path.basename(file).replace(".csv", ".parquet")
    parquet_path = parquet_directory / file_name

    df.sink_parquet(parquet_path)
    print(f"✅ Created {os.path.basename(parquet_path)}")


def partition_parquet(args):
    parquet_directory = get_parquet_directory(args.city)
    output_path = get_city_output_directory(args.city)

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


def convert_csvs_to_parquet(files, args):
    for file in files:
        print(f"Processing {file}")
        create_parquet(file, args)


def transform(args):
    source_directory = get_raw_files_directory(args.city)
    trip_files = get_csv_files(source_directory)
    config = load_city_config(args.city)
    filtered_files = filter_filenames(trip_files, config)

    convert_csvs_to_parquet(filtered_files, args)
    partition_parquet(args)
