import os
import polars as pl
from citybikeshare.config.loader import load_city_config
from citybikeshare.utils.io_transform import (
    delete_folder,
)
from citybikeshare.etl.pipelines.common import PROCESSING_FUNCTIONS
from citybikeshare.etl.constants import DEFAULT_PROCESSING_PIPELINE
from citybikeshare.utils.paths import (
    get_csv_files,
)
from citybikeshare.context import PipelineContext
from citybikeshare.etl.state import (
    file_signature,
    is_unchanged,
    load_state,
    write_state,
)


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

    # Keep truncate_ragged_lines FALSE so ragged rows fail loudly instead of being silently
    # mangled. A "ragged" row has more fields than the header declares, e.g. for a 3-column
    # header "start_time,end_time,station":
    #   "2024-01-01 00:00,2024-01-01 00:10,Elm St,EXTRA,42.3"  -> 5 fields, 2 trailing extras
    # Causes: an unquoted comma inside a value ("Main St, Suite 100" splits in two), a stray
    # trailing comma, schema drift, or files concatenated without a separating newline.
    # Truncating would only be safe for *trailing* extras; an unquoted comma in an early column
    # shifts later values and silently mis-aligns the row — so we'd rather error and inspect.
    # A city whose raggedness is understood and benign can opt in via its read_csv_options:
    #   read_csv_options: { truncate_ragged_lines: true }   (it overrides this default below).
    base = {
        "encoding": "utf8-lossy",
        "infer_schema_length": 0,
        "truncate_ragged_lines": False,
    } | opts
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


def create_parquet(file, context: PipelineContext, config):
    csv_options = config.get("read_csv_options", {})
    params = get_csv_scan_params(file, csv_options)

    df = pl.scan_csv(file, **params)
    for step in config.get("processing_pipeline", DEFAULT_PROCESSING_PIPELINE):
        execute_step = PROCESSING_FUNCTIONS[step]
        df = execute_step(df, config, context)

    parquet_directory = context.parquet_directory
    file_name = os.path.basename(file).replace(".csv", ".parquet")
    parquet_path = parquet_directory / file_name

    df.sink_parquet(parquet_path)
    print(f"✅ Created {os.path.basename(parquet_path)}")


def partition_parquet(context: PipelineContext):
    parquet_directory = context.parquet_directory
    output_path = context.transformed_directory

    if not list(parquet_directory.glob("*.parquet")):
        print("⚠️ No parquet files to partition; clearing output")
        delete_folder(output_path)
        return

    print(f"Scanning all files in {parquet_directory}")
    lf = pl.scan_parquet(
        parquet_directory / "*.parquet", extra_columns="ignore"
    ).with_columns(
        [
            pl.col("start_time").dt.year().alias("year"),
            pl.col("start_time").dt.month().alias("month"),
        ]
    )

    df = lf.collect(engine="streaming")

    # Rebuild the partitioned output from scratch so stale partitions don't linger
    # alongside new ones. The per-file parquet cache in parquet/ is untouched.
    print(f"🗑️  Rebuilding partitioned output at {output_path}")
    delete_folder(output_path)

    df.write_parquet(
        output_path,
        partition_by=["year", "month"],
    )

    print("All files created and partitioned!")


def _parquet_name(csv_file):
    return os.path.basename(csv_file).replace(".csv", ".parquet")


def _remove_orphan_parquets(context: PipelineContext, expected_names):
    """Delete parquet files whose source CSV is no longer in the input set, so
    removed/renamed inputs don't linger in the partitioned output."""
    for pq in context.parquet_directory.glob("*.parquet"):
        if pq.name not in expected_names:
            pq.unlink()
            print(f"🗑️  Removed orphan parquet: {pq.name}")


def transform_city_data(context: PipelineContext, incremental: bool = True):
    source_directory = context.transform_input_directory
    trip_files = get_csv_files(source_directory)
    config = load_city_config(context.city)
    filtered_files = filter_filenames(trip_files, config)

    state = load_state(context.transform_state_path) if incremental else {}
    new_state: dict = {}
    expected_parquets = set()

    for file in filtered_files:
        name = os.path.basename(file)
        parquet_name = _parquet_name(file)
        expected_parquets.add(parquet_name)
        parquet_path = context.parquet_directory / parquet_name
        recorded = state.get(name)

        if incremental and is_unchanged(file, recorded) and parquet_path.exists():
            print(f"🟡 Skipping transform - {name} unchanged")
            new_state[name] = recorded
            continue

        print(f"Processing {file}")
        create_parquet(file, context, config)
        new_state[name] = {**file_signature(file), "outputs": [parquet_name]}

    _remove_orphan_parquets(context, expected_parquets)
    write_state(context.transform_state_path, new_state)
    partition_parquet(context)
