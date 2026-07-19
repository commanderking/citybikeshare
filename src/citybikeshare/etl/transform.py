import gzip
import os
from dataclasses import replace
import polars as pl
from citybikeshare.config.loader import load_city_config
from citybikeshare.utils.io_transform import (
    delete_folder,
)
from citybikeshare.etl.pipelines.common import PROCESSING_FUNCTIONS
from citybikeshare.etl.station_maps import PRE_TRANSFORM_FUNCTIONS
from citybikeshare.etl.station_coordinates import update_gbfs_station_coordinates
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


def _read_first_line_fields(file_path):
    """Return the comma-split fields of a file's first line (gzip-aware)."""
    opener = gzip.open if str(file_path).endswith(".gz") else open
    with opener(file_path, "rt", encoding="utf-8", errors="replace") as file:
        return file.readline().strip().split(",")


def determine_has_header(file_path, expected_columns):
    # Not all files have headers
    return all(item in expected_columns for item in _read_first_line_fields(file_path))


def _header_name_candidates(opts):
    """Every column name a headerless layout could assign — used by has_header detection to tell
    a header row (all fields are known names) from a data row. Spans all `new_columns_by_count`
    layouts so a headed file from any era is recognized."""
    by_count = opts.get("new_columns_by_count")
    if by_count:
        return {name for layout in by_count.values() for name in layout}
    return set(opts.get("new_columns") or [])


def get_csv_scan_params(file_path, opts):
    """Build the pl.scan_csv kwargs. The scan only decides whether row 1 is a header; naming a
    headerless file's positional columns is deferred to the `assign_positional_columns` pipeline
    step (Polars emits column_1..N, which that step renames). Everything except our header-control
    keys is a real scan_csv kwarg (encoding, null_values, …) and passes straight through."""
    has_header = opts.get("has_header", True)
    base = {"encoding": "utf8-lossy", "infer_schema_length": 0} | {
        k: v
        for k, v in opts.items()
        if k not in ("has_header", "new_columns", "new_columns_by_count")
    }

    if has_header == "auto":
        has_header = determine_has_header(file_path, _header_name_candidates(opts))

    if has_header:
        return base | {"has_header": True}

    # Headerless: columns arrive as column_1..N; assign_positional_columns names them downstream.
    return base | {"has_header": False, "infer_schema_length": 10000}


def create_parquet(file, context: PipelineContext, config):
    csv_options = config.get("read_csv_options", {})
    params = get_csv_scan_params(file, csv_options)

    df = pl.scan_csv(file, **params)
    # Expose the current input file so file-scoped steps can target it by name.
    context = replace(context, source_file=file)
    for step in config.get("processing_pipeline", DEFAULT_PROCESSING_PIPELINE):
        execute_step = PROCESSING_FUNCTIONS[step]
        df = execute_step(df, config, context)

    parquet_directory = context.parquet_directory
    file_name = _parquet_name(file)
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

    # Rebuild the partitioned output from scratch so stale partitions don't linger
    # alongside new ones. The per-file parquet cache in parquet/ is untouched.
    print(f"🗑️  Rebuilding partitioned output at {output_path}")
    delete_folder(output_path)

    # Stream straight to the partitioned dataset (year=YYYY/month=MM) rather than collecting
    # the whole city into memory first — bounded memory, single streaming pass.
    lf.sink_parquet(
        pl.PartitionByKey(output_path, by=["year", "month"]),
        mkdir=True,
    )

    print("All files created and partitioned!")


def _parquet_name(csv_file):
    base = os.path.basename(csv_file)
    for suffix in (".csv.gz", ".csv"):
        if base.endswith(suffix):
            return base[: -len(suffix)] + ".parquet"
    return base + ".parquet"


def _remove_orphan_parquets(context: PipelineContext, expected_names):
    """Delete parquet files whose source CSV is no longer in the input set, so
    removed/renamed inputs don't linger in the partitioned output."""
    for pq in context.parquet_directory.glob("*.parquet"):
        if pq.name not in expected_names:
            pq.unlink()
            print(f"🗑️  Removed orphan parquet: {pq.name}")


def run_pre_transform_steps(context: PipelineContext, config: dict):
    """Run a city's configured `pre_transform_pipeline` once, before the file loop."""
    for step in config.get("pre_transform_pipeline", []):
        PRE_TRANSFORM_FUNCTIONS[step](context, config)
    # A GBFS `coordinates.source` auto-refreshes its committed coordinates here — declared once
    # under `coordinates.source`, so there's no separate pre-transform step to keep in sync.
    source = (config.get("coordinates") or {}).get("source") or {}
    if source.get("type") == "gbfs":
        update_gbfs_station_coordinates(context, required=False)


def transform_city_data(context: PipelineContext, incremental: bool = True):
    source_directory = context.transform_input_directory
    trip_files = get_csv_files(source_directory)
    config = load_city_config(context.city)
    run_pre_transform_steps(context, config)
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
