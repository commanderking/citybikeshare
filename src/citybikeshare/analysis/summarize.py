import json
import polars as pl
from citybikeshare.context import PipelineContext
from citybikeshare.analysis.utils import derive_duration_column


def summarize_city(context: PipelineContext):
    """
    Summarize transformed Parquet data by year for a single city.
    """

    print(f"Preparing summary for: {context.city}")

    input_directory = context.transformed_directory
    analysis_directory = context.analysis_directory

    # Read all parquet files
    lf = pl.scan_parquet(input_directory / "**/*.parquet")
    # Ensure a datetime column exists
    if "start_time" not in lf.collect_schema().names():
        raise ValueError(
            f"{context.city}: missing 'start_time' column in transformed data"
        )

    # These are the columns we expect for every iteration of the data
    columns_for_null_check = [
        "start_time",
        "end_time",
        "start_station_name",
        "end_station_name",
    ]

    # Derive `duration` but DO NOT filter: the quality metrics (trip_count, null_rows)
    # must be computed over the full data, or they can't see the very rows (null or
    # negative times) they exist to surface — and `null_rows` would always read ~0 for
    # start_time/end_time because those rows would already be gone. Duration stats are
    # restricted to valid trips via a per-aggregation mask, and the count of valid trips
    # is reported alongside the total so dropped rows are visible, not silently removed.
    lf = derive_duration_column(lf)
    valid_duration = (pl.col("end_time") - pl.col("start_time") >= 0) & pl.col(
        "duration"
    ).is_not_null()
    duration = pl.col("duration").filter(valid_duration)

    summary = (
        lf.group_by("year")
        .agg(
            [
                pl.len().alias("trip_count"),  # total trips (unfiltered)
                duration.len().alias("valid_duration_count"),
                duration.median().alias("duration_median"),
                duration.quantile(0.05).alias("duration_5_percent"),
                duration.quantile(0.25).alias("duration_q1"),
                duration.quantile(0.75).alias("duration_q3"),
                duration.quantile(0.95).alias("duration_95_percent"),
                # Number of rows with at least one null among the key columns.
                pl.any_horizontal(
                    [pl.col(c).is_null() for c in columns_for_null_check]
                )
                .sum()
                .alias("null_rows"),
            ]
        )
        .sort("year")
        .collect()
        .to_dicts()
    )

    analysis_directory = context.analysis_directory
    analysis_directory.mkdir(parents=True, exist_ok=True)

    # Write JSON output
    output_file = analysis_directory / "summary.json"
    with open(output_file, "w") as f:
        json.dump(summary, f, indent=2)

    print(f"✅ Wrote summary for {context.city} to {output_file}")
