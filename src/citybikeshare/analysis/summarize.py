import json
import polars as pl
from src.citybikeshare.context import PipelineContext
from src.citybikeshare.analysis.utils import append_duration_column


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
    # Determine duration column
    duration_column = "duration"
    summary = (
        lf.pipe(append_duration_column)
        .group_by("year")
        .agg(
            [
                pl.count().alias("trip_count"),
                pl.col(duration_column).median().alias("duration_median"),
                pl.col(duration_column).quantile(0.05).alias("duration_5_percent"),
                pl.col(duration_column).quantile(0.25).alias("duration_q1"),
                pl.col(duration_column).quantile(0.75).alias("duration_q3"),
                pl.col(duration_column).quantile(0.95).alias("duration_95_percent"),
                # Counts if null is any column of row
                pl.sum_horizontal(
                    [pl.col(c).is_null().cast(pl.Int64) for c in columns_for_null_check]
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
    output_file = analysis_directory / f"summary.json"
    with open(output_file, "w") as f:
        json.dump(summary, f, indent=2)

    print(f"✅ Wrote summary for {context.city} to {output_file}")
